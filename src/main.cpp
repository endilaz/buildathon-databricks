// Shopping Sessions pipeline.
//
//   links    parse in parallel; the persons map is built in parallel
//   parse    one pass over events and clicks: each valid event and click is
//            resolved to its person and filed in that person's bucket; every
//            copy of an event also leaves a small record by event_id
//   dedup    per event_id bucket: the earliest ingest wins, and every other
//            copy's filed event is marked dropped where it lies
//   sessions per person bucket: group by person, sessions, credit, assists
//   output   hand-written Parquet, one user_daily file per worker
#include "parquet.hpp"
#include "sessions.hpp"
#include "table.hpp"
#include <map>
#include <type_traits>
#include <sched.h>
#include <sys/resource.h>

namespace ss {
namespace {

constexpr size_t kEventBucketLines = 65536;  // event_id buckets: about this many lines each
constexpr size_t kPersonBucketEvents = 65536;  // person buckets: few enough that staging groups stay in L2

#pragma pack(push, 8)
// Click times relative to the click window start: 32 bits when events pack in u64.
template <class E>
using Rel = std::conditional_t<std::is_same_v<E, u64>, u32, u64>;
template <class E>
struct PersonEvent {  // a valid event, filed under its person; ev is all ones once dedup drops it
    Key person;
    E ev;
};
template <class E>
struct DedupRec {  // every parsed copy of an event, by event_id
    Key eid;
    PersonEvent<E>* ev;  // where its event was filed; null when the copy is invalid
    i64 ingest;
};
template <class E>
struct PersonClick {
    Key person, cid;
    Rel<E> rel;
    u32 camp;
};
#pragma pack(pop)
static_assert(sizeof(DedupRec<u64>) == 32 && sizeof(PersonClick<u64>) == 40 && sizeof(PersonEvent<u64>) == 24);

// ---------------------------------------------------------------- catalog

struct Catalog {
    std::vector<std::string> categories;  // sorted byte-wise; index = rank
    u32 unknown = 0;
    KeyMap<u32> products;
    bool numbered = false;        // products named <prefix><decimal> use by_number
    char prefix = 0;
    std::vector<u16> by_number;   // 0xFFFF: no such product
    KeyMap<u32> campaigns;
    std::vector<Key> campaign_ids;
    std::vector<std::string> channels;

    static bool product_number(Field id, char prefix, u32& num) { return prefixed_number(id, prefix, num); }

    u32 category(Field pid) const {
        u32 num;
        if (numbered && product_number(pid, prefix, num) && num < (1u << 22)) {
            if (num >= by_number.size()) return unknown;
            u16 c = by_number[num];
            return c == 0xFFFF ? unknown : c;
        }
        const u32* c = products.find(make_key(pid));
        return c ? *c : unknown;
    }

    explicit Catalog(const std::string& dir) {
        Input pf = map_input(dir + "/products.csv");
        std::vector<std::pair<Field, Field>> rows;
        for_lines<2>(pf, 0, pf.len, [&](Field (&f)[2]) {
            if (!f[0].empty()) rows.emplace_back(f[0], f[1]);
        });
        // distinct category names, then ranks in byte-wise order
        std::unordered_map<std::string_view, u32> rank{{"unknown", 0}};
        for (auto& r : rows) rank.try_emplace(r.second, 0);
        for (auto& [name, r] : rank) categories.emplace_back(name);
        std::sort(categories.begin(), categories.end());
        for (u32 i = 0; i < categories.size(); ++i) rank[categories[i]] = i;
        unknown = rank.at("unknown");
        size_t first[256] = {};
        for (auto& r : rows) ++first[u8(r.first[0])];
        prefix = char(std::max_element(first, first + 256) - first);
        numbered = categories.size() < 0xFFFF;
        products.reset(rows.size());
        for (auto& r : rows) {
            u32 c = rank.at(r.second);
            products.insert(make_key(r.first), c);
            u32 num;
            if (numbered && product_number(r.first, prefix, num) && num < (1u << 22)) {
                if (num >= by_number.size()) by_number.resize(num + 1, 0xFFFF);
                by_number[num] = u16(c);
            }
        }

        Input cf = map_input(dir + "/campaigns.csv");
        for_lines<2>(cf, 0, cf.len, [&](Field (&f)[2]) {
            if (f[0].empty()) return;
            Key k = make_key(f[0]);
            u32& idx = campaigns.get_or_insert(k, [&] {
                campaign_ids.push_back(k);
                channels.emplace_back();
                return u32(campaign_ids.size() - 1);
            });
            channels[idx] = std::string(f[1]);
        });
        // product and campaign keys were copied; the mappings stay for Fields in rows
    }
};

// ---------------------------------------------------------------- links

struct Persons {
    // every ID in a kept link line -> its person's name: one flat
    // linear-probing table (one huge-page allocation), split by the top bits
    // of the key hash into equal regions that are filled in parallel
    static constexpr u32 kPartBits = 6;
    static size_t part(u64 h) { return size_t(h >> (64 - kPartBits)); }
    std::vector<KeySlot<Key>> slots = std::vector<KeySlot<Key>>(size_t(1) << kPartBits);
    size_t region_bits = 0;  // each region holds 2^region_bits slots
    size_t count = 0;

    size_t slot(u64 h) const { return (part(h) << region_bits) | (size_t(h) & ((size_t(1) << region_bits) - 1)); }
    size_t next(size_t i) const {
        size_t mask = (size_t(1) << region_bits) - 1;
        return (i & ~mask) | ((i + 1) & mask);
    }
    // Inserts k (absent: value v) into its region; returns its value.
    Key& insert(Key k, u64 h, Key v) {
        for (size_t i = slot(h);; i = next(i)) {
            auto& s = slots[i];
            if (s.k == k) return s.v;
            if (!s.k) {
                s.k = k;
                s.v = v;
                return s.v;
            }
        }
    }
    Key name(Key user) const { return name_hashed(user, key_hash(user)); }
    // Linked or not is a coin toss from line to line: the first slot's answer
    // is selected, not branched on; only a collision walks on.
    Key name_select(Key user, u64 h) const {
        size_t i = slot(h);
        const auto& s = slots[i];
        Key k = s.k;
        Key hit = s.v;
        if ((k != user) & (k != 0)) [[unlikely]] return name_hashed(user, h);
        return k == user ? hit : user;
    }
    // h must be key_hash(user)
    Key name_hashed(Key user, u64 h) const {
        for (size_t i = slot(h);; i = next(i)) {
            const auto& s = slots[i];
            if (s.k == user) return s.v;
            if (!s.k) return user;
        }
    }
    void prefetch_user(Key user) const { prefetch_hashed(key_hash(user)); }
    void prefetch_hashed(u64 h) const { prefetch(&slots[slot(h)]); }
    size_t size() const { return count; }
};

#pragma pack(push, 8)
struct Endpoint {
    Key k;
    u32 ref;  // 2 * pair index + side
};
#pragma pack(pop)

// Union-find over the link pairs with every step but the unions parallel:
// endpoints are split by key-hash part, each part gives its IDs dense
// indexes in its own region of the final table, the unions run over those
// small integers, and each region then maps its IDs to their person's name.
inline Persons build_persons(std::vector<std::vector<std::pair<Key, Key>>>& pairs, size_t threads) {
    constexpr size_t P = size_t(1) << Persons::kPartBits;
    const size_t nv = pairs.size();
    std::vector<size_t> base(nv + 1, 0);
    for (size_t t = 0; t < nv; ++t) base[t + 1] = base[t] + pairs[t].size();
    const size_t n = base[nv];
    Persons out;
    if (!n) return out;
    std::vector<int> workers(threads);

    std::vector<Scatter<Endpoint>> spread;
    spread.reserve(nv);
    for (size_t t = 0; t < nv; ++t) spread.emplace_back(P);
    run_tasks(workers, nv, [&](int&, size_t t) {
        auto& v = pairs[t];
        for (size_t j = 0; j < v.size(); ++j) {
            u32 ref = u32((base[t] + j) * 2);
            spread[t].push(Persons::part(key_hash(v[j].first)), Endpoint{v[j].first, ref});
            spread[t].push(Persons::part(key_hash(v[j].second)), Endpoint{v[j].second, ref + 1});
        }
    });

    // dense index per part; an endpoint's id is (index << kPartBits) | part
    size_t most = 0;
    for (size_t p = 0; p < P; ++p) {
        size_t cnt = 0;
        for (auto& sc : spread) cnt += sc.size(p);
        most = std::max(most, cnt);
    }
    // regions sized for every endpoint being distinct, at most 2/3 full
    while ((size_t(1) << out.region_bits) * 2 < most * 3) ++out.region_bits;
    out.slots.assign(P << out.region_bits, KeySlot<Key>{});
    std::vector<u32> endpoint(2 * n), sizes(P);
    run_tasks(workers, P, [&](int&, size_t p) {
        u32 next = 0;
        for (auto& sc : spread)
            sc.for_each(p, [&](const Endpoint& e) {
                Key& v = out.insert(e.k, key_hash(e.k), Key(next));
                if (u32(v) == next) ++next;
                endpoint[e.ref] = (u32(v) << Persons::kPartBits) | u32(p);
            });
        sizes[p] = next;
    });
    std::vector<u32> offset(P + 1, 0);
    for (size_t p = 0; p < P; ++p) offset[p + 1] = offset[p] + sizes[p];
    const u32 ids = offset[P];
    out.count = ids;
    auto global = [&](u32 e) { return offset[e & (P - 1)] + (e >> Persons::kPartBits); };

    // unions; a non-root's parent always has a smaller index
    std::vector<u32> parent(ids);
    for (u32 i = 0; i < ids; ++i) parent[i] = i;
    auto find = [&](u32 x) {
        while (parent[x] != x) {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        return x;
    };
    for (size_t j = 0; j < n; ++j) {
        u32 ra = find(global(endpoint[2 * j])), rb = find(global(endpoint[2 * j + 1]));
        if (ra != rb) parent[std::max(ra, rb)] = std::min(ra, rb);
    }
    // in index order every parent is already final, so one pass compresses
    // all paths and finds each root's smallest name
    const size_t region = size_t(1) << out.region_bits;
    std::vector<Key> name(ids);
    run_tasks(workers, P, [&](int&, size_t p) {
        for (size_t i = p * region; i < (p + 1) * region; ++i)
            if (auto& s = out.slots[i]; s.k) name[offset[p] + u32(s.v)] = s.k;
    });
    for (u32 i = 0; i < ids; ++i) {
        u32 r = parent[i] = parent[parent[i]];
        if (r != i && key_less(name[i], name[r])) name[r] = name[i];
    }
    run_tasks(workers, P, [&](int&, size_t p) {
        for (size_t i = p * region; i < (p + 1) * region; ++i)
            if (auto& s = out.slots[i]; s.k) s.v = name[parent[offset[p] + u32(s.v)]];
    });
    for (auto& v : pairs) std::vector<std::pair<Key, Key>>().swap(v);
    return out;
}

// ---------------------------------------------------------------- chunks

struct Chunk {
    const Input* in;
    size_t s, e;
};

inline void plan_chunks(const std::vector<Input>& files, size_t target, std::vector<Chunk>& out) {
    for (auto& f : files) {
        if (!f.len) continue;
        size_t parts = std::max<size_t>(1, (f.len + target - 1) / target);
        size_t step = (f.len + parts - 1) / parts;
        for (size_t s = 0; s < f.len; s += step) out.push_back({&f, s, std::min(f.len, s + step)});
    }
}

inline size_t pow2_at_least(size_t n) {
    size_t p = 1;
    while (p < n) p <<= 1;
    return p;
}
inline u32 log2_exact(size_t p) { return u32(__builtin_ctzll(p)); }

// ---------------------------------------------------------------- pipeline

struct Pipeline {
    std::string data_dir, out_dir;
    size_t threads;
    Timer timer;
    Window w{};
    Catalog* catalog = nullptr;
    std::vector<Input> link_files, event_files, click_files;

    // Person lookups during the parse run this many lines after their
    // prefetch, so each (DRAM) miss overlaps the parsing of the lines between.
    static constexpr size_t kReadPiece = size_t(256) << 10;
    static constexpr size_t kParseDelay = 16;
    template <class E>
    struct PendingEvent {
        Key eid, user;
        i64 ingest;
        E ev;
        u64 h;  // key_hash(user)
    };
    template <class E>
    struct ParseState {
        StagedScatter<PersonEvent<E>> ev;  // valid events by person hash
        StagedScatter<DedupRec<E>> dd;     // every copy by event_id hash
        StagedScatter<PersonClick<E>> ck;  // valid clicks by person hash
        PendingEvent<E> pending_ev[kParseDelay];
        PersonClick<E> pending_ck[kParseDelay];
        u64 pending_ck_h[kParseDelay];
        size_t nev = 0, nck = 0;
        std::vector<char> buf;  // the chunk being parsed, when inputs are read rather than mapped
        ParseState(size_t event_buckets, size_t person_buckets)
            : ev(person_buckets), dd(event_buckets), ck(person_buckets) {}
    };
    size_t event_buckets = 0, person_buckets = 0;
    u32 event_shift = 0, person_shift = 0;
    Persons persons;

    Pipeline(std::string d, std::string o, size_t t, bool profile)
        : data_dir(std::move(d)), out_dir(std::move(o)), threads(t), timer(profile) {}

    void read_manifest() {
        Input m = map_input(data_dir + "/manifest.json");
        std::string_view json(m.data, m.len);
        auto get = [&](std::string_view name) {
            size_t k = json.find("\"" + std::string(name) + "\"");
            size_t colon = k == json.npos ? k : json.find(':', k + name.size() + 2);
            size_t a = colon == json.npos ? colon : json.find('"', colon + 1);
            size_t b = a == json.npos ? a : json.find('"', a + 1);
            i64 v;
            if (b == json.npos || !parse_ts(json.substr(a + 1, b - a - 1), v))
                die("bad manifest field %s", std::string(name).c_str());
            return v;
        };
        w.start = get("window_start");
        w.end = get("window_end");
        w.click_start = w.start - kWeek;
        w.day0 = floor_div(w.start, kDay);
        w.days = w.end > w.start ? u32(floor_div(w.end - 1, kDay) - w.day0 + 1) : 1;
        unmap_input(m);
    }

    template <class E>
    void flush_event(ParseState<E>& st, size_t i) {
        const PendingEvent<E>& x = st.pending_ev[i];
        Key person = persons.name_select(x.user, x.h);
        PersonEvent<E>* p = st.ev.push(size_t(key_hash(person) >> person_shift), PersonEvent<E>{person, x.ev});
        st.dd.push(size_t(key_hash(x.eid) >> event_shift), DedupRec<E>{x.eid, p, x.ingest});
    }
    template <class E>
    void flush_click(ParseState<E>& st, size_t i) {
        PersonClick<E>& c = st.pending_ck[i];
        c.person = persons.name_select(c.person, st.pending_ck_h[i]);
        st.ck.push(size_t(key_hash(c.person) >> person_shift), c);
    }
    template <class E>
    void drain(ParseState<E>& st) {
        for (size_t i = st.nev > kParseDelay ? st.nev - kParseDelay : 0; i < st.nev; ++i) flush_event(st, i % kParseDelay);
        for (size_t i = st.nck > kParseDelay ? st.nck - kParseDelay : 0; i < st.nck; ++i) flush_click(st, i % kParseDelay);
        st.nev = st.nck = 0;
    }

    template <class E>
    void parse_event(Field (&f)[7], ParseState<E>& st) {
        if (f[0].empty()) return;
        i64 ingest;
        if (!parse_f1(f[3], ingest)) return;
        Key eid = make_key(f[0]);
        i64 ts;
        if (!f[1].empty() && !f[5].empty() && parse_ts(f[2], ts) && ts >= w.start && ts < w.end &&
            ingest - ts <= kDay) {
            // event_type by fixed-width loads (fields are followed by padding)
            Field type = f[4];
            bool ok = false;
            u32 price = 0;
            if (type.size() == 8) {
                ok = load64(type.data()) == load64("purchase") && parse_price(f[6], price);
            } else if (type.size() == 4) {
                u32 t;
                std::memcpy(&t, type.data(), 4);
                ok = t == load32("view") || t == load32("cart");
            }
            if (ok) {
                // only purchases need a category: top_category sums purchase prices
                u32 cat = price ? catalog->category(f[5]) : 0;
                Key user = make_key(f[1]);
                u64 h = key_hash(user);
                persons.prefetch_hashed(h);
                size_t i = st.nev % kParseDelay;
                if (st.nev >= kParseDelay) flush_event(st, i);
                st.pending_ev[i] = PendingEvent<E>{eid, user, ingest, Pack<E>::event(u64(ts - w.start), cat, price), h};
                ++st.nev;
                return;
            }
        }
        // an invalid copy files no event, but can still be the earliest copy
        st.dd.push(size_t(key_hash(eid) >> event_shift), DedupRec<E>{eid, nullptr, ingest});
    }

    template <class E>
    void parse_click(Field (&f)[4], ParseState<E>& st) {
        if (f[0].empty() || f[1].empty() || f[2].empty()) return;
        const u32* camp = catalog->campaigns.find(make_key(f[2]));
        if (!camp) return;
        i64 ts;
        if (!parse_ts(f[3], ts) || ts < w.click_start || ts >= w.end) return;
        Key user = make_key(f[1]);
        u64 h = key_hash(user);
        persons.prefetch_hashed(h);
        size_t i = st.nck % kParseDelay;
        if (st.nck >= kParseDelay) flush_click(st, i);
        st.pending_ck[i] = PersonClick<E>{user, make_key(f[0]), Rel<E>(ts - w.click_start), *camp};
        st.pending_ck_h[i] = h;
        ++st.nck;
    }

    void run() {
        read_manifest();
        std::thread catalog_thread([&] { catalog = new Catalog(data_dir); });
        auto map_dir = [&](const char* name) {
            std::vector<Input> v;
            for (auto& p : csv_files(data_dir + "/" + name)) v.push_back(map_input(p));
            return v;
        };
        link_files = map_dir("links");
        // events and clicks are read with pread in L2-sized pieces (on a VM that
        // can beat faulting and walking 4 KB file pages); SHOPPING_MMAP=1 maps them
        auto open_dir = [&](const char* name) {
            std::vector<Input> v;
            for (auto& p : csv_files(data_dir + "/" + name)) v.push_back(open_input(p));
            return v;
        };
        bool mapped = std::getenv("SHOPPING_MMAP") != nullptr;
        event_files = mapped ? map_dir("events") : open_dir("events");
        click_files = mapped ? map_dir("clicks") : open_dir("clicks");
        size_t ev_bytes = 0, ck_bytes = 0;
        for (auto& f : event_files) ev_bytes += f.len;
        for (auto& f : click_files) ck_bytes += f.len;
        Reservation::global().reserve(2 * ev_bytes + 2 * ck_bytes + (threads << 28) + (size_t(256) << 20));
        timer.lap("setup");

        // links
        std::vector<Chunk> chunks;
        plan_chunks(link_files, size_t(1) << 20, chunks);
        std::vector<std::vector<std::pair<Key, Key>>> link_pairs(threads);
        size_t link_bytes = 0;
        for (auto& f : link_files) link_bytes += f.len;
        for (auto& v : link_pairs) v.reserve(link_bytes / 18 / threads + 1024);  // lines are ~20 bytes
        run_tasks(link_pairs, chunks.size(), [&](auto& out, size_t i) {
            auto& c = chunks[i];
            for_lines<2>(*c.in, c.s, c.e, [&](Field (&f)[2]) {
                if (!f[0].empty() && !f[1].empty()) out.emplace_back(make_key(f[0]), make_key(f[1]));
            });
        });
        catalog_thread.join();
        timer.lap("links");

        // events are packed in 64 bits when the window, prices and categories fit
        bool small = catalog->categories.size() <= 256 && w.end - w.click_start < (i64(1) << 32) &&
                     !std::getenv("SHOPPING_WIDE");  // forces the general path, for tests
        if (small)
            process<u64>(link_pairs, ev_bytes);
        else
            process<u128>(link_pairs, ev_bytes);
    }

    template <class E>
    void process(std::vector<std::vector<std::pair<Key, Key>>>& link_pairs, size_t ev_bytes) {
        std::vector<Chunk> chunks;
        size_t est_lines = ev_bytes / 72 + 1;
        event_buckets = std::clamp<size_t>(pow2_at_least(est_lines / kEventBucketLines), 16, 4096);
        event_shift = 64 - log2_exact(event_buckets);
        person_buckets = std::clamp<size_t>(pow2_at_least(est_lines / kPersonBucketEvents), 16, 8192);
        person_shift = 64 - log2_exact(person_buckets);
        plan_chunks(event_files, size_t(4) << 20, chunks);
        size_t event_chunks = chunks.size();
        plan_chunks(click_files, size_t(4) << 20, chunks);
        {
            auto t0 = std::chrono::steady_clock::now();
            persons = build_persons(link_pairs, threads);
            if (timer.on)
                std::fprintf(stderr, "[%8s] persons built in %.1f ms (%zu ids)\n", "",
                             std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count(),
                             persons.size());
        }
        std::vector<ParseState<E>> parsed;
        parsed.reserve(threads);
        for (size_t t = 0; t < threads; ++t) parsed.emplace_back(event_buckets, person_buckets);
        run_tasks(parsed, chunks.size(), [&](ParseState<E>& st, size_t i) {
            auto& c = chunks[i];
            auto parse_range = [&](const Input& in, size_t s, size_t e) {
                if (i < event_chunks)
                    for_lines<7>(in, s, e, [&](Field (&f)[7]) { parse_event<E>(f, st); });
                else
                    for_lines<4>(in, s, e, [&](Field (&f)[4]) { parse_click<E>(f, st); });
            };
            if (c.in->data) {
                populate(*c.in, c.s, c.e + 4096);
                parse_range(*c.in, c.s, c.e);
            } else {
                // Read in pieces small enough to stay in L2 while parsed. A piece
                // is read from one byte before it (to see whether it starts a
                // line) through the end of the line that crosses its end.
                const Input& file = *c.in;
                for (size_t ps = c.s; ps < c.e; ps += kReadPiece) {
                    size_t pe = std::min(c.e, ps + kReadPiece);
                    size_t from = ps ? ps - 1 : 0, to = std::min(file.len, pe + 4096);
                    for (;;) {
                        size_t need = to - from + 64;
                        if (st.buf.size() < need) st.buf.resize(need);
                        read_range(file, from, to, st.buf.data());
                        size_t tail = pe - from - 1;  // the piece's last byte, in buf
                        if (to == file.len || std::memchr(st.buf.data() + tail, '\n', to - from - tail)) break;
                        to = std::min(file.len, to + (to - from));  // a very long line: read more
                    }
                    parse_range(Input{st.buf.data(), to - from, 0, -1}, ps - from, pe - from);
                }
            }
            drain(st);  // nothing may stay pending once the chunk is done
            store_fence();
        });
        for (auto& st : parsed) {  // write out every bucket's partial group
            st.ev.flush();
            st.dd.flush();
            st.ck.flush();
        }
        timer.lap("parse");
        // the records hold keys, not pointers: release the inputs off the critical path
        std::thread([files = std::move(event_files), clicks = std::move(click_files),
                     links = std::move(link_files)] {
            for (auto* v : {&files, &clicks, &links})
                for (auto& f : *v) unmap_input(f);
        }).detach();

        finish<E>(parsed);
    }

    template <class E>
    void finish(std::vector<ParseState<E>>& parsed) {
        using P = Pack<E>;
        // ---- dedup: per event_id bucket the earliest copy wins; every other
        // copy's filed event (if it has one) is dropped where it lies
        struct DedupState {
            std::vector<u64> table;
            std::vector<const DedupRec<E>*> blocks;
            std::vector<u32> lens;
        };
        std::vector<DedupState> dedup(threads);
        run_tasks(dedup, event_buckets, [&](DedupState& st, size_t b) {
            st.blocks.clear();
            st.lens.clear();
            size_t n = 0;
            for (auto& p : parsed) {
                for (size_t j = 0; j < p.dd.num_blocks(b); ++j) {
                    auto [blk, len] = p.dd.block(b, j);
                    st.blocks.push_back(blk);
                    st.lens.push_back(len);
                }
                n += p.dd.size(b);
            }
            if (!n) return;
            size_t cap = pow2_at_least(2 * n), mask = cap - 1;
            st.table.assign(cap, 0);
            auto rec = [&](u64 ref) -> const DedupRec<E>& { return st.blocks[ref >> 7][ref & 127]; };
            const E dropped = ~E(0);
            for (size_t bi = 0; bi < st.blocks.size(); ++bi) {
                const DedupRec<E>* blk = st.blocks[bi];
                for (u32 i = 0; i < st.lens[bi]; ++i) {
                    const DedupRec<E>& r = blk[i];
                    u64 ref = (u64(bi) << 7) | i;
                    u64 h = key_hash(r.eid);
                    u64 tag = h >> 32;
                    u64 entry = (tag << 32) | (ref + 1);
                    for (size_t s = h & mask;; s = (s + 1) & mask) {
                        u64 cur = st.table[s];
                        if (!cur) {
                            st.table[s] = entry;
                            break;
                        }
                        if ((cur >> 32) == tag) {
                            const DedupRec<E>& o = rec((cur & 0xFFFFFFFFu) - 1);
                            if (o.eid == r.eid) {
                                if (r.ingest < o.ingest) {  // the new copy wins
                                    st.table[s] = entry;
                                    if (o.ev) o.ev->ev = dropped;
                                } else if (r.ev) {
                                    r.ev->ev = dropped;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        });
        timer.lap("dedup");

        // ---- sessions per person bucket, biggest buckets first
        const u32 ncamp = u32(catalog->campaign_ids.size());
        const u32 ncat = u32(catalog->categories.size());
        std::vector<std::pair<size_t, u32>> order(person_buckets);
        for (size_t p = 0; p < person_buckets; ++p) {
            size_t n = 0;
            for (auto& d : parsed) n += d.ev.size(p) + d.ck.size(p);
            order[p] = {n, u32(p)};
        }
        std::sort(order.begin(), order.end(), [](auto& a, auto& b) { return a.first > b.first; });

        struct SessionState {
            PersonWork<E> work;
            KeyMap<u32> local{1024};
            std::vector<Key> names, cids, raw_cids;
            std::vector<u32> lids, tmp_lids, offsets, pos, ck_lids, ck_offsets, camps, raw_camps, hist;
            std::vector<E> raw, tmp, evs, cks, ck_raw;
            SessionState(const Window& w, u32 c, u32 k) : work(w, c, k) {}
        };
        std::vector<SessionState> sess;
        sess.reserve(threads);
        size_t kept = 0;
        for (auto& d : parsed)
            for (size_t p = 0; p < person_buckets; ++p) kept += d.ev.size(p);
        for (size_t t = 0; t < threads; ++t) {
            sess.emplace_back(w, ncamp, ncat);
            // about one user-day row per 8 kept events: reserve (untouched) room for 2x that
            sess.back().work.rows.reserve(kept / threads / 4 + 1024);
        }
        run_tasks(sess, person_buckets, [&](SessionState& st, size_t task) {
            size_t p = order[task].second;
            size_t n = 0;
            for (auto& d : parsed) n += d.ev.size(p);
            if (!n) return;
            st.local.reset(n / 48 + 16);
            st.names.clear();
            st.offsets.assign(1, 0);
            st.raw.resize(n);
            st.lids.resize(n);
            size_t k = 0;
            u64 max_hi = 0;
            const E dropped = ~E(0);
            for (auto& d : parsed) {
                d.ev.for_each(p, [&](const PersonEvent<E>& e) {
                    if (e.ev == dropped) return;  // a later copy of an event (dedup)
                    // records are packed to 8 bytes: copy keys out before taking
                    // references (a Key& assumes 16-byte alignment)
                    const Key person = e.person;
                    u32 lid = st.local.get_or_insert(person, [&] {
                        st.names.push_back(person);
                        st.offsets.push_back(0);
                        return u32(st.names.size() - 1);
                    });
                    st.offsets[lid + 1] += 1;
                    st.raw[k] = e.ev;
                    st.lids[k++] = lid;
                    max_hi = std::max(max_hi, P::hi(e.ev));
                });
            }
            n = k;  // copies dropped by dedup are gone
            st.raw.resize(n);
            st.lids.resize(n);
            if (!n) return;
            // the whole bucket in time order; the stable placement keeps it per person
            radix_by_time<E>(st.raw, st.lids, st.tmp, st.tmp_lids, max_hi, st.hist);
            const size_t people = st.names.size();
            for (size_t i = 0; i < people; ++i) st.offsets[i + 1] += st.offsets[i];
            st.evs.resize(n);
            st.pos.assign(st.offsets.begin(), st.offsets.end() - 1);
            for (k = 0; k < n; ++k) st.evs[st.pos[st.lids[k]]++] = st.raw[k];

            // clicks of persons with events, likewise in time order
            st.ck_raw.clear();
            st.ck_lids.clear();
            st.raw_cids.clear();
            st.raw_camps.clear();
            max_hi = 0;
            for (auto& d : parsed) {
                d.ck.for_each(p, [&](const PersonClick<E>& c) {
                    const u32* lid = st.local.find(c.person);
                    if (!lid) return;
                    u64 rel = c.rel;
                    st.ck_raw.push_back(P::pair(rel, st.ck_raw.size()));
                    st.ck_lids.push_back(*lid);
                    const Key cid = c.cid;  // packed member: copy before binding a reference
                    st.raw_cids.push_back(cid);
                    st.raw_camps.push_back(c.camp);
                    max_hi = std::max(max_hi, rel);
                });
            }
            radix_by_time<E>(st.ck_raw, st.ck_lids, st.tmp, st.tmp_lids, max_hi, st.hist);
            const size_t m = st.ck_raw.size();
            st.ck_offsets.assign(people + 1, 0);
            for (u32 l : st.ck_lids) st.ck_offsets[l + 1] += 1;
            for (size_t i = 0; i < people; ++i) st.ck_offsets[i + 1] += st.ck_offsets[i];
            st.cks.resize(m);
            st.cids.resize(m);
            st.camps.resize(m);
            st.pos.assign(st.ck_offsets.begin(), st.ck_offsets.end() - 1);
            for (k = 0; k < m; ++k) {
                u32 q = st.pos[st.ck_lids[k]]++;
                size_t idx = P::lo(st.ck_raw[k]);
                st.cks[q] = P::pair(P::hi(st.ck_raw[k]), q);
                st.cids[q] = st.raw_cids[idx];
                st.camps[q] = st.raw_camps[idx];
            }
            for (size_t l = 0; l < people; ++l) {
                st.work.run(st.names[l], st.evs.data() + st.offsets[l], st.offsets[l + 1] - st.offsets[l],
                            st.cks.data() + st.ck_offsets[l], st.ck_offsets[l + 1] - st.ck_offsets[l], st.cids.data(),
                            st.camps.data());
            }
        });
        timer.lap("sessions");

        // ---- output
        std::string ud_dir = out_dir + "/user_daily", cd_dir = out_dir + "/campaign_daily";
        for (auto* dir : {&ud_dir, &cd_dir}) {
            std::error_code ec;
            std::filesystem::create_directories(*dir, ec);
            for (auto it = std::filesystem::directory_iterator(*dir, ec); !ec && it != std::filesystem::directory_iterator();
                 it.increment(ec))
                if (it->path().extension() == ".parquet") std::filesystem::remove(it->path(), ec);
        }
        size_t total_rows = 0;
        for (auto& s : sess) total_rows += s.work.rows.size();
        std::vector<int> dummy(threads);
        run_tasks(dummy, sess.size() + 1, [&](int&, size_t i) {
            if (i == sess.size()) {
                write_campaigns(sess, cd_dir);
                return;
            }
            auto& rows = sess[i].work.rows;
            if (rows.empty() && !(i == 0 && total_rows == 0)) return;
            char name[48];
            std::snprintf(name, sizeof name, "/part-%05zu.parquet", i);
            write_users(rows, ud_dir + name);
        });
        timer.lap("output");
        if (timer.on) {
            struct rusage u{};
            getrusage(RUSAGE_SELF, &u);
            std::fprintf(stderr, "[%8s] faults=%ld maxrss=%ldMB threads=%zu\n", "", u.ru_minflt, u.ru_maxrss / 1024, threads);
        }
    }

    void write_users(const std::vector<UserRow>& rows, const std::string& path) {
        using parquet::Column, parquet::Kind;
        std::vector<Column> cols;
        cols.push_back({"user_id", Kind::String, {}});
        cols.push_back({"day", Kind::Date, {}});
        cols.push_back({"sessions", Kind::Int64, {}});
        cols.push_back({"events", Kind::Int64, {}});
        cols.push_back({"revenue_cents", Kind::Int64, {}});
        cols.push_back({"top_category", Kind::OptString, {}});
        size_t n = rows.size();
        cols[0].values.reserve(n * 14);
        cols[1].values.reserve(n * 4);
        for (int c = 2; c < 5; ++c) cols[c].values.reserve(n * 8);
        cols[5].present.reserve(n);
        for (auto& r : rows) {
            cols[0].add_key(r.name);
            cols[1].add_i32(r.day);
            cols[2].add_i64(r.sessions);
            cols[3].add_i64(r.events);
            cols[4].add_i64(i64(r.revenue));
            bool has = r.top != kNoCategory;
            cols[5].present.push_back(has);
            if (has) cols[5].add_string(catalog->categories[r.top]);
        }
        parquet::write_file(path, cols, n);
    }

    template <class S>
    void write_campaigns(std::vector<S>& sess, const std::string& dir) {
        using parquet::Column, parquet::Kind;
        const u32 ncamp = u32(catalog->campaign_ids.size());
        std::vector<std::array<u64, 4>> sum(size_t(ncamp + 1) * w.days);
        for (auto& s : sess) {
            auto& cells = s.work.cells.cells;
            for (size_t i = 0; i < sum.size(); ++i)
                for (int k = 0; k < 4; ++k) sum[i][k] += cells[i][k];
        }
        std::vector<Column> cols;
        cols.push_back({"campaign_id", Kind::String, {}});
        cols.push_back({"channel", Kind::String, {}});
        cols.push_back({"day", Kind::Date, {}});
        cols.push_back({"sessions", Kind::Int64, {}});
        cols.push_back({"purchasing_sessions", Kind::Int64, {}});
        cols.push_back({"revenue_cents", Kind::Int64, {}});
        cols.push_back({"assisted_sessions", Kind::Int64, {}});
        size_t rows = 0;
        std::string id;
        for (u32 c = 0; c <= ncamp; ++c) {
            for (u32 d = 0; d < w.days; ++d) {
                auto& v = sum[size_t(c) * w.days + d];
                if (!v[0] && !v[3]) continue;
                id.clear();
                if (c == ncamp) {
                    id = "organic";
                    cols[1].add_string("organic");
                } else {
                    append_key(catalog->campaign_ids[c], id);
                    cols[1].add_string(catalog->channels[c]);
                }
                cols[0].add_string(id);
                cols[2].add_i32(i32(w.day0 + d));
                for (int k = 0; k < 4; ++k) cols[3 + k].add_i64(i64(v[k]));
                ++rows;
            }
        }
        parquet::write_file(dir + "/part-00000.parquet", cols, rows);
    }
};

}  // namespace
}  // namespace ss

// Big containers (the persons map, dedup tables, session arrays, output
// buffers) get their own huge-page mappings, like the arena.
void* operator new(size_t n) {
    void* p = n >= ss::kBigAlloc ? ss::big_alloc(n) : std::malloc(n ? n : 1);
    if (!p) ss::die("out of memory (%zu bytes)", n);
    return p;
}
void* operator new[](size_t n) { return operator new(n); }
void operator delete(void* p) noexcept {
    if (!ss::big_free(p)) std::free(p);
}
void operator delete[](void* p) noexcept { operator delete(p); }
void operator delete(void* p, size_t) noexcept { operator delete(p); }
void operator delete[](void* p, size_t) noexcept { operator delete(p); }

// CPUs this process may use: the hardware count, limited by the affinity
// mask and a cgroup CPU quota (hardware_concurrency() sees neither, so a
// container capped at 8 CPUs on a large host would oversubscribe badly).
static long available_cpus() {
    long n = long(std::thread::hardware_concurrency());
    if (n <= 0) n = 8;
#ifdef __linux__
    cpu_set_t set;
    if (sched_getaffinity(0, sizeof set, &set) == 0) n = std::min<long>(n, CPU_COUNT(&set));
    auto quota = [&](long q, long period) {
        if (q > 0 && period > 0) n = std::min(n, std::max(1L, (q + period - 1) / period));
    };
    if (FILE* f = std::fopen("/sys/fs/cgroup/cpu.max", "r")) {  // cgroup v2: "<quota|max> <period>"
        char q[32] = {};
        long period = 0;
        if (std::fscanf(f, "%31s %ld", q, &period) == 2 && std::strcmp(q, "max") != 0) quota(std::atol(q), period);
        std::fclose(f);
    } else if (FILE* fq = std::fopen("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "r")) {  // cgroup v1
        long q = -1, period = 0;
        if (std::fscanf(fq, "%ld", &q) != 1) q = -1;
        std::fclose(fq);
        if (FILE* fp = std::fopen("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "r")) {
            if (std::fscanf(fp, "%ld", &period) != 1) period = 0;
            std::fclose(fp);
        }
        quota(q, period);
    }
#endif
    return std::max(1L, n);
}

int main(int argc, char** argv) {
    const char* data = std::getenv("DATA_DIR");
    const char* out = std::getenv("OUTPUT_DIR");
    // the fixture runner also passes DATA_DIR and OUTPUT_DIR as the last two arguments
    if (argc >= 3) data = argv[argc - 2], out = argv[argc - 1];
    // NCPU, capped by what the container may actually run at once
    long threads = 0;
    if (const char* n = std::getenv("NCPU")) threads = std::atol(n);
    long usable = available_cpus();
    threads = threads > 0 ? std::min(threads, usable) : usable;
    threads = std::clamp<long>(threads, 1, 64);
    ss::Pipeline p(data ? data : "/data", out ? out : "/output", size_t(threads),
                   std::getenv("SHOPPING_PROFILE") != nullptr);
    p.run();
    std::fflush(stderr);
    _exit(0);  // the kernel reclaims everything; skip destructors
}
