// Per-person work: sessions, user-day rows, credited campaign, assists.
#pragma once
#include "text.hpp"
#include <array>

namespace ss {

constexpr i64 kGap = 30 * kMinute;
constexpr i64 kWeek = 7 * kDay;
constexpr u32 kNoCategory = UINT32_MAX;

// A person's events and clicks live in memory as single integers that sort
// in time order. E is u64 when the window, prices and categories fit:
//   event  = rel_ts << 32 | price << 8 | category
//   click  = rel_ts << 32 | position
// and u128 (64-bit halves) otherwise.
template <class E>
struct Pack;
template <>
struct Pack<u64> {
    static u64 event(u64 rel, u32 cat, u32 price) { return rel << 32 | u64(price) << 8 | cat; }
    static u32 cat(u64 e) { return u32(e & 0xFF); }
    static u32 price(u64 e) { return u32(e) >> 8; }
    static u64 pair(u64 hi, u64 lo) { return hi << 32 | lo; }
    static u64 hi(u64 e) { return e >> 32; }
    static u64 lo(u64 e) { return e & 0xFFFFFFFFu; }
};
template <>
struct Pack<u128> {
    static u128 event(u64 rel, u32 cat, u32 price) { return u128(rel) << 64 | u128(price) << 32 | cat; }
    static u32 cat(u128 e) { return u32(e); }
    static u32 price(u128 e) { return u32(u64(e) >> 32); }
    static u128 pair(u64 hi, u64 lo) { return u128(hi) << 64 | lo; }
    static u64 hi(u128 e) { return u64(e >> 64); }
    static u64 lo(u128 e) { return u64(e); }
};

// Time is sorted at this granularity (2^kCoarse ms) by the bucket radix
// sort; each person's array is then finished by an insertion pass, which is
// nearly free on nearly sorted input.
constexpr int kCoarse = 10;

// Stable LSD radix sort of the pairs (a[i], b[i]) by Pack<E>::hi(a[i]) >> kCoarse,
// 11 bits a pass; max_hi bounds hi.
template <class E, class B>
void radix_by_time(std::vector<E>& a, std::vector<B>& b, std::vector<E>& ta, std::vector<B>& tb, u64 max_hi,
                   std::vector<u32>& hist) {
    max_hi >>= kCoarse;
    using P = Pack<E>;
    const size_t n = a.size();
    if (n < 2 || max_hi == 0) return;
    ta.resize(n);
    tb.resize(n);
    hist.resize(2048);
    const int bits = 64 - __builtin_clzll(max_hi);
    for (int shift = 0; shift < bits; shift += 11) {
        std::fill(hist.begin(), hist.end(), 0);
        for (size_t i = 0; i < n; ++i) ++hist[(P::hi(a[i]) >> (shift + kCoarse)) & 2047];
        u32 sum = 0;
        bool one_digit = false;
        for (auto& h : hist) {
            u32 c = h;
            one_digit |= c == n;
            h = sum;
            sum += c;
        }
        if (one_digit) continue;
        for (size_t i = 0; i < n; ++i) {
            u32 q = hist[(P::hi(a[i]) >> (shift + kCoarse)) & 2047]++;
            ta[q] = a[i];
            tb[q] = b[i];
        }
        a.swap(ta);
        b.swap(tb);
    }
}

// Finishes a nearly sorted array.
template <class T>
inline void insertion_finish(T* a, size_t n) {
    for (size_t i = 1; i < n; ++i) {
        if (!(a[i] < a[i - 1])) [[likely]] continue;
        T x = a[i];
        size_t j = i;
        do {
            a[j] = a[j - 1];
            --j;
        } while (j > 0 && x < a[j - 1]);
        a[j] = x;
    }
}

struct Window {
    i64 start, end, click_start;  // events [start, end), clicks [click_start, end)
    i64 day0;                     // epoch day of start
    u32 days;
};

#pragma pack(push, 8)
struct UserRow {
    Key name;
    i32 day;
    u32 sessions, events, top;
    u64 revenue;
};
#pragma pack(pop)

// (campaign, day) -> sessions, purchasing sessions, revenue, assisted sessions.
struct CampaignCells {
    u32 days = 0;
    std::vector<std::array<u64, 4>> cells;
    CampaignCells(u32 campaigns_with_organic, u32 d) : days(d), cells(size_t(campaigns_with_organic) * d) {}
    std::array<u64, 4>& at(u32 campaign, u32 day) { return cells[size_t(campaign) * days + day]; }
};

// Up to this many sessions x clicks, assists come straight from each
// session's window of clicks; above it, from merged per-campaign intervals.
constexpr size_t kDirectAssists = 4096;

template <class E>
class PersonWork {
    using P = Pack<E>;
    struct Session {
        u64 start;  // relative to the click window start
        u32 day, events, lo, hi;
        u64 revenue;
    };
    const Window& w_;
    u32 organic_;
    std::vector<Session> sessions_;
    std::vector<u64> spend_;
    std::vector<u32> touched_;
    std::vector<u64> stamp_;
    u64 stamp_id_ = 0;
    std::vector<E> pairs_;
    std::vector<u64> starts_;
    std::vector<u32> day_end_;

public:
    std::vector<UserRow> rows;
    CampaignCells cells;

    PersonWork(const Window& w, u32 campaigns, u32 categories)
        : w_(w), organic_(campaigns), spend_(categories), stamp_(campaigns + 1), cells(campaigns + 1, w.days) {}

    void credit(u32 campaign, const Session& s) {
        auto& c = cells.at(campaign, s.day);
        c[0] += 1;
        c[1] += s.revenue != 0;
        c[2] += s.revenue;
    }

    // ev: the person's events, in time order at kCoarse granularity; ck:
    // click keys pair(ts - click_start, pos), likewise, with cid[pos] and
    // camp[pos] the rest of each click. Both are finished here.
    void run(Key name, E* ev, size_t n, E* ck, size_t m, const Key* cid, const u32* camp) {
        if (kCoarse) {
            insertion_finish(ev, n);
            insertion_finish(ck, m);
        }
        sessions_.clear();
        const u64 offset = u64(w_.start - w_.click_start);
        for (size_t i = 0; i < n;) {
            size_t lo = i;
            u64 first = P::hi(ev[i]), prev = first, revenue = 0;
            while (i < n && P::hi(ev[i]) - prev <= u64(kGap)) {
                prev = P::hi(ev[i]);
                revenue += P::price(ev[i]);
                ++i;
            }
            i64 start = w_.start + i64(first);
            u32 day = u32(floor_div(start, kDay) - w_.day0);
            sessions_.push_back({first + offset, day, u32(i - lo), u32(lo), u32(i), revenue});
        }
        const size_t ns = sessions_.size();

        // user_daily
        for (size_t k = 0; k < ns;) {
            u32 day = sessions_[k].day;
            UserRow row{name, i32(w_.day0 + day), 0, 0, kNoCategory, 0};
            for (; k < ns && sessions_[k].day == day; ++k) {
                const Session& s = sessions_[k];
                row.sessions += 1;
                row.events += s.events;
                if (!s.revenue) continue;
                row.revenue += s.revenue;
                for (u32 e = s.lo; e < s.hi; ++e) {
                    u32 price = P::price(ev[e]);
                    if (!price) continue;
                    u32 c = P::cat(ev[e]);
                    if (!spend_[c]) touched_.push_back(c);
                    spend_[c] += price;
                }
            }
            if (!touched_.empty()) {
                u32 best = touched_[0];
                for (u32 c : touched_)
                    if (spend_[c] > spend_[best] || (spend_[c] == spend_[best] && c < best)) best = c;
                row.top = best;
                for (u32 c : touched_) spend_[c] = 0;
                touched_.clear();
            }
            rows.push_back(row);
        }

        // credit
        if (m == 0) {
            for (auto& s : sessions_) credit(organic_, s);
            return;
        }
        const bool direct = ns * m <= kDirectAssists;
        size_t j = 0, run = 0, chosen_run = SIZE_MAX, lo = 0;
        u32 chosen = organic_;
        for (auto& s : sessions_) {
            while (j < m && P::hi(ck[j]) <= s.start) {
                if (j == 0 || P::hi(ck[j]) != P::hi(ck[j - 1])) run = j;
                ++j;
            }
            u32 campaign = organic_;
            if (j > 0 && s.start - P::hi(ck[j - 1]) <= u64(kWeek)) {
                if (chosen_run != run) {
                    // the latest timestamp; among equal ones the smallest click_id
                    size_t best = P::lo(ck[run]);
                    for (size_t q = run + 1; q < j; ++q) {
                        size_t pos = P::lo(ck[q]);
                        if (key_less(cid[pos], cid[best])) best = pos;
                    }
                    chosen_run = run;
                    chosen = camp[best];
                }
                campaign = chosen;
            }
            credit(campaign, s);
            if (direct) {
                while (lo < j && P::hi(ck[lo]) + u64(kWeek) < s.start) ++lo;
                ++stamp_id_;
                for (size_t q = lo; q < j; ++q) {
                    u32 c = camp[P::lo(ck[q])];
                    if (stamp_[c] != stamp_id_) {
                        stamp_[c] = stamp_id_;
                        cells.at(c, s.day)[3] += 1;
                    }
                }
            }
        }
        if (direct) return;

        // A campaign assists exactly the sessions starting inside the union of
        // [click.ts, click.ts + 7 days] over its clicks.
        starts_.resize(ns);
        day_end_.resize(ns);
        for (size_t k = ns; k-- > 0;) {
            starts_[k] = sessions_[k].start;
            day_end_[k] = (k + 1 < ns && sessions_[k + 1].day == sessions_[k].day) ? day_end_[k + 1] : u32(k + 1);
        }
        pairs_.resize(m);
        for (size_t q = 0; q < m; ++q) pairs_[q] = P::pair(camp[P::lo(ck[q])], P::hi(ck[q]));
        std::sort(pairs_.begin(), pairs_.end());
        for (size_t q = 0; q < m;) {
            u32 c = u32(P::hi(pairs_[q]));
            u64 a = P::lo(pairs_[q]), b = a + u64(kWeek);
            for (++q;; ++q) {
                bool same = q < m && u32(P::hi(pairs_[q])) == c;
                if (same && P::lo(pairs_[q]) <= b) {
                    b = P::lo(pairs_[q]) + u64(kWeek);
                    continue;
                }
                size_t x = std::lower_bound(starts_.begin(), starts_.end(), a) - starts_.begin();
                size_t y = std::upper_bound(starts_.begin(), starts_.end(), b) - starts_.begin();
                while (x < y) {
                    size_t e = std::min<size_t>(day_end_[x], y);
                    cells.at(c, sessions_[x].day)[3] += e - x;
                    x = e;
                }
                if (!same) break;
                a = P::lo(pairs_[q]);
                b = a + u64(kWeek);
            }
        }
    }
};

}  // namespace ss
