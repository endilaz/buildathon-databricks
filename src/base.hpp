// Shared primitives: integer types, packed ID keys, hashing, prefetch,
// a task-pulling thread helper, and stage timing.
#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <sys/mman.h>
#include <unistd.h>

namespace ss {

using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;
using i32 = int32_t;
using i64 = int64_t;
using u128 = unsigned __int128;

[[noreturn]] inline void die(const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    std::fputs("shopping-sessions: ", stderr);
    std::vfprintf(stderr, fmt, ap);
    std::fputc('\n', stderr);
    va_end(ap);
    _exit(1);
}

inline u64 load64(const char* p) {
    u64 v;
    std::memcpy(&v, p, 8);
    return v;
}
inline u32 load32(const char* p) {
    u32 v;
    std::memcpy(&v, p, 4);
    return v;
}

// ---------------------------------------------------------------- keys
//
// An ID of 1..10 bytes from [0-9A-Z_a-z] becomes a u64: each byte a 6-bit
// code (1..63, in ASCII order; 0 pads), the first byte in the top bits of the
// low 60. Integer order is byte-wise order (padding sorts first, as a prefix
// does) and equal integers mean equal strings; zero is never a key.
//
// Every other ID is long: about 1 in 8 of the generator's IDs (those with a
// 32-hex-digit suffix), and any ID with another byte. A long ID is keyed
// kLong | 63 bits of a hash of its bytes, computed where it is read, with no
// shared state. Two distinct long IDs share a key with probability about
// n^2 / 2^64 (under 1e-5 for 10M of them). Where a long ID's bytes are needed
// (person names, click_id ties, output), record_key also files them in one
// insert-only table, with 16 more hash bits: a key shared by two recorded IDs
// is noticed (collided), and so is a table too full to take more (full).
// Either restarts the run (main.cpp): a fuller table gets a bigger one, a
// collision gets SHOPPING_EXACT_IDS=1, where every long ID is interned under
// a lock and keyed by its index, exactly but slowly.
using Key = u64;
constexpr Key kLong = Key(1) << 63;

struct KeyCodes {
    u8 code[256];  // 0xFF: not in the alphabet
    char byte[64];
};
constexpr KeyCodes make_key_codes() {
    KeyCodes t{};
    for (int c = 0; c < 256; ++c) t.code[c] = 0xFF;
    u8 next = 1;
    for (int c = 0; c < 256; ++c) {
        bool in = (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || c == '_' || (c >= 'a' && c <= 'z');
        if (in) {
            t.code[c] = next;
            t.byte[next] = char(c);
            ++next;
        }
    }
    return t;
}
inline constexpr KeyCodes kKeyCodes = make_key_codes();
static_assert(kKeyCodes.code[u8('z')] == 63 && kKeyCodes.code[u8('0')] == 1);

inline u64 mum(u64 a, u64 b) {
    u128 r = u128(a) * b;
    return u64(r) ^ u64(r >> 64);
}

// Two independent 64-bit hashes of the n bytes at p; reads nothing outside them.
inline void id_hash(const char* p, size_t n, u64& h1, u64& h2) {
    constexpr u64 k1 = 0xA0761D6478BD642FULL, k2 = 0xE7037ED1A0B428DBULL;
    u64 a = 0x243F6A8885A308D3ULL ^ n, b = 0x13198A2E03707344ULL ^ (u64(n) << 32);
    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        u64 w = load64(p + i);
        a = mum(a ^ w, k1);
        b = mum(b ^ w, k2);
    }
    if (i < n) {
        u64 w = 0;
        std::memcpy(&w, p + i, n - i);
        a = mum(a ^ w, k1);
        b = mum(b ^ w, k2);
    }
    h1 = mum(a ^ 0x8EBC6AF09C88C6E3ULL, 0x589965CC75374CC3ULL);
    h2 = mum(b ^ 0x1D8E4E27C47D124FULL, 0xEB44ACCAB455D165ULL);
}

inline void cpu_relax() {
#if defined(__x86_64__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield");
#endif
}

struct LongIds {
    // val: the recorded bytes (u32 length, then the ID) | 16 check bits << 48
    struct Slot {
        u64 key, val;
    };
    static constexpr u64 kPtrMask = (u64(1) << 48) - 1;
    Slot* slots = nullptr;
    size_t mask = 0;
    std::atomic<bool> collided{false}, full{false};
    const bool exact = std::getenv("SHOPPING_EXACT_IDS") != nullptr;
    // exact mode
    std::mutex mu;
    std::unordered_map<std::string, u32> index;
    std::deque<std::string> names;

    // n slots (a power of two), zero pages until touched, huge pages asked for
    void reserve(size_t n) {
        constexpr size_t huge = size_t(2) << 20;
        size_t len = (n * sizeof(Slot) + huge - 1) / huge * huge;
        void* p = mmap(nullptr, len + huge, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0);
        if (p == MAP_FAILED) die("cannot map %zu bytes", len);
        uintptr_t aligned = (uintptr_t(p) + huge - 1) & ~uintptr_t(huge - 1);
#ifdef __linux__
        madvise(reinterpret_cast<void*>(aligned), len, MADV_HUGEPAGE);
#endif
        slots = reinterpret_cast<Slot*>(aligned);
        mask = n - 1;
    }
    size_t capacity() const { return slots ? mask + 1 : 0; }
};
inline LongIds& long_ids() {
    static LongIds ids;
    return ids;
}

// Recorded bytes live in per-thread chunks that are never freed.
struct IdArena {
    char* cur = nullptr;
    char* end = nullptr;
};
inline thread_local IdArena tl_id_arena;
inline const char* store_id(const char* p, size_t n) {
    auto& a = tl_id_arena;
    size_t need = (4 + n + 7) & ~size_t(7);
    if (size_t(a.end - a.cur) < need) {
        size_t sz = std::max<size_t>(size_t(1) << 20, need);
        a.cur = static_cast<char*>(std::malloc(sz));
        if (!a.cur || (uintptr_t(a.cur + sz) >> 48)) die("cannot store long IDs");
        a.end = a.cur + sz;
    }
    u32 len = u32(n);
    std::memcpy(a.cur, &len, 4);
    std::memcpy(a.cur + 4, p, n);
    const char* r = a.cur;
    a.cur += need;
    return r;
}

// Files a long ID's bytes (k and check from make_key_check). A no-op in exact mode.
inline void record_long(Key k, u16 check, const char* p, size_t n) {
    auto& t = long_ids();
    if (t.exact) return;
    size_t i = size_t(k) & t.mask;
    for (int probe = 0; probe < 256; ++probe, i = (i + 1) & t.mask) {
        LongIds::Slot* s = &t.slots[i];
        u64 cur = __atomic_load_n(&s->key, __ATOMIC_RELAXED);
        if (cur == 0) {
            if (__atomic_compare_exchange_n(&s->key, &cur, k, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                __atomic_store_n(&s->val, u64(uintptr_t(store_id(p, n))) | u64(check) << 48, __ATOMIC_RELEASE);
                return;
            }
            // cur is now the key another thread put here
        }
        if (cur == k) {
            u64 v;
            while (!(v = __atomic_load_n(&s->val, __ATOMIC_ACQUIRE))) cpu_relax();
            if (u16(v >> 48) != check) t.collided.store(true, std::memory_order_relaxed);
            return;
        }
    }
    t.full.store(true, std::memory_order_relaxed);
}

[[gnu::noinline]] inline Key intern_exact(const char* p, size_t n) {
    auto& t = long_ids();
    std::lock_guard<std::mutex> lock(t.mu);
    auto [it, fresh] = t.index.try_emplace(std::string(p, n), u32(t.names.size()));
    if (fresh) t.names.emplace_back(p, n);
    return kLong | it->second;
}

[[gnu::noinline]] inline Key long_key(const char* p, size_t n, u16& check) {
    auto& t = long_ids();
    if (t.exact) [[unlikely]]
        return intern_exact(p, n);
    u64 h1, h2;
    id_hash(p, n, h1, h2);
#ifdef SS_KEY_BITS  // tests: few key bits, so long IDs collide
    h1 &= (u64(1) << SS_KEY_BITS) - 1;
#endif
    check = u16(h2);
    return kLong | (h1 >> 1);
}
template <bool Record>
inline Key long_key(const char* p, size_t n) {
    u16 check = 0;
    Key k = long_key(p, n, check);
    if constexpr (Record) record_long(k, check, p, n);
    return k;
}
inline void prefetch_long(Key k) {
    auto& t = long_ids();
    if (t.slots) __builtin_prefetch(&t.slots[size_t(k) & t.mask], 1, 3);
}

// Requires n >= 1 and 16 readable bytes at p (every input buffer is padded).
// make_key: IDs only compared for equality (event_id, product, campaign
// lookups); record_key: IDs whose bytes may be needed later.
template <bool Record>
[[gnu::always_inline]] inline Key make_key_as(const char* p, size_t n) {
    if (n <= 10) [[likely]] {
        u64 v = 0;
        u32 bad = 0;
        for (size_t i = 0; i < 10; ++i) {
            u32 c = i < n ? kKeyCodes.code[u8(p[i])] : 0;
            bad |= c;
            v = (v << 6) | (c & 63);
        }
        if (!(bad & 0xC0)) [[likely]] return v;
    }
    return long_key<Record>(p, n);
}
[[gnu::always_inline]] inline Key make_key(const char* p, size_t n) { return make_key_as<false>(p, n); }
[[gnu::always_inline]] inline Key make_key(std::string_view s) { return make_key_as<false>(s.data(), s.size()); }
[[gnu::always_inline]] inline Key record_key(const char* p, size_t n) { return make_key_as<true>(p, n); }
// Like make_key; a long ID also sets check, for a record_long later.
[[gnu::always_inline]] inline Key make_key_check(std::string_view s, u16& check) {
    const char* p = s.data();
    size_t n = s.size();
    if (n <= 10) [[likely]] {
        u64 v = 0;
        u32 bad = 0;
        for (size_t i = 0; i < 10; ++i) {
            u32 c = i < n ? kKeyCodes.code[u8(p[i])] : 0;
            bad |= c;
            v = (v << 6) | (c & 63);
        }
        if (!(bad & 0xC0)) [[likely]] return v;
    }
    return long_key(p, n, check);
}
[[gnu::always_inline]] inline Key record_key(std::string_view s) { return make_key_as<true>(s.data(), s.size()); }

inline bool is_long(Key k) { return (k & kLong) != 0; }

// A packed key's bytes into out (at most 10); returns their count.
inline size_t packed_bytes(Key k, char* out) {
    size_t n = 0;
    for (int i = 0; i < 10; ++i) {
        u32 c = u32(k >> (54 - 6 * i)) & 63;
        if (!c) break;
        out[n++] = kKeyCodes.byte[c];
    }
    return n;
}

// A recorded long ID's bytes (once the threads recording it have joined).
inline std::string_view long_bytes(Key k) {
    auto& t = long_ids();
    if (t.exact) {
        std::lock_guard<std::mutex> lock(t.mu);
        return t.names[u32(k)];
    }
    for (size_t i = size_t(k) & t.mask;; i = (i + 1) & t.mask) {
        const auto& s = t.slots[i];
        if (s.key == k) {
            const char* p = reinterpret_cast<const char*>(uintptr_t(s.val & LongIds::kPtrMask));
            u32 n;
            std::memcpy(&n, p, 4);
            return {p + 4, n};
        }
        if (!s.key) die("long ID used before it was recorded");
    }
}

// Any key's bytes; buf (16 bytes) holds a packed key's.
inline std::string_view key_bytes(Key k, char* buf) {
    if (is_long(k)) [[unlikely]]
        return long_bytes(k);
    return {buf, packed_bytes(k, buf)};
}

inline void append_key(Key k, std::string& out) {
    char buf[16];
    out += key_bytes(k, buf);
}

[[gnu::noinline]] inline bool key_less_slow(Key a, Key b) {
    if (a == b) return false;
    char x[16], y[16];
    // string_view compares as unsigned bytes (char_traits<char>::compare)
    return key_bytes(a, x) < key_bytes(b, y);
}
inline bool key_less(Key a, Key b) {
    if (((a | b) & kLong) != 0) [[unlikely]] return key_less_slow(a, b);
    return a < b;
}

inline u64 mix64(u64 x) {
    x ^= x >> 32;
    x *= 0xd6e8feb86659fd93ULL;
    x ^= x >> 32;
    x *= 0xd6e8feb86659fd93ULL;
    x ^= x >> 32;
    return x;
}
[[gnu::always_inline]] inline u64 key_hash(Key k) { return mix64(k * 0x9E3779B97F4A7C15ULL); }

inline void prefetch(const void* p) { __builtin_prefetch(p, 0, 3); }

// ---------------------------------------------------------------- threads

// Runs `threads` workers; each owns states[t] and pulls task indexes
// 0..tasks-1 from a shared counter.
template <class State, class Work>
void run_tasks(std::vector<State>& states, size_t tasks, Work work) {
    std::atomic<size_t> next{0};
    auto body = [&](size_t t) {
        for (;;) {
            size_t i = next.fetch_add(1, std::memory_order_relaxed);
            if (i >= tasks) break;
            work(states[t], i);
        }
    };
    std::vector<std::thread> pool;
    for (size_t t = 1; t < states.size(); ++t) pool.emplace_back(body, t);
    body(0);
    for (auto& th : pool) th.join();
}

// ---------------------------------------------------------------- timing

struct Timer {
    bool on;
    std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now(), last = t0;
    explicit Timer(bool enabled) : on(enabled) {}
    void lap(const char* what) {
        if (!on) return;
        auto now = std::chrono::steady_clock::now();
        std::fprintf(stderr, "[%8.1f ms] %-24s %8.1f ms\n",
                     std::chrono::duration<double, std::milli>(now - t0).count(), what,
                     std::chrono::duration<double, std::milli>(now - last).count());
        last = now;
    }
};

}  // namespace ss
