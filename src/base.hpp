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
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
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
// An ID of 1..15 bytes becomes a u128 holding its bytes from the most
// significant end, zero padded, with the length in the lowest byte. Comparing
// two such integers compares the strings byte-wise (the length byte settles
// a string against its own NUL-extended forms), and equal integers mean equal
// strings. Zero is never a key. Longer IDs are interned and keyed
// (index << 8) | 0xFF; equality stays exact and ordering falls back to bytes.
using Key = u128;
constexpr u8 kLongTag = 0xFF;

struct LongIds {
    std::mutex mu;
    std::unordered_map<std::string, u32> index;
    std::vector<std::string> names;
};
inline LongIds& long_ids() {
    static LongIds ids;
    return ids;
}

[[gnu::cold, gnu::noinline]] inline Key intern_long(const char* p, size_t n) {
    auto& t = long_ids();
    std::lock_guard<std::mutex> lock(t.mu);
    auto [it, fresh] = t.index.try_emplace(std::string(p, n), u32(t.names.size()));
    if (fresh) t.names.emplace_back(p, n);
    return (Key(it->second) << 8) | kLongTag;
}

// Requires n >= 1 and 16 readable bytes at p (every input buffer is padded).
[[gnu::always_inline]] inline Key make_key(const char* p, size_t n) {
    if (n <= 15) [[likely]] {
        u64 hi = __builtin_bswap64(load64(p));
        u64 lo = __builtin_bswap64(load64(p + 8));
        Key k = (Key(hi) << 64) | lo;
        k &= ~Key(0) << (8 * (16 - n));
        return k | Key(n);
    }
    return intern_long(p, n);
}
[[gnu::always_inline]] inline Key make_key(std::string_view s) { return make_key(s.data(), s.size()); }

// For values that are not inside a padded buffer.
inline Key make_key_copy(std::string_view s) {
    if (s.size() > 15) return intern_long(s.data(), s.size());
    char buf[32] = {};
    std::memcpy(buf, s.data(), s.size());
    return make_key(buf, s.size());
}

inline bool is_long(Key k) { return u8(k) == kLongTag; }

inline void append_key(Key k, std::string& out) {
    if (is_long(k)) [[unlikely]] {
        auto& t = long_ids();
        std::lock_guard<std::mutex> lock(t.mu);
        out += t.names[u32(k >> 8)];
        return;
    }
    size_t n = u8(k);
    for (size_t i = 0; i < n; ++i) out.push_back(char(u8(k >> (8 * (15 - i)))));
}

[[gnu::cold, gnu::noinline]] inline bool key_less_slow(Key a, Key b) {
    if (a == b) return false;
    std::string x, y;
    append_key(a, x);
    append_key(b, y);
    return x < y;  // std::string compares as unsigned bytes
}
inline bool key_less(Key a, Key b) {
    if ((u8(a) == kLongTag) || (u8(b) == kLongTag)) [[unlikely]] return key_less_slow(a, b);
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
[[gnu::always_inline]] inline u64 key_hash(Key k) {
    u64 hi = u64(k >> 64), lo = u64(k);
    return mix64(hi ^ ((lo * 0x9E3779B97F4A7C15ULL) >> 7 | (lo * 0x9E3779B97F4A7C15ULL) << 57));
}

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
