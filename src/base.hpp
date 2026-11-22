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
// An ID of 1..10 bytes from [0-9A-Z_a-z] becomes a u64: each byte a 6-bit
// code (1..63, in ASCII order; 0 pads), the first byte in the top bits of the
// low 60. Integer order is byte-wise order (padding sorts first, as a prefix
// does) and equal integers mean equal strings; zero is never a key. Every
// ID the generator writes fits. Any other ID (another byte, or longer) is
// interned and keyed kInterned | index: equality stays exact and ordering
// falls back to the bytes.
using Key = u64;
constexpr Key kInterned = Key(1) << 63;

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
    return kInterned | it->second;
}

// Requires n >= 1 and 16 readable bytes at p (every input buffer is padded).
[[gnu::always_inline]] inline Key make_key(const char* p, size_t n) {
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
    return intern_long(p, n);
}
[[gnu::always_inline]] inline Key make_key(std::string_view s) { return make_key(s.data(), s.size()); }

// For values that are not inside a padded buffer.
inline Key make_key_copy(std::string_view s) {
    if (s.size() > 10) return intern_long(s.data(), s.size());
    char buf[32] = {};
    std::memcpy(buf, s.data(), s.size());
    return make_key(buf, s.size());
}

inline bool is_interned(Key k) { return (k & kInterned) != 0; }

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

inline void append_key(Key k, std::string& out) {
    if (is_interned(k)) [[unlikely]] {
        auto& t = long_ids();
        std::lock_guard<std::mutex> lock(t.mu);
        out += t.names[u32(k)];
        return;
    }
    char buf[16];
    out.append(buf, packed_bytes(k, buf));
}

[[gnu::cold, gnu::noinline]] inline bool key_less_slow(Key a, Key b) {
    if (a == b) return false;
    std::string x, y;
    append_key(a, x);
    append_key(b, y);
    return x < y;  // std::string compares as unsigned bytes
}
inline bool key_less(Key a, Key b) {
    if (((a | b) & kInterned) != 0) [[unlikely]] return key_less_slow(a, b);
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
