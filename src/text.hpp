// CSV line splitting with 64-byte separator bitmasks, timestamp and price
// parsing. Inputs carry at least 64 readable bytes past their end.
#pragma once
#include "memory.hpp"
#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace ss {

// ---------------------------------------------------------------- scanner

struct Masks {
    u64 nl, sep, quote;  // bit i: byte i is '\n'; ',' or '\n'; '"'
};

inline Masks block_masks(const char* p) {
#if defined(__AVX2__)
    __m256i a = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
    __m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32));
    auto eq = [&](char c) {
        __m256i v = _mm256_set1_epi8(c);
        u64 lo = u32(_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)));
        u64 hi = u32(_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)));
        return lo | (hi << 32);
    };
    u64 nl = eq('\n');
    return {nl, nl | eq(','), eq('"')};
#elif defined(__aarch64__)
    const uint8_t* q = reinterpret_cast<const uint8_t*>(p);
    uint8x16_t v0 = vld1q_u8(q), v1 = vld1q_u8(q + 16), v2 = vld1q_u8(q + 32), v3 = vld1q_u8(q + 48);
    static const uint8_t weights[16] = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    uint8x16_t w = vld1q_u8(weights);
    auto eq = [&](uint8_t c) {
        uint8x16_t s = vdupq_n_u8(c);
        uint8x16_t a = vpaddq_u8(vandq_u8(vceqq_u8(v0, s), w), vandq_u8(vceqq_u8(v1, s), w));
        uint8x16_t b = vpaddq_u8(vandq_u8(vceqq_u8(v2, s), w), vandq_u8(vceqq_u8(v3, s), w));
        a = vpaddq_u8(a, b);
        a = vpaddq_u8(a, a);
        return vgetq_lane_u64(vreinterpretq_u64_u8(a), 0);
    };
    u64 nl = eq('\n');
    return {nl, nl | eq(','), eq('"')};
#else
    Masks m{0, 0, 0};
    for (int i = 0; i < 64; ++i) {
        u64 bit = u64(1) << i;
        if (p[i] == '\n') m.nl |= bit;
        if (p[i] == ',') m.sep |= bit;
        if (p[i] == '"') m.quote |= bit;
    }
    m.sep |= m.nl;
    return m;
#endif
}

using Field = std::string_view;

// Walks `data[0, len)` from a line start. The end of the data acts as a
// newline. A field is well formed when it has no quotes, or is wrapped in
// one pair with none inside; a per-line quote count checks the latter.
class Scanner {
    const char* d_;
    size_t len_, base_;
    u64 seps_ = 0, nl_ = 0, quote_ = 0;
    u32 quotes_ = 0;       // quotes in [start, base_)
    u32 line_quotes_ = 0;  // quotes in [start, current line start)

    void load() {
        Masks m = block_masks(d_ + base_);
        size_t left = len_ - base_;
        if (left < 64) {
            u64 keep = (u64(1) << left) - 1;
            m.nl = (m.nl & keep) | (u64(1) << left);
            m.sep = (m.sep & keep) | m.nl;
            m.quote &= keep;
        }
        nl_ = m.nl;
        quote_ = m.quote;
        seps_ = m.sep;
    }

public:
    Scanner(const char* data, size_t len, size_t pos) : d_(data), len_(len), base_(pos) { load(); }

    // Splits the line at p (where the previous line ended) into N fields.
    // Returns the next line's start; `ok` says whether the line is well formed.
    template <int N>
    inline size_t line(size_t p, Field (&f)[N], bool& ok) {
        int k = 0;
        u32 expect = 0;
        for (;;) {
            while (!seps_) {
                quotes_ += u32(__builtin_popcountll(quote_));
                base_ += 64;
                load();
            }
            u32 bit = u32(__builtin_ctzll(seps_));
            seps_ &= seps_ - 1;
            size_t q = base_ + bit;
            const char* a = d_ + p;
            size_t n = q - p;
            // branch-free: fields are quoted at random, so a branch here mispredicts
            // (for n == 0 the last byte read is the field's own separator)
            u32 quoted = u32(n >= 2) & u32(a[0] == '"') & u32(a[n - (n != 0)] == '"');
            expect += 2 * quoted;
            if (k < N) f[k] = Field(a + quoted, n - 2 * size_t(quoted));
            if ((nl_ >> bit) & 1) {
                u32 seen = quotes_ + u32(__builtin_popcountll(quote_ & ((u64(1) << bit) - 1)));
                ok = k == N - 1 && seen - line_quotes_ == expect;
                line_quotes_ = seen;
                return q + 1;
            }
            ++k;
            p = q + 1;
        }
    }
};

// Calls fn(fields) for each well-formed line starting in [s, e). A file's
// first line is its header.
template <int N, class F>
inline void for_lines(const Input& in, size_t s, size_t e, F fn) {
    const char* d = in.data;
    size_t len = in.len, p;
    if (s == 0 || d[s - 1] != '\n') {
        const void* nl = std::memchr(d + s, '\n', len - s);
        p = nl ? size_t(static_cast<const char*>(nl) - d) + 1 : len;
    } else {
        p = s;
    }
    if (p >= e || p >= len) return;
    Scanner sc(d, len, p);
    Field f[N];
    while (p < e && p < len) {
        bool ok;
        p = sc.line<N>(p, f, ok);
        if (ok) fn(f);
    }
}

// ---------------------------------------------------------------- time

constexpr i64 kSecond = 1000, kMinute = 60 * kSecond, kHour = 60 * kMinute, kDay = 24 * kHour;

constexpr i64 floor_div(i64 a, i64 b) { return a / b - ((a % b != 0) & ((a < 0) != (b < 0))); }

constexpr i64 days_from_civil(i64 y, i64 m, i64 d) {
    y -= m <= 2;
    i64 era = floor_div(y, 400);
    i64 yoe = y - era * 400;
    i64 doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    return era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
}

constexpr bool is_leap(u32 y) { return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0); }
constexpr u8 kMonthDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

inline u32 dig(char c) { return u32(u8(c)) - u32('0'); }

// Reference (scalar) parsers; the fast paths below are tested against them.
inline bool parse_civil_ref(const char* p, i64& out) {
    u32 y0 = dig(p[0]), y1 = dig(p[1]), y2 = dig(p[2]), y3 = dig(p[3]);
    u32 m0 = dig(p[5]), m1 = dig(p[6]), d0 = dig(p[8]), d1 = dig(p[9]);
    u32 h0 = dig(p[11]), h1 = dig(p[12]), n0 = dig(p[14]), n1 = dig(p[15]), s0 = dig(p[17]), s1 = dig(p[18]);
    u32 bad = (y0 > 9) | (y1 > 9) | (y2 > 9) | (y3 > 9) | (m0 > 9) | (m1 > 9) | (d0 > 9) | (d1 > 9) |
              (h0 > 9) | (h1 > 9) | (n0 > 9) | (n1 > 9) | (s0 > 9) | (s1 > 9);
    if (bad | (p[4] != '-') | (p[7] != '-') | (p[10] != 'T') | (p[13] != ':') | (p[16] != ':')) return false;
    u32 year = y0 * 1000 + y1 * 100 + y2 * 10 + y3, month = m0 * 10 + m1, day = d0 * 10 + d1;
    u32 hour = h0 * 10 + h1, minute = n0 * 10 + n1, second = s0 * 10 + s1;
    if (month - 1 > 11 || day == 0 || hour > 23 || minute > 59 || second > 59) return false;
    if (day > kMonthDays[month - 1] + u32(month == 2 && is_leap(year))) return false;
    out = days_from_civil(year, month, day) * kDay + i64(hour) * kHour + i64(minute) * kMinute + i64(second) * kSecond;
    return true;
}

inline bool parse_epoch_digits_ref(Field f, i64& out) {
    size_t n = f.size(), i = 0;
    if (n == 0) return false;
    while (i < n && f[i] == '0') ++i;
    if (n - i > 19) {
        for (; i < n; ++i)
            if (dig(f[i]) > 9) return false;
        return false;
    }
    u64 v = 0;
    for (; i < n; ++i) {
        u32 d = dig(f[i]);
        if (d > 9) return false;
        v = v * 10 + d;
    }
    if (v > u64(INT64_MAX)) return false;
    out = i64(v);
    return true;
}

// ---- fast paths: 8-byte loads (every field is followed by readable padding)

constexpr u64 kAsciiZeros = 0x3030303030303030ULL;
constexpr u64 kHighBits = 0x8080808080808080ULL;

// The bytes of x (input already XORed with '0's) under mask's high bits are
// all 0..9. A carry out of one byte only arises from a byte >= 0x8A, which is
// itself flagged, so the verdict is exact.
inline bool digits_ok(u64 x, u64 mask) { return ((x | (x + 0x7676767676767676ULL)) & mask) == 0; }
inline u32 byte_at(u64 x, int i) { return u32(x >> (8 * i)) & 0xFF; }

// For each month of 1970..2099: epoch day of its 1st, and its length.
constexpr u32 kTableYear0 = 1970, kTableYears = 130;
struct MonthTable {
    i32 first[kTableYears * 12];
    u8 days[kTableYears * 12];
};
constexpr MonthTable make_month_table() {
    MonthTable t{};
    for (u32 i = 0; i < kTableYears * 12; ++i) {
        u32 y = kTableYear0 + i / 12, m = i % 12 + 1;
        t.first[i] = i32(days_from_civil(y, m, 1));
        t.days[i] = u8(kMonthDays[m - 1] + u32(m == 2 && is_leap(y)));
    }
    return t;
}
inline constexpr MonthTable kMonths = make_month_table();

// YYYY-MM-DDTHH:MM:SS at p, in epoch milliseconds.
[[gnu::always_inline]] inline bool parse_civil(const char* p, i64& out) {
    // little-endian loads: byte i of a word is p[offset + i]
    u64 a = load64(p);                   // Y Y Y Y - M M -
    u64 b = load64(p + 8);               // D D T H H : M M
    u64 c = load64(p + 16) & 0xFFFFFF;   // : S S
    bool seps = (a & 0xFF0000FF00000000ULL) == 0x2D00002D00000000ULL &&  // '-' at 4 and 7
                (b & 0x0000FF0000FF0000ULL) == 0x00003A0000540000ULL &&  // 'T' at 2, ':' at 5
                (c & 0xFF) == 0x3A;                                      // ':'
    u64 xa = a ^ kAsciiZeros, xb = b ^ kAsciiZeros, xc = c ^ 0x303030;
    if (!seps || !digits_ok(xa, 0x0080800080808080ULL) || !digits_ok(xb, 0x8080008080008080ULL) ||
        !digits_ok(xc, 0x808000))
        return false;
    u32 year = byte_at(xa, 0) * 1000 + byte_at(xa, 1) * 100 + byte_at(xa, 2) * 10 + byte_at(xa, 3);
    u32 month = byte_at(xa, 5) * 10 + byte_at(xa, 6);
    u32 day = byte_at(xb, 0) * 10 + byte_at(xb, 1);
    u32 hour = byte_at(xb, 3) * 10 + byte_at(xb, 4);
    u32 minute = byte_at(xb, 6) * 10 + byte_at(xb, 7);
    u32 second = byte_at(xc, 1) * 10 + byte_at(xc, 2);
    if (month - 1 > 11 || day == 0 || hour > 23 || minute > 59 || second > 59) return false;
    i64 days;
    if (year - kTableYear0 < kTableYears) {
        u32 i = (year - kTableYear0) * 12 + month - 1;
        if (day > kMonths.days[i]) return false;
        days = kMonths.first[i] + i64(day) - 1;
    } else {
        if (day > kMonthDays[month - 1] + u32(month == 2 && is_leap(year))) return false;
        days = days_from_civil(year, month, day);
    }
    out = days * kDay + i64(hour) * kHour + i64(minute) * kMinute + i64(second) * kSecond;
    return true;
}

// 8 digit values (one per byte, first digit lowest) -> their decimal value.
inline u64 eight_digits(u64 x) {
    x = x * 10 + (x >> 8);  // byte 2i: d[2i] * 10 + d[2i + 1]
    return (((x & 0x000000FF000000FFULL) * (100 + (1000000ULL << 32))) +
            (((x >> 16) & 0x000000FF000000FFULL) * (1 + (10000ULL << 32)))) >> 32;
}

// <prefix><1..7 digits without a leading zero> -> the number.
inline bool prefixed_number_ref(Field id, char prefix, u32& num) {
    size_t n = id.size();
    if (n < 2 || n > 8 || id[0] != prefix || id[1] == '0') return false;
    u32 v = 0;
    for (size_t i = 1; i < n; ++i) {
        u32 d = dig(id[i]);
        if (d > 9) return false;
        v = v * 10 + d;
    }
    num = v;
    return true;
}
[[gnu::always_inline]] inline bool prefixed_number(Field id, char prefix, u32& num) {
    size_t n = id.size();
    if (n < 2 || n > 8 || id[0] != prefix || id[1] == '0') return false;
    size_t m = n - 1;
    u64 x = load64(id.data() + 1) ^ kAsciiZeros;
    if (!digits_ok(x, kHighBits >> (8 * (8 - m)))) return false;
    // shift the digits to the top (zeros enter as leading digits; bytes past the field fall off)
    num = u32(eight_digits(x << (8 * (8 - m))));
    return true;
}

// Epoch milliseconds as ASCII digits (any length, leading zeros allowed).
// A value beyond int64 is reported as invalid: it is outside every window.
[[gnu::always_inline]] inline bool parse_epoch_digits(Field f, i64& out) {
    const char* p = f.data();
    size_t n = f.size();
    if (n == 0) return false;
    for (size_t i = 0; i < n; i += 8) {
        size_t left = n - i;
        u64 mask = left >= 8 ? kHighBits : kHighBits >> (8 * (8 - left));
        if (!digits_ok(load64(p + i) ^ kAsciiZeros, mask)) return false;
    }
    while (n > 19 && *p == '0') ++p, --n;
    if (n > 19) return false;
    u64 v = 0;
    size_t head = n % 8;
    for (size_t j = 0; j < head; ++j) v = v * 10 + dig(p[j]);
    for (size_t j = head; j < n; j += 8) v = v * 100000000 + eight_digits(load64(p + j) ^ kAsciiZeros);
    if (v > u64(INT64_MAX)) return false;
    out = i64(v);
    return true;
}


// Any of: YYYY-MM-DDTHH:MM:SSZ, YYYY-MM-DDTHH:MM:SS(+|-)HH:MM, epoch-ms digits.
[[gnu::always_inline]] inline bool parse_ts(Field f, i64& out) {
    size_t n = f.size();
    const char* p = f.data();
    if ((n == 20 || n == 25) && p[4] == '-') {
        if (!parse_civil(p, out)) return false;
        if (n == 20) return p[19] == 'Z';
        char sign = p[19];
        u32 a = dig(p[20]), b = dig(p[21]), c = dig(p[23]), d = dig(p[24]);
        if ((sign != '+' && sign != '-') || p[22] != ':' || a > 9 || b > 9 || c > 9 || d > 9) return false;
        u32 oh = a * 10 + b, om = c * 10 + d;
        if (oh > 23 || om > 59) return false;
        i64 off = i64(oh) * kHour + i64(om) * kMinute;
        out += sign == '+' ? -off : off;
        return true;
    }
    return parse_epoch_digits(f, out);
}

[[gnu::always_inline]] inline bool parse_f1(Field f, i64& out) {
    return f.size() == 20 && f[19] == 'Z' && parse_civil(f.data(), out);
}

// A purchase price: ASCII digits, value 1..10,000,000.
[[gnu::always_inline]] inline bool parse_price(Field f, u32& out) {
    if (f.empty()) return false;
    u32 v = 0;
    for (char c : f) {
        u32 d = dig(c);
        if (d > 9) return false;
        v = v * 10 + d;
        if (v > 10000000) return false;
    }
    if (v == 0) return false;
    out = v;
    return true;
}

}  // namespace ss
