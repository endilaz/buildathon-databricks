// Fast parsing paths against simple references, on mutated and random
// inputs followed by garbage (the fast paths read past each field).
#include "../src/text.hpp"
#include <random>

using namespace ss;

static std::mt19937_64 rng(12345);
static int failures = 0;

#define CHECK(cond, ...)                          \
    do {                                          \
        if (!(cond)) {                            \
            if (++failures <= 20) {               \
                std::fprintf(stderr, __VA_ARGS__); \
                std::fputc('\n', stderr);         \
            }                                     \
        }                                         \
    } while (0)

// A copy of s followed by random bytes.
static std::string padded(const std::string& s) {
    std::string b = s;
    for (int i = 0; i < 64; ++i) b.push_back(char(rng()));
    return b;
}

static std::string mutate(std::string s, const std::string& alphabet) {
    int edits = int(rng() % 3);
    for (int i = 0; i < edits && !s.empty(); ++i) s[rng() % s.size()] = alphabet[rng() % alphabet.size()];
    if (rng() % 10 == 0) s.resize(rng() % (s.size() + 1));
    if (rng() % 10 == 0) s += std::string(rng() % 8, '0' + char(rng() % 10));
    if (rng() % 20 == 0) s.insert(0, std::string(rng() % 25, '0'));
    return s;
}

static void timestamps() {
    const std::string bases[] = {"2026-10-03T23:58:10Z", "2028-02-29T00:00:00Z", "1969-12-31T23:59:59Z",
                                 "2100-02-28T12:00:00Z", "2099-12-31T23:59:59Z", "0000-03-01T00:00:00Z",
                                 "9999-12-31T23:59:59Z", "1970-01-01T00:00:00Z"};
    const std::string alphabet = "0123456789-:TZ +tz.\xb9\x80\xff";
    for (int iter = 0; iter < 3000000; ++iter) {
        std::string s = mutate(bases[rng() % 8], alphabet);
        if (s.size() < 19) continue;
        std::string b = padded(s);
        i64 x = 0, y = 0;
        bool a = parse_civil(b.data(), x), r = parse_civil_ref(b.data(), y);
        CHECK(a == r && (!a || x == y), "civil %s: fast %d %lld ref %d %lld", s.c_str(), a, (long long)x, r,
              (long long)y);
    }
    // every calendar day of years around the table's edges, at a fixed time
    for (u32 y : {0u, 1600u, 1900u, 1969u, 1970u, 1971u, 2000u, 2024u, 2028u, 2098u, 2099u, 2100u, 2400u, 9999u})
        for (u32 m = 0; m <= 13; ++m)
            for (u32 d = 0; d <= 32; ++d) {
                char buf[96];
                std::snprintf(buf, sizeof buf, "%04u-%02u-%02uT12:34:56Z", y, m, d);
                std::string b = padded(buf);
                i64 x = 0, z = 0;
                bool a = parse_civil(b.data(), x), r = parse_civil_ref(b.data(), z);
                CHECK(a == r && (!a || x == z), "calendar %s", buf);
            }
}

static void epoch_digits() {
    const std::string bases[] = {"1791071890000", "0001791071890000", "86400000", "9223372036854775807",
                                 "9223372036854775808", "18446744073709551615", "0", "00000000", "123"};
    const std::string alphabet = "0123456789/:a \xb0";
    for (int iter = 0; iter < 3000000; ++iter) {
        std::string s = mutate(bases[rng() % 9], alphabet);
        if (rng() % 8 == 0) {
            s.clear();
            size_t n = rng() % 26;
            for (size_t i = 0; i < n; ++i) s.push_back('0' + char(rng() % 10));
        }
        std::string b = padded(s);
        Field f(b.data(), s.size());
        i64 x = 0, y = 0;
        bool a = parse_epoch_digits(f, x), r = parse_epoch_digits_ref(f, y);
        CHECK(a == r && (!a || x == y), "digits '%s': fast %d %lld ref %d %lld", s.c_str(), a, (long long)x, r,
              (long long)y);
    }
}

static void product_numbers() {
    const std::string alphabet = "0123456789pq/:\xb0";
    for (int iter = 0; iter < 2000000; ++iter) {
        std::string s = "p";
        size_t n = rng() % 9;
        for (size_t i = 0; i < n; ++i) s.push_back(alphabet[rng() % (rng() % 4 ? 10 : alphabet.size())]);
        if (rng() % 10 == 0) s[0] = 'q';
        std::string b = padded(s);
        Field f(b.data(), s.size());
        u32 x = 0, y = 0;
        bool a = prefixed_number(f, 'p', x), r = prefixed_number_ref(f, 'p', y);
        CHECK(a == r && (!a || x == y), "product '%s': fast %d %u ref %d %u", s.c_str(), a, x, r, y);
    }
}

// The scanner against a direct reading of the dialect rules.
template <int N>
static bool split_ref(const std::string& line, std::string (&out)[N]) {
    std::vector<std::string> fields;
    size_t s = 0;
    for (;;) {
        size_t e = line.find(',', s);
        fields.push_back(line.substr(s, e == std::string::npos ? std::string::npos : e - s));
        if (e == std::string::npos) break;
        s = e + 1;
    }
    if (fields.size() != N) return false;
    for (int i = 0; i < N; ++i) {
        std::string& f = fields[i];
        if (f.size() >= 2 && f.front() == '"' && f.back() == '"') f = f.substr(1, f.size() - 2);
        if (f.find('"') != std::string::npos) return false;
        out[i] = f;
    }
    return true;
}

static void scanner() {
    const std::string alphabet = "ab,\"\r 1";
    for (int iter = 0; iter < 2000; ++iter) {
        std::vector<std::string> lines;
        std::string text;
        size_t count = 1 + rng() % 300;
        for (size_t i = 0; i < count; ++i) {
            std::string l;
            size_t n = rng() % (rng() % 4 == 0 ? 150 : 14);
            for (size_t j = 0; j < n; ++j) l.push_back(alphabet[rng() % alphabet.size()]);
            lines.push_back(l);
            text += l;
            if (i + 1 < count || rng() % 2) text += '\n';
        }
        std::string buf = text + std::string(128, '\0');
        Input in{buf.data(), text.size(), 0};
        // for_lines skips the first line as a header
        std::vector<std::vector<std::string>> got;
        for_lines<3>(in, 0, in.len, [&](Field (&f)[3]) { got.push_back({std::string(f[0]), std::string(f[1]), std::string(f[2])}); });
        std::vector<std::vector<std::string>> want;
        for (size_t i = 1; i < lines.size(); ++i) {
            if (i + 1 == lines.size() && lines[i].empty() && text.back() == '\n') break;
            std::string f[3];
            if (split_ref<3>(lines[i], f)) want.push_back({f[0], f[1], f[2]});
        }
        CHECK(got == want, "scanner case %d: got %zu lines, want %zu", iter, got.size(), want.size());
    }
}

int main() {
    timestamps();
    epoch_digits();
    product_numbers();
    scanner();
    if (failures) {
        std::fprintf(stderr, "%d failures\n", failures);
        return 1;
    }
    std::puts("parse tests passed");
}
