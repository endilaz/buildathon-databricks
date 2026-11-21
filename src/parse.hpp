#pragma once
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace shopping {

class MappedFile {
    const char* bytes_ = nullptr;
    size_t size_ = 0;
public:
    explicit MappedFile(const std::filesystem::path& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("cannot open " + path.string());
        struct stat st{};
        if (fstat(fd, &st) != 0) { close(fd); throw std::runtime_error("cannot stat " + path.string()); }
        size_ = static_cast<size_t>(st.st_size);
        if (size_) {
            void* mapped = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0);
            if (mapped == MAP_FAILED) { close(fd); throw std::runtime_error("cannot map " + path.string()); }
            bytes_ = static_cast<const char*>(mapped);
        }
        close(fd);
    }
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    ~MappedFile() { if (bytes_) munmap(const_cast<char*>(bytes_), size_); }
    std::string_view view() const { return size_ ? std::string_view(bytes_, size_) : std::string_view{}; }
};

inline std::string_view csv_data(std::string_view file) {
    auto end = file.find('\n');
    return end == file.npos ? std::string_view{} : file.substr(end + 1);
}
inline std::string_view csv_chunk(std::string_view input, size_t lo, size_t hi) {
    auto align = [&](size_t offset) {
        if (offset == 0 || offset >= input.size() || input[offset - 1] == '\n') return offset;
        auto lf = input.find('\n', offset);
        return lf == input.npos ? input.size() : lf + 1;
    };
    size_t start = align(lo);
    return input.substr(start, align(hi) - start);
}

class CsvReader {
    std::string_view rest_;
public:
    explicit CsvReader(std::string_view data) : rest_(data) {}
    bool next(std::string_view& line) {
        if (rest_.empty()) return false;
        auto n = rest_.find('\n');
        if (n == rest_.npos) { line = rest_; rest_ = {}; }
        else { line = rest_.substr(0, n); rest_.remove_prefix(n + 1); }
        return true;
    }
};

template<size_t N> class CsvFields {
    std::array<std::string_view, N> fields_;
public:
    std::string_view operator[](size_t i) const { return fields_[i]; }
    bool parse(std::string_view line) {
        if (line.empty()) return false;
        const char* p = line.data();
        const char* end = p + line.size();
        for (size_t i = 0; i < N; ++i) {
            bool quoted = p < end && *p == '"';
            if (quoted) ++p;
            const char* start = p;
            while (p < end && *p != ',' && *p != '"') ++p;
            fields_[i] = {start, size_t(p - start)};
            if (quoted) {
                if (p == end || *p != '"') return false;
                ++p;
            }
            if (i + 1 == N) return p == end;
            if (p == end || *p != ',') return false;
            ++p;
        }
        return false;
    }
};

inline bool parse_i64(std::string_view text, int64_t& out) {
    if (text.empty()) return false;
    int64_t v = 0;
    for (unsigned char c : text) {
        unsigned d = c - unsigned('0');
        if (d > 9 || v > (std::numeric_limits<int64_t>::max() - d) / 10) return false;
        v = v * 10 + d;
    }
    out = v;
    return true;
}
inline int two_digits(const char* s) {
    unsigned a = static_cast<unsigned char>(s[0]) - unsigned('0');
    unsigned b = static_cast<unsigned char>(s[1]) - unsigned('0');
    return (a > 9 || b > 9) ? 100 : int(a * 10 + b);
}
inline int epoch_day(int year, int month, int day) {
    year -= month <= 2;
    int era = (year >= 0 ? year : year - 399) / 400;
    int y = year - era * 400;
    int doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    return era * 146097 + y * 365 + y / 4 - y / 100 + doy - 719468;
}
inline bool parse_timestamp(std::string_view s, int64_t& out) {
    bool iso = (s.size() == 20 || s.size() == 25) && s[4] == '-';
    if (!iso) return parse_i64(s, out);
    const char* p = s.data();
    if (p[7] != '-' || p[10] != 'T' || p[13] != ':' || p[16] != ':') return false;
    int century = two_digits(p), y = two_digits(p + 2);
    int year = century * 100 + y, month = two_digits(p + 5), day = two_digits(p + 8);
    int hour = two_digits(p + 11), minute = two_digits(p + 14), second = two_digits(p + 17);
    if (century == 100 || y == 100 || month < 1 || month > 12 || day < 1 ||
        hour > 23 || minute > 59 || second > 59) return false;
    constexpr int lengths[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    int leap = month == 2 && year % 4 == 0 && (year % 100 || year % 400 == 0);
    if (day > lengths[month-1] + leap) return false;
    int offset = 0;
    if (s.size() == 20) {
        if (p[19] != 'Z') return false;
    } else {
        if ((p[19] != '+' && p[19] != '-') || p[22] != ':') return false;
        int h = two_digits(p+20), m = two_digits(p+23);
        if (h > 23 || m > 59) return false;
        offset = (h*60 + m) * (p[19] == '+' ? 1 : -1);
    }
    out = int64_t(epoch_day(year, month, day)) * 86400000 +
          int64_t((hour*60 + minute - offset)*60 + second) * 1000;
    return true;
}
struct Window { int64_t start, end; };
inline Window read_manifest(const std::filesystem::path& path) {
    MappedFile file(path);
    auto json = file.view();
    auto get = [&](std::string_view name) {
        auto key = json.find(std::string("\"") + std::string(name) + "\"");
        auto colon = key == json.npos ? key : json.find(':', key + name.size() + 2);
        auto from = colon == json.npos ? colon : json.find('"', colon + 1);
        auto to = from == json.npos ? from : json.find('"', from + 1);
        int64_t value;
        if (to == json.npos || !parse_timestamp(json.substr(from + 1, to - from - 1), value))
            throw std::runtime_error("invalid manifest field " + std::string(name));
        return value;
    };
    return {get("window_start"), get("window_end")};
}

} // namespace shopping
