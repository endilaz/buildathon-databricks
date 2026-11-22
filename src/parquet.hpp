// Minimal Parquet writer: one row group, one uncompressed PLAIN data page
// (v1) per column, no statistics. Metadata in the Thrift compact protocol.
#pragma once
#include "base.hpp"
#include <deque>
#include <fcntl.h>
#include <sys/uio.h>

namespace ss::parquet {

enum class Kind { String, OptString, Date, Int64 };

struct Column {
    const char* name;
    Kind kind;
    std::string values;            // the PLAIN-encoded non-null values
    std::vector<u8> present = {};  // OptString only: 1 per row when not null

    void add_string(std::string_view s) {
        u32 n = u32(s.size());
        values.append(reinterpret_cast<const char*>(&n), 4);
        values.append(s);
    }
    void add_key(Key k) {
        if (is_interned(k)) [[unlikely]] {
            std::string s;
            append_key(k, s);
            add_string(s);
            return;
        }
        char buf[16];
        u32 n = u32(packed_bytes(k, buf + 4));
        std::memcpy(buf, &n, 4);
        values.append(buf, 4 + n);
    }
    void add_i32(i32 v) { values.append(reinterpret_cast<const char*>(&v), 4); }
    void add_i64(i64 v) { values.append(reinterpret_cast<const char*>(&v), 8); }
};

class Thrift {
    std::vector<int16_t> last_{0};

public:
    std::string out;
    void varint(u64 v) {
        while (v >= 0x80) {
            out.push_back(char(v | 0x80));
            v >>= 7;
        }
        out.push_back(char(v));
    }
    void zigzag(i64 v) { varint((u64(v) << 1) ^ u64(v >> 63)); }
    void field(int16_t id, u8 type) {
        int16_t delta = int16_t(id - last_.back());
        if (delta > 0 && delta <= 15) {
            out.push_back(char((delta << 4) | type));
        } else {
            out.push_back(char(type));
            zigzag(id);
        }
        last_.back() = id;
    }
    void i32_field(int16_t id, i64 v) { field(id, 5), zigzag(v); }
    void i64_field(int16_t id, i64 v) { field(id, 6), zigzag(v); }
    void string_field(int16_t id, std::string_view s) {
        field(id, 8);
        varint(s.size());
        out.append(s);
    }
    void list_field(int16_t id, u8 elem, size_t n) {
        field(id, 9);
        if (n < 15) {
            out.push_back(char((n << 4) | elem));
        } else {
            out.push_back(char(0xF0 | elem));
            varint(n);
        }
    }
    void begin_struct(int16_t id) { field(id, 12), last_.push_back(0); }
    void begin_element() { last_.push_back(0); }
    void end_struct() { out.push_back(0), last_.pop_back(); }
    void stop() { out.push_back(0); }
};

constexpr u8 kI32 = 5, kBinary = 8, kStruct = 12;

inline int physical(Kind k) { return k == Kind::Int64 ? 2 : k == Kind::Date ? 1 : 6; }

// The file as a sequence of byte ranges: small owned headers and the
// columns' own value buffers, so no column is ever copied before the write.
struct Pieces {
    std::deque<std::string> owned;  // deque: references stay valid as it grows
    std::vector<std::pair<const char*, size_t>> parts;
    size_t size = 0;
    void add(const std::string& s) {
        parts.emplace_back(s.data(), s.size());
        size += s.size();
    }
    void own(std::string s) { add(owned.emplace_back(std::move(s))); }
};

// The columns must outlive the returned pieces.
inline Pieces encode(std::vector<Column>& cols, size_t rows) {
    Pieces file;
    file.own("PAR1");
    struct Chunk {
        i64 offset, size;
    };
    std::vector<Chunk> chunks;
    if (rows) {
        for (auto& c : cols) {
            std::string levels;
            if (c.kind == Kind::OptString) {
                // definition levels: one bit-packed run of width 1, length-prefixed
                size_t groups = (rows + 7) / 8;
                Thrift h;
                h.varint((u64(groups) << 1) | 1);
                u32 n = u32(h.out.size() + groups);
                levels.append(reinterpret_cast<const char*>(&n), 4);
                levels += h.out;
                size_t base = levels.size();
                levels.resize(base + groups, 0);
                for (size_t i = 0; i < rows; ++i) levels[base + i / 8] |= char(c.present[i] << (i % 8));
            }
            size_t body = levels.size() + c.values.size();
            Thrift ph;
            ph.i32_field(1, 0);  // DATA_PAGE
            ph.i32_field(2, i64(body));
            ph.i32_field(3, i64(body));
            ph.begin_struct(5);
            ph.i32_field(1, i64(rows));
            ph.i32_field(2, 0);  // PLAIN
            ph.i32_field(3, 3);  // RLE
            ph.i32_field(4, 3);  // RLE
            ph.end_struct();
            ph.stop();
            i64 offset = i64(file.size);
            file.own(std::move(ph.out));
            if (!levels.empty()) file.own(std::move(levels));
            file.add(c.values);
            chunks.push_back({offset, i64(file.size) - offset});
        }
    }
    Thrift t;
    t.i32_field(1, 1);
    t.list_field(2, kStruct, cols.size() + 1);
    t.begin_element();
    t.string_field(4, "schema");
    t.i32_field(5, i64(cols.size()));
    t.end_struct();
    for (auto& c : cols) {
        t.begin_element();
        t.i32_field(1, physical(c.kind));
        t.i32_field(3, c.kind == Kind::OptString ? 1 : 0);  // OPTIONAL : REQUIRED
        t.string_field(4, c.name);
        if (c.kind == Kind::String || c.kind == Kind::OptString) {
            t.i32_field(6, 0);  // converted UTF8
            t.begin_struct(10);
            t.begin_struct(1);  // LogicalType.STRING
            t.end_struct();
            t.end_struct();
        } else if (c.kind == Kind::Date) {
            t.i32_field(6, 6);  // converted DATE
            t.begin_struct(10);
            t.begin_struct(6);  // LogicalType.DATE
            t.end_struct();
            t.end_struct();
        }
        t.end_struct();
    }
    t.i64_field(3, i64(rows));
    if (rows) {
        t.list_field(4, kStruct, 1);
        t.begin_element();
        t.list_field(1, kStruct, cols.size());
        i64 total = 0;
        for (size_t i = 0; i < cols.size(); ++i) {
            auto& c = cols[i];
            total += chunks[i].size;
            t.begin_element();
            t.i64_field(2, chunks[i].offset);
            t.begin_struct(3);
            t.i32_field(1, physical(c.kind));
            t.list_field(2, kI32, 2);
            t.zigzag(0);  // PLAIN
            t.zigzag(3);  // RLE
            t.list_field(3, kBinary, 1);
            t.varint(std::strlen(c.name));
            t.out += c.name;
            t.i32_field(4, 0);  // UNCOMPRESSED
            t.i64_field(5, i64(rows));
            t.i64_field(6, chunks[i].size);
            t.i64_field(7, chunks[i].size);
            t.i64_field(9, chunks[i].offset);
            t.end_struct();
            t.end_struct();
        }
        t.i64_field(2, total);
        t.i64_field(3, i64(rows));
        t.end_struct();
    } else {
        t.list_field(4, kStruct, 0);
    }
    t.string_field(6, "shopping-sessions");
    t.stop();
    u32 n = u32(t.out.size());
    t.out.append(reinterpret_cast<const char*>(&n), 4);
    t.out += "PAR1";
    file.own(std::move(t.out));
    return file;
}

inline void write_file(const std::string& path, std::vector<Column>& cols, size_t rows) {
    Pieces file = encode(cols, rows);
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) die("cannot create %s", path.c_str());
    std::vector<iovec> io;
    for (auto [p, n] : file.parts)
        if (n) io.push_back({const_cast<char*>(p), n});
    size_t i = 0;
    while (i < io.size()) {
        int count = int(std::min<size_t>(io.size() - i, 512));
        ssize_t w = writev(fd, io.data() + i, count);
        if (w <= 0) die("cannot write %s", path.c_str());
        // skip what was written, possibly ending inside an iovec
        size_t done = size_t(w);
        while (i < io.size() && done >= io[i].iov_len) done -= io[i++].iov_len;
        if (done) {
            io[i].iov_base = static_cast<char*>(io[i].iov_base) + done;
            io[i].iov_len -= done;
        }
    }
    close(fd);
}

}  // namespace ss::parquet
