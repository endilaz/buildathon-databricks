// Input mappings (padded so fixed-width loads may run past any field) and
// the bump arena behind the radix-scatter buckets.
#pragma once
#include "base.hpp"
#include <filesystem>
#include <tuple>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

namespace ss {

struct Input {
    const char* data = nullptr;
    size_t len = 0;
    size_t mapped = 0;
};

inline size_t page_size() {
    static size_t page = size_t(sysconf(_SC_PAGESIZE));
    return page;
}

// Maps a file read-only with at least one zero page after its bytes.
inline Input map_input(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) die("cannot open %s", path.c_str());
    struct stat st{};
    if (fstat(fd, &st) != 0) die("cannot stat %s", path.c_str());
    size_t len = size_t(st.st_size), page = page_size();
    size_t total = (len + page - 1) / page * page + page;
    void* base = mmap(nullptr, total, PROT_READ, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (base == MAP_FAILED) die("cannot reserve %zu bytes for %s", total, path.c_str());
    if (len) {
        void* p = mmap(base, len, PROT_READ, MAP_PRIVATE | MAP_FIXED, fd, 0);
        if (p == MAP_FAILED) die("cannot map %s", path.c_str());
    }
    close(fd);
    return {static_cast<const char*>(base), len, total};
}

inline void unmap_input(const Input& in) {
    if (in.data) munmap(const_cast<char*>(in.data), in.mapped);
}

// Faults in the page-cache pages of [s, e) with one call (Linux 5.14+).
inline void populate(const Input& in, size_t s, size_t e) {
#ifdef __linux__
    uintptr_t a = (uintptr_t(in.data) + s) & ~uintptr_t(4095);
    uintptr_t b = uintptr_t(in.data) + std::min(e, in.len);
    if (b > a) madvise(reinterpret_cast<void*>(a), b - a, 22 /* MADV_POPULATE_READ */);
#else
    (void)in, (void)s, (void)e;
#endif
}

inline std::vector<std::string> csv_files(const std::filesystem::path& dir) {
    std::vector<std::string> out;
    std::error_code ec;
    for (auto it = std::filesystem::directory_iterator(dir, ec); !ec && it != std::filesystem::directory_iterator();
         it.increment(ec)) {
        auto& p = it->path();
        if (p.extension() == ".csv" && it->is_regular_file(ec)) out.push_back(p.string());
    }
    std::sort(out.begin(), out.end());
    return out;
}

// Allocations of 1 MB or more get their own 2 MB-aligned mapping with huge
// pages asked for (so first touches fault in 2 MB at a time, not 4 KB). A
// 64-byte header in front records the mapping; such a block is the only kind
// whose address is 64 bytes past a 2 MB boundary with the magic in front
// (malloc returns 16 bytes past a chunk header).
constexpr size_t kBigAlloc = size_t(1) << 20;
constexpr uintptr_t kHugePage = uintptr_t(2) << 20;
constexpr u64 kBigMagic = 0x6c6c612d67696273ULL;
struct BigHeader {
    u64 magic;
    void* base;
    size_t len;
    u64 pad[5];
};
static_assert(sizeof(BigHeader) == 64);

inline void* big_alloc(size_t n) {
    size_t len = (n + sizeof(BigHeader) + kHugePage - 1) / kHugePage * kHugePage + kHugePage;
    void* raw = mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0);
    if (raw == MAP_FAILED) return nullptr;
    uintptr_t aligned = (uintptr_t(raw) + kHugePage - 1) & ~(kHugePage - 1);
#ifdef __linux__
    madvise(reinterpret_cast<void*>(aligned), uintptr_t(raw) + len - aligned, MADV_HUGEPAGE);
#endif
    auto* h = reinterpret_cast<BigHeader*>(aligned);
    h->magic = kBigMagic;
    h->base = raw;
    h->len = len;
    return h + 1;
}
// Frees p if big_alloc made it; false otherwise.
inline bool big_free(void* p) {
    if (!p || (uintptr_t(p) & (kHugePage - 1)) != sizeof(BigHeader)) return false;
    auto* h = static_cast<BigHeader*>(p) - 1;
    if (h->magic != kBigMagic) return false;
    h->magic = 0;
    munmap(h->base, h->len);
    return true;
}

// ---------------------------------------------------------------- arena

inline void* map_anon(size_t bytes) {
    constexpr size_t huge = size_t(2) << 20;
    size_t len = (bytes + huge - 1) / huge * huge;
    void* p = mmap(nullptr, len + huge, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0);
    if (p == MAP_FAILED) die("cannot map %zu bytes", len);
    uintptr_t aligned = (uintptr_t(p) + huge - 1) & ~uintptr_t(huge - 1);
#ifdef __linux__
    madvise(reinterpret_cast<void*>(aligned), len, MADV_HUGEPAGE);
#endif
    return reinterpret_cast<void*>(aligned);
}

// One address-space reservation shared by every arena and cut into slabs
// with an atomic bump, so the hot phases make no mmap calls.
struct Reservation {
    std::atomic<uintptr_t> next{0};
    uintptr_t end = 0;
    void reserve(size_t bytes) {
        uintptr_t p = uintptr_t(map_anon(bytes));
        next.store(p);
        end = p + bytes;
    }
    static Reservation& global() {
        static Reservation r;
        return r;
    }
};

class Arena {
    static constexpr size_t kSlab = size_t(2) << 20;
    uintptr_t cur_ = 0, end_ = 0;
    // memory no longer read (already faulted in), used before fresh slabs
    std::vector<std::pair<uintptr_t, uintptr_t>> free_;

public:
    void recycle(const void* p, size_t bytes) { free_.emplace_back(uintptr_t(p), uintptr_t(p) + bytes); }
    void take_free(Arena& from) {
        free_.insert(free_.end(), from.free_.begin(), from.free_.end());
        from.free_.clear();
    }
    void* alloc(size_t bytes) {
        uintptr_t p = (cur_ + 63) & ~uintptr_t(63);
        while (p + bytes > end_ && !free_.empty()) {
            std::tie(cur_, end_) = free_.back();
            free_.pop_back();
            p = (cur_ + 63) & ~uintptr_t(63);
        }
        if (p + bytes > end_) [[unlikely]] {
            size_t size = std::max(kSlab, (bytes + 64 + kSlab - 1) / kSlab * kSlab);
            auto& r = Reservation::global();
            uintptr_t s = r.next.fetch_add(size);
            if (s == 0 || s + size > r.end) {
                size = std::max(size, size_t(64) << 20);
                s = uintptr_t(map_anon(size));
            }
            cur_ = s;
            end_ = s + size;
            p = (cur_ + 63) & ~uintptr_t(63);
        }
        cur_ = p + bytes;
        return reinterpret_cast<void*>(p);
    }
};

// Radix-scatter buffer: per bucket, a list of fixed-size blocks from the
// owner's arena. A push is a store and a counter bump; nothing is copied.
template <class T>
class Scatter {
public:
    static constexpr u32 kBlock = 128;

private:
    // The hot half of a bucket (written on every push) is kept apart from
    // its block list, so 2048 buckets' cursors fit in 32 KB of L1.
    struct Cursor {
        T* cur = nullptr;
        u32 fill = kBlock;
    };
    Arena arena_;
    std::vector<Cursor> cursors_;
    std::vector<std::vector<T*>> blocks_;

    [[gnu::noinline]] void grow(size_t i) {
        Cursor& c = cursors_[i];
        c.cur = static_cast<T*>(arena_.alloc(sizeof(T) * kBlock));
        c.fill = 0;
        blocks_[i].push_back(c.cur);
    }

public:
    explicit Scatter(size_t n = 1) : cursors_(n), blocks_(n) {}
    Arena& arena() { return arena_; }
    // Hands bucket i's blocks to `to` for reuse; nothing may read them afterwards.
    void recycle_bucket(size_t i, Arena& to) const {
        for (T* p : blocks_[i]) to.recycle(p, sizeof(T) * kBlock);
    }
    inline void push(size_t i, const T& x) {
        Cursor& c = cursors_[i];
        if (c.fill == kBlock) [[unlikely]] grow(i);
        c.cur[c.fill++] = x;
    }
    size_t size(size_t i) const {
        size_t n = blocks_[i].size();
        return n == 0 ? 0 : (n - 1) * kBlock + cursors_[i].fill;
    }
    size_t num_blocks(size_t i) const { return blocks_[i].size(); }
    // Block j of bucket i and its length.
    std::pair<const T*, u32> block(size_t i, size_t j) const {
        auto& b = blocks_[i];
        return {b[j], j + 1 == b.size() ? cursors_[i].fill : kBlock};
    }
    template <class F>
    void for_each(size_t i, F f) const {
        auto& b = blocks_[i];
        for (size_t j = 0; j < b.size(); ++j) {
            const T* p = b[j];
            u32 n = j + 1 == b.size() ? cursors_[i].fill : kBlock;
            for (u32 k = 0; k < n; ++k) f(p[k]);
        }
    }
};

}  // namespace ss
