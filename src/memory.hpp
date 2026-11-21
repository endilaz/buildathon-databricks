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

// Asks for transparent huge pages on the 2 MB-aligned interior of a big
// untouched allocation (the scorer's THP mode may be "madvise").
inline void advise_huge(void* p, size_t n) {
#ifdef __linux__
    constexpr uintptr_t huge = uintptr_t(2) << 20;
    uintptr_t a = (uintptr_t(p) + huge - 1) & ~(huge - 1), b = (uintptr_t(p) + n) & ~(huge - 1);
    if (b > a) madvise(reinterpret_cast<void*>(a), b - a, MADV_HUGEPAGE);
#else
    (void)p, (void)n;
#endif
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
    struct Bucket {
        T* cur = nullptr;
        u32 fill = kBlock;
        std::vector<T*> blocks;
    };
    Arena arena_;
    std::vector<Bucket> buckets_;

    [[gnu::noinline]] void grow(Bucket& b) {
        b.cur = static_cast<T*>(arena_.alloc(sizeof(T) * kBlock));
        b.fill = 0;
        b.blocks.push_back(b.cur);
    }

public:
    explicit Scatter(size_t n = 1) : buckets_(n) {}
    Arena& arena() { return arena_; }
    // Hands bucket i's blocks to `to` for reuse; nothing may read them afterwards.
    void recycle_bucket(size_t i, Arena& to) const {
        for (T* p : buckets_[i].blocks) to.recycle(p, sizeof(T) * kBlock);
    }
    inline void push(size_t i, const T& x) {
        Bucket& b = buckets_[i];
        if (b.fill == kBlock) [[unlikely]] grow(b);
        b.cur[b.fill++] = x;
    }
    size_t size(size_t i) const {
        auto& b = buckets_[i];
        return b.blocks.empty() ? 0 : (b.blocks.size() - 1) * kBlock + b.fill;
    }
    size_t num_blocks(size_t i) const { return buckets_[i].blocks.size(); }
    // Block j of bucket i and its length.
    std::pair<const T*, u32> block(size_t i, size_t j) const {
        auto& b = buckets_[i];
        return {b.blocks[j], j + 1 == b.blocks.size() ? b.fill : kBlock};
    }
    template <class F>
    void for_each(size_t i, F f) const {
        auto& b = buckets_[i];
        for (size_t j = 0; j < b.blocks.size(); ++j) {
            const T* p = b.blocks[j];
            u32 n = j + 1 == b.blocks.size() ? b.fill : kBlock;
            for (u32 k = 0; k < n; ++k) f(p[k]);
        }
    }
};

}  // namespace ss
