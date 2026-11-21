#pragma once
#include "identity.hpp"
#include "work.hpp"

namespace shopping {

// A true duplicate repeats all its bits. Collisions only send additional IDs to
// the exact comparison path. Blocked hashing keeps each lookup in one word.
class DuplicateFilter {
    std::unique_ptr<std::atomic<uint64_t>[]> repeated_;
    size_t word_mask_;

    static uint64_t bits(uint64_t h) {
        return (uint64_t(1) << (h & 63)) |
               (uint64_t(1) << ((h >> 6) & 63)) |
               (uint64_t(1) << ((h >> 12) & 63));
    }
    static Text first_field(Text line) {
        auto comma = line.find(',');
        if (comma == Text::npos) return {};
        auto id = line.substr(0, comma);
        if (id.size() >= 2 && id.front() == '"' && id.back() == '"')
            id = id.substr(1, id.size() - 2);
        return id;
    }

public:
    DuplicateFilter(const Files& files, size_t threads) {
        size_t words = 1;
        // About 12–24 bits per typical event, capped at 64 MiB per bitset.
        while (words < files.bytes / 384 && words < (size_t(1) << 23)) words *= 2;
        word_mask_ = words - 1;
        auto seen = std::make_unique<std::atomic<uint64_t>[]>(words);
        repeated_ = std::make_unique<std::atomic<uint64_t>[]>(words);
        for (size_t i = 0; i < words; ++i) {
            seen[i].store(0, std::memory_order_relaxed);
            repeated_[i].store(0, std::memory_order_relaxed);
        }
        parallel(threads, files.chunks.size(), [&](size_t, size_t i) {
            CsvReader rows(files.chunks[i]);
            Text line;
            while (rows.next(line)) {
                auto id = first_field(line);
                if (id.empty()) continue;
                auto h = std::hash<Text>{}(id);
                size_t word = (h >> 18) & word_mask_;
                uint64_t mask = bits(h);
                uint64_t overlap = seen[word].fetch_or(mask, std::memory_order_relaxed) & mask;
                if (overlap) repeated_[word].fetch_or(overlap, std::memory_order_relaxed);
            }
        });
    }
    bool maybe_repeated(Text id) const {
        auto h = std::hash<Text>{}(id);
        auto mask = bits(h);
        return (repeated_[(h >> 18) & word_mask_].load(std::memory_order_relaxed) & mask) == mask;
    }
};
} // namespace shopping
