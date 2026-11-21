#pragma once
#include "parse.hpp"
#include <algorithm>
#include <atomic>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace shopping {

template<class Function>
void parallel(size_t workers, size_t count, Function fn) {
    if (!count) return;
    workers = std::min(workers, count);
    if (workers == 1) {
        for (size_t i = 0; i < count; ++i) fn(0, i);
        return;
    }
    std::atomic<size_t> next{0};
    std::exception_ptr failure;
    std::mutex error_lock;
    auto work = [&](size_t worker) {
        try {
            for (;;) {
                const auto i = next.fetch_add(1, std::memory_order_relaxed);
                if (i >= count) break;
                fn(worker, i);
            }
        } catch (...) {
            next.store(count, std::memory_order_relaxed);
            std::lock_guard<std::mutex> lock(error_lock);
            if (!failure) failure = std::current_exception();
        }
    };
    std::vector<std::thread> threads;
    for (size_t i = 1; i < workers; ++i) threads.emplace_back(work, i);
    work(0);
    for (auto& thread : threads) thread.join();
    if (failure) std::rethrow_exception(failure);
}

struct Files {
    std::vector<std::unique_ptr<MappedFile>> maps;
    std::vector<std::string_view> chunks;
    size_t bytes = 0;
    explicit Files(const std::filesystem::path& dir) {
        constexpr size_t chunk_size = 2 * 1024 * 1024;
        for (const auto& file : std::filesystem::directory_iterator(dir)) {
            if (!file.is_regular_file() || file.path().extension() != ".csv") continue;
            maps.push_back(std::make_unique<MappedFile>(file.path()));
            auto data = csv_data(maps.back()->view());
            bytes += data.size();
            for (size_t i = 0; i < data.size(); i += chunk_size) {
                auto chunk = csv_chunk(data, i, std::min(i + chunk_size, data.size()));
                if (!chunk.empty()) chunks.push_back(chunk);
            }
        }
        std::sort(chunks.begin(), chunks.end(), [](auto a, auto b) { return a.size() > b.size(); });
    }
};

} // namespace shopping
