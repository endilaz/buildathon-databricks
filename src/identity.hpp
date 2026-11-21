#pragma once
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace shopping {

using Text = std::string_view;
using Index = uint32_t;
constexpr Index missing = std::numeric_limits<Index>::max();

inline Index as_index(size_t count) {
    if (count >= missing) throw std::length_error("ID dictionary exceeds 32-bit capacity");
    return static_cast<Index>(count);
}

// Pack short identifiers sharing a prefix into an exact machine-word key.
// Everything else, including trailing NULs, uses the full byte string.
class NameIndex {
    std::vector<uint64_t> keys_;
    std::vector<Index> values_;
    std::unordered_map<Text, Index> other_;
    size_t size_ = 0;
    char prefix_ = 0;

    uint64_t pack(Text name) const {
        if (name.size() < 2 || name.size() > 9 || name.front() != prefix_ || name.back() == 0) return 0;
        uint64_t v = 0;
        if (name.size() == 9) std::memcpy(&v, name.data()+1, 8);
        else std::memcpy(&v, name.data()+1, name.size()-1);
        return v;
    }
    size_t position(uint64_t key) const {
        uint64_t mixed = (key ^ (key >> 29)) * 0x9e3779b97f4a7c15ULL;
        size_t mask = keys_.size() - 1;
        size_t i = (mixed ^ (mixed >> 32)) & mask;
        while (keys_[i] && keys_[i] != key) i = (i+1) & mask;
        return i;
    }
    void grow() {
        auto old_keys = std::move(keys_);
        auto old_values = std::move(values_);
        size_t count = std::max<size_t>(16, old_keys.size()*2);
        keys_.assign(count, 0);
        values_.resize(count);
        for (size_t i = 0; i < old_keys.size(); ++i) {
            if (!old_keys[i]) continue;
            auto at = position(old_keys[i]);
            keys_[at] = old_keys[i];
            values_[at] = old_values[i];
        }
    }
public:
    Index find(Text name) const {
        uint64_t key = pack(name);
        if (key && !keys_.empty()) {
            auto i = position(key);
            return keys_[i] ? values_[i] : missing;
        }
        auto it = other_.find(name);
        return it == other_.end() ? missing : it->second;
    }
    Index& get(Text name, Index initial) {
        if (keys_.empty() && other_.empty()) prefix_ = name.front();
        uint64_t key = pack(name);
        if (!key) return other_.try_emplace(name, initial).first->second;
        if (size_*10 >= keys_.size()*7) grow();
        size_t i = position(key);
        if (!keys_[i]) {
            keys_[i] = key;
            values_[i] = initial;
            ++size_;
        }
        return values_[i];
    }
};

// Keys reference immutable memory-mapped CSVs, including names seen only in links.
class People {
    NameIndex ids_;
    std::vector<Index> parent_, size_, smallest_;
    std::vector<Text> labels_;

    Index insert(Text name) {
        Index next = as_index(labels_.size());
        Index value = ids_.get(name, next);
        if (value == next) {
            labels_.push_back(name);
            parent_.push_back(value);
            size_.push_back(1);
            smallest_.push_back(value);
        }
        return value;
    }
    Index root(Index i) {
        while (parent_[i] != i) {
            parent_[i] = parent_[parent_[i]];
            i = parent_[i];
        }
        return i;
    }
public:
    void link(Text a, Text b) {
        Index x = root(insert(a));
        Index y = root(insert(b));
        if (x == y) return;
        if (size_[x] < size_[y]) std::swap(x, y);
        parent_[y] = x;
        size_[x] += size_[y];
        if (labels_[smallest_[y]] < labels_[smallest_[x]]) smallest_[x] = smallest_[y];
    }

    std::vector<Text> finalize() {
        std::vector<Index> dense(labels_.size(), missing);
        std::vector<Text> names;
        for (size_t i = 0; i < labels_.size(); ++i) {
            auto component = root(static_cast<Index>(i));
            if (dense[component] == missing) {
                dense[component] = as_index(names.size());
                names.push_back(labels_[smallest_[component]]);
            }
            ids_.get(labels_[i], missing) = dense[component];
        }
        std::vector<Index>().swap(parent_);
        std::vector<Index>().swap(size_);
        std::vector<Index>().swap(smallest_);
        std::vector<Text>().swap(labels_);
        return names;
    }
    Index find(Text name) const {
        return ids_.find(name);
    }
};

class LocalPeople {
    struct Hit { Text key; Index value = missing; };
    std::vector<Hit> cache_ = std::vector<Hit>(32768);
    NameIndex singles_;
public:
    std::vector<Text> names;
    std::vector<Index> remap;
    Index get(Text name, const People& links, Index first_single) {
        auto& hit = cache_[std::hash<Text>{}(name) & (cache_.size() - 1)];
        if (hit.key == name) return hit.value;
        Index value = links.find(name);
        if (value == missing) {
            Index next = as_index(size_t(first_single) + names.size());
            value = singles_.get(name, next);
            if (value == next) names.push_back(name);
        }
        hit = {name, value};
        return value;
    }
    Index global(Index id, Index first_single) const {
        return id < first_single ? id : remap[id - first_single];
    }
};

} // namespace shopping
