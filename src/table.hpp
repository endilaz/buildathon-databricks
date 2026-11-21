// Linear-probing map from Key (never 0) to a small value, key and value
// in one slot so a hit touches one cache line.
#pragma once
#include "base.hpp"

namespace ss {

#pragma pack(push, 8)
template <class V>
struct KeySlot {
    Key k;
    V v;
};
#pragma pack(pop)

template <class V>
class KeyMap {
    std::vector<KeySlot<V>> slots_;
    size_t mask_ = 0, size_ = 0;

    static size_t capacity_for(size_t n) {
        size_t cap = 16;
        while (cap < n * 2) cap <<= 1;
        return cap;
    }
    void grow() {
        std::vector<KeySlot<V>> old(capacity_for(slots_.size()));
        old.swap(slots_);
        mask_ = slots_.size() - 1;
        size_ = 0;
        for (auto& s : old)
            if (s.k) insert(s.k, s.v);
    }

public:
    explicit KeyMap(size_t n = 8) { reset(n); }
    void reset(size_t n) {
        size_t cap = capacity_for(n);
        if (cap == slots_.size()) {
            std::memset(static_cast<void*>(slots_.data()), 0, cap * sizeof(KeySlot<V>));
        } else {
            std::vector<KeySlot<V>>(cap).swap(slots_);
        }
        mask_ = cap - 1;
        size_ = 0;
    }
    size_t size() const { return size_; }
    size_t slot(Key k) const { return size_t(key_hash(k)) & mask_; }
    void prefetch_key(Key k) const { prefetch(&slots_[slot(k)]); }
    void prefetch_hash(u64 h) const { prefetch(&slots_[size_t(h) & mask_]); }

    const V* find(Key k) const { return find_hashed(k, key_hash(k)); }
    // h must be key_hash(k)
    const V* find_hashed(Key k, u64 h) const {
        for (size_t i = size_t(h) & mask_;; i = (i + 1) & mask_) {
            const auto& s = slots_[i];
            if (s.k == k) return &s.v;
            if (!s.k) return nullptr;
        }
    }
    // Inserts make() if k is absent; returns a reference to k's value.
    template <class Make>
    V& get_or_insert(Key k, Make make) {
        if ((size_ + 1) * 4 > slots_.size() * 3) [[unlikely]] grow();
        for (size_t i = slot(k);; i = (i + 1) & mask_) {
            auto& s = slots_[i];
            if (s.k == k) return s.v;
            if (!s.k) {
                s.k = k;
                s.v = make();
                ++size_;
                return s.v;
            }
        }
    }
    void insert(Key k, V v) {
        get_or_insert(k, [&] { return v; }) = v;
    }
    template <class F>
    void for_each(F f) {
        for (auto& s : slots_)
            if (s.k) f(s.k, s.v);
    }
};

}  // namespace ss
