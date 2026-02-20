#pragma once

#include <list>
#include <unordered_map>
#include <optional>
#include <mutex>
#include <atomic>
#include <cstdint>
#include <functional>

template<typename K, typename V>
class LRUCache {
public:
    explicit LRUCache(size_t capacity) : capacity_(capacity) {}

    // Non-copyable, non-movable (contains mutex)
    LRUCache(const LRUCache&) = delete;
    LRUCache& operator=(const LRUCache&) = delete;

    std::optional<V> get(const K& key) {
        std::lock_guard lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            ++miss_count_;
            return std::nullopt;
        }
        ++hit_count_;
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        return it->second->second;
    }

    void put(const K& key, V value) {
        std::lock_guard lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second->second = std::move(value);
            lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
            return;
        }

        if (lru_list_.size() >= capacity_) {
            auto& last = lru_list_.back();
            map_.erase(last.first);
            lru_list_.pop_back();
            ++eviction_count_;
        }

        lru_list_.emplace_front(key, std::move(value));
        map_[key] = lru_list_.begin();
    }

    void invalidate(const K& key) {
        std::lock_guard lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            lru_list_.erase(it->second);
            map_.erase(it);
        }
    }

    template<typename Pred>
    size_t invalidate_if(Pred predicate) {
        std::lock_guard lock(mutex_);
        size_t removed = 0;
        for (auto it = map_.begin(); it != map_.end(); ) {
            if (predicate(it->first)) {
                lru_list_.erase(it->second);
                it = map_.erase(it);
                ++removed;
            } else {
                ++it;
            }
        }
        return removed;
    }

    void clear() {
        std::lock_guard lock(mutex_);
        lru_list_.clear();
        map_.clear();
    }

    size_t size() const {
        std::lock_guard lock(mutex_);
        return map_.size();
    }

    size_t capacity() const { return capacity_; }

    double hit_rate() const {
        uint64_t total = hit_count_.load(std::memory_order_relaxed)
                       + miss_count_.load(std::memory_order_relaxed);
        return total > 0
            ? static_cast<double>(hit_count_.load(std::memory_order_relaxed)) / total
            : 0.0;
    }

    uint64_t hits()      const { return hit_count_.load(std::memory_order_relaxed); }
    uint64_t misses()    const { return miss_count_.load(std::memory_order_relaxed); }
    uint64_t evictions() const { return eviction_count_.load(std::memory_order_relaxed); }

private:
    size_t capacity_;
    std::list<std::pair<K, V>> lru_list_;
    std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> map_;
    mutable std::mutex mutex_;
    std::atomic<uint64_t> hit_count_{0};
    std::atomic<uint64_t> miss_count_{0};
    std::atomic<uint64_t> eviction_count_{0};
};
