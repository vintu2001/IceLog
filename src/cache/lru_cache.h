#pragma once

#include <list>
#include <unordered_map>
#include <optional>
#include <mutex>
#include <atomic>
#include <cstdint>
#include <functional>

/**
 * Thread-safe LRU cache with O(1) get/put/invalidate.
 *
 * Used to cache partition metadata keyed by "table_name:snapshot_id".
 * Eviction is least-recently-used when capacity is exceeded.
 *
 * Template parameters:
 *   K — key type   (e.g. std::string)
 *   V — value type (e.g. TableMetadataResponse)
 */
template<typename K, typename V>
class LRUCache {
public:
    explicit LRUCache(size_t capacity) : capacity_(capacity) {}

    // Non-copyable, non-movable (contains mutex)
    LRUCache(const LRUCache&) = delete;
    LRUCache& operator=(const LRUCache&) = delete;

    /**
     * Retrieve a value by key. Returns std::nullopt on cache miss.
     * Promotes the entry to most-recently-used on hit.
     */
    std::optional<V> get(const K& key) {
        std::lock_guard lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            ++miss_count_;
            return std::nullopt;
        }
        ++hit_count_;
        // Splice to front — O(1) promotion
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        return it->second->second;
    }

    /**
     * Insert or update a key-value pair.
     * If the key already exists, the value is updated and promoted.
     * If the cache is full, the least-recently-used entry is evicted.
     */
    void put(const K& key, V value) {
        std::lock_guard lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second->second = std::move(value);
            lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
            return;
        }

        // Evict LRU entry if at capacity
        if (lru_list_.size() >= capacity_) {
            auto& last = lru_list_.back();
            map_.erase(last.first);
            lru_list_.pop_back();
            ++eviction_count_;
        }

        lru_list_.emplace_front(key, std::move(value));
        map_[key] = lru_list_.begin();
    }

    /**
     * Remove a specific key from the cache.
     */
    void invalidate(const K& key) {
        std::lock_guard lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            lru_list_.erase(it->second);
            map_.erase(it);
        }
    }

    /**
     * Remove all entries matching a predicate on the key.
     * Useful for table-level invalidation (e.g. invalidate all snapshots of "events").
     */
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

    /**
     * Clear the entire cache.
     */
    void clear() {
        std::lock_guard lock(mutex_);
        lru_list_.clear();
        map_.clear();
    }

    // ── Observability ────────────────────────────────

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

    // Doubly-linked list ordered by recency (front = most recent)
    std::list<std::pair<K, V>> lru_list_;

    // Hash map for O(1) key → list iterator lookup
    std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> map_;

    mutable std::mutex mutex_;

    // Lock-free stats counters
    std::atomic<uint64_t> hit_count_{0};
    std::atomic<uint64_t> miss_count_{0};
    std::atomic<uint64_t> eviction_count_{0};
};
