#include <gtest/gtest.h>
#include "cache/lru_cache.h"
#include <string>
#include <thread>
#include <vector>


TEST(LRUCacheTest, PutAndGet) {
    LRUCache<std::string, int> cache(3);

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);

    EXPECT_EQ(cache.get("a").value(), 1);
    EXPECT_EQ(cache.get("b").value(), 2);
    EXPECT_EQ(cache.get("c").value(), 3);
}

TEST(LRUCacheTest, MissReturnsNullopt) {
    LRUCache<std::string, int> cache(5);
    EXPECT_FALSE(cache.get("nonexistent").has_value());
}

TEST(LRUCacheTest, EvictsLeastRecentlyUsed) {
    LRUCache<std::string, int> cache(2);

    cache.put("a", 1);
    cache.put("b", 2);

    cache.put("c", 3);

    EXPECT_FALSE(cache.get("a").has_value());
    EXPECT_EQ(cache.get("b").value(), 2);
    EXPECT_EQ(cache.get("c").value(), 3);
}

TEST(LRUCacheTest, AccessPromotesToFront) {
    LRUCache<std::string, int> cache(2);

    cache.put("a", 1);
    cache.put("b", 2);

    cache.get("a");

    cache.put("c", 3);

    EXPECT_EQ(cache.get("a").value(), 1);
    EXPECT_FALSE(cache.get("b").has_value());
    EXPECT_EQ(cache.get("c").value(), 3);
}

TEST(LRUCacheTest, UpdateExistingKey) {
    LRUCache<std::string, int> cache(5);

    cache.put("x", 10);
    cache.put("x", 20);

    EXPECT_EQ(cache.get("x").value(), 20);
    EXPECT_EQ(cache.size(), 1u);
}


TEST(LRUCacheTest, InvalidateSingleKey) {
    LRUCache<std::string, int> cache(5);
    cache.put("a", 1);
    cache.put("b", 2);

    cache.invalidate("a");

    EXPECT_FALSE(cache.get("a").has_value());
    EXPECT_EQ(cache.get("b").value(), 2);
    EXPECT_EQ(cache.size(), 1u);
}

TEST(LRUCacheTest, InvalidateByPredicate) {
    LRUCache<std::string, int> cache(10);
    cache.put("events:1", 100);
    cache.put("events:2", 200);
    cache.put("users:1", 300);

    size_t removed = cache.invalidate_if([](const std::string& key) {
        return key.starts_with("events:");
    });

    EXPECT_EQ(removed, 2u);
    EXPECT_FALSE(cache.get("events:1").has_value());
    EXPECT_FALSE(cache.get("events:2").has_value());
    EXPECT_EQ(cache.get("users:1").value(), 300);
}

TEST(LRUCacheTest, ClearRemovesAll) {
    LRUCache<std::string, int> cache(5);
    cache.put("a", 1);
    cache.put("b", 2);

    cache.clear();

    EXPECT_EQ(cache.size(), 0u);
    EXPECT_FALSE(cache.get("a").has_value());
}


TEST(LRUCacheTest, HitRateTracking) {
    LRUCache<std::string, int> cache(5);
    cache.put("a", 1);

    cache.get("a");
    cache.get("b");
    cache.get("a");

    EXPECT_EQ(cache.hits(), 2u);
    EXPECT_EQ(cache.misses(), 1u);
    EXPECT_NEAR(cache.hit_rate(), 2.0 / 3.0, 0.01);
}


TEST(LRUCacheTest, ConcurrentReadWrite) {
    LRUCache<int, int> cache(1000);

    constexpr int NUM_THREADS = 8;
    constexpr int OPS_PER_THREAD = 5000;

    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key = t * OPS_PER_THREAD + i;
                cache.put(key, key * 2);
                cache.get(key);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_GT(cache.size(), 0u);
    EXPECT_LE(cache.size(), 1000u);
}
