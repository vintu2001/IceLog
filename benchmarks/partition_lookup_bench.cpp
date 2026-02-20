#include "cache/lru_cache.h"
#include <benchmark/benchmark.h>
#include <string>
#include <random>
#include <vector>

struct BenchPartition {
    std::string key;
    std::string path;
    int64_t row_count;
    int64_t size_bytes;
};

using PartitionList = std::vector<BenchPartition>;

static LRUCache<std::string, PartitionList>& get_warm_cache() {
    static LRUCache<std::string, PartitionList> cache(50000);
    static bool initialized = false;

    if (!initialized) {
        for (int table = 0; table < 1000; ++table) {
            for (int snap = 0; snap < 50; ++snap) {
                std::string key = "table_" + std::to_string(table) + ":" + std::to_string(snap);

                PartitionList parts;
                parts.reserve(100);
                for (int p = 0; p < 100; ++p) {
                    parts.push_back({
                        "month=2025-" + std::to_string(p % 12 + 1),
                        "s3://bucket/table_" + std::to_string(table) + "/part-" + std::to_string(p) + ".parquet",
                        100000 + p * 1000,
                        50 * 1024 * 1024LL
                    });
                }
                cache.put(key, std::move(parts));
            }
        }
        initialized = true;
    }
    return cache;
}


static void BM_CacheHit(benchmark::State& state) {
    auto& cache = get_warm_cache();
    std::string key = "table_500:25";

    for (auto _ : state) {
        auto result = cache.get(key);
        benchmark::DoNotOptimize(result);
    }
}
BENCHMARK(BM_CacheHit)->Threads(1)->Threads(4)->Threads(8);


static void BM_CacheMiss(benchmark::State& state) {
    auto& cache = get_warm_cache();
    std::string key = "nonexistent_table:99999";

    for (auto _ : state) {
        auto result = cache.get(key);
        benchmark::DoNotOptimize(result);
    }
}
BENCHMARK(BM_CacheMiss)->Threads(1)->Threads(4);


static void BM_CachePut(benchmark::State& state) {
    LRUCache<std::string, PartitionList> cache(10000);
    int i = 0;

    for (auto _ : state) {
        std::string key = "bench:" + std::to_string(i++);
        PartitionList parts = {{"key", "path", 100000, 50 * 1024 * 1024LL}};
        cache.put(key, std::move(parts));
    }
}
BENCHMARK(BM_CachePut)->Threads(1)->Threads(4);


static void BM_InvalidateTable(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        LRUCache<std::string, int> cache(1000);
        for (int i = 0; i < 1000; ++i) {
            cache.put("events:" + std::to_string(i), i);
        }
        state.ResumeTiming();

        cache.invalidate_if([](const std::string& key) {
            return key.starts_with("events:");
        });
    }
}
BENCHMARK(BM_InvalidateTable);


static void BM_MixedWorkload(benchmark::State& state) {
    LRUCache<int, int> cache(10000);
    std::mt19937 rng(42 + state.thread_index());
    std::uniform_int_distribution<int> dist(0, 20000);

    for (auto _ : state) {
        int key = dist(rng);
        if (key % 5 == 0) {
            cache.put(key, key * 2);
        } else {
            auto val = cache.get(key);
            benchmark::DoNotOptimize(val);
        }
    }
}
BENCHMARK(BM_MixedWorkload)->Threads(1)->Threads(4)->Threads(8)->Threads(16);

BENCHMARK_MAIN();
