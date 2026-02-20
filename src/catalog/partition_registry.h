#pragma once

#include "db/pg_client.h"
#include "db/connection_pool.h"
#include "lock/lock_manager.h"
#include "cache/lru_cache.h"
#include "transaction/mvcc_manager.h"

#include <string>
#include <vector>
#include <optional>
#include <cstdint>

class PartitionRegistry {
public:
    PartitionRegistry(ConnectionPool& pool, PgClient& pg,
                      LockManager& locks, MVCCManager& mvcc,
                      size_t cache_capacity);

    std::optional<std::vector<PartitionRow>> get_partitions(
        const std::string& table_name,
        uint64_t snapshot_id);

    std::optional<std::vector<PartitionRow>> get_partitions_paged(
        const std::string& table_name,
        uint64_t snapshot_id,
        int32_t page_size,
        int64_t last_partition_id);

    struct PartitionStats {
        int64_t total_partitions = 0;
        int64_t total_rows       = 0;
        int64_t total_bytes      = 0;
        int64_t avg_size_bytes   = 0;
    };

    PartitionStats get_stats(const std::string& table_name);

    struct CommitResult {
        bool     success;
        uint64_t snapshot_id;
        std::string error_msg;
    };

    CommitResult commit_snapshot(
        const std::string& table_name,
        uint64_t parent_snapshot_id,
        const std::string& operation,
        const std::vector<PartitionRow>& new_partitions,
        const std::vector<std::string>& deleted_partition_keys);

    void invalidate_table_cache(const std::string& table_name);


    double   cache_hit_rate() const { return cache_.hit_rate(); }
    size_t   cache_size()     const { return cache_.size(); }
    uint64_t cache_hits()     const { return cache_.hits(); }
    uint64_t cache_misses()   const { return cache_.misses(); }

private:
    ConnectionPool& pool_;
    PgClient&       pg_;
    LockManager&    locks_;
    MVCCManager&    mvcc_;

    mutable LRUCache<std::string, std::vector<PartitionRow>> cache_;

    std::string make_cache_key(const std::string& table, uint64_t snap) const {
        return table + ":" + std::to_string(snap);
    }
};
