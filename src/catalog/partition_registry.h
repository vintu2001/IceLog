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

/**
 * Partition Registry — manages the lifecycle of data partitions.
 *
 * Responsible for:
 *   - Querying partitions visible at a given snapshot (MVCC-aware)
 *   - Committing new partitions as part of a snapshot
 *   - Marking partitions as deleted (for overwrite operations)
 *   - Caching hot partition lists for sub-10ms retrieval
 *
 * Cache keys are "table_name:snapshot_id" to ensure each snapshot's
 * partition list is independently cacheable.
 */
class PartitionRegistry {
public:
    PartitionRegistry(ConnectionPool& pool, PgClient& pg,
                      LockManager& locks, MVCCManager& mvcc,
                      size_t cache_capacity);

    /**
     * Get all live partitions for a table at a given snapshot.
     * Reads from LRU cache first; falls back to PostgreSQL on miss.
     *
     * @param snapshot_id  0 means "latest committed snapshot"
     */
    std::optional<std::vector<PartitionRow>> get_partitions(
        const std::string& table_name,
        uint64_t snapshot_id);

    /**
     * Paginated variant for tables with very large partition counts.
     * Uses keyset pagination (partition_id > last_seen) for O(1) paging.
     */
    std::optional<std::vector<PartitionRow>> get_partitions_paged(
        const std::string& table_name,
        uint64_t snapshot_id,
        int32_t page_size,
        int64_t last_partition_id);

    /**
     * Aggregate stats for all live partitions of a table.
     */
    struct PartitionStats {
        int64_t total_partitions = 0;
        int64_t total_rows       = 0;
        int64_t total_bytes      = 0;
        int64_t avg_size_bytes   = 0;
    };

    PartitionStats get_stats(const std::string& table_name);

    /**
     * Commit a new snapshot with added/deleted partitions.
     *
     * 1. Validates optimistic concurrency (parent snapshot check).
     * 2. Inserts a new snapshot record.
     * 3. Inserts new partition records.
     * 4. Marks deleted partitions.
     * 5. Advances the table's current_snapshot_id.
     * 6. Invalidates stale cache entries.
     */
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

    /**
     * Invalidate all cached entries for a table (called after DDL changes).
     */
    void invalidate_table_cache(const std::string& table_name);

    // ── Cache stats ─────────────────────────────────

    double   cache_hit_rate() const { return cache_.hit_rate(); }
    size_t   cache_size()     const { return cache_.size(); }
    uint64_t cache_hits()     const { return cache_.hits(); }
    uint64_t cache_misses()   const { return cache_.misses(); }

private:
    ConnectionPool& pool_;
    PgClient&       pg_;
    LockManager&    locks_;
    MVCCManager&    mvcc_;

    // Cache: "table_name:snapshot_id" → partition list
    mutable LRUCache<std::string, std::vector<PartitionRow>> cache_;

    std::string make_cache_key(const std::string& table, uint64_t snap) const {
        return table + ":" + std::to_string(snap);
    }
};
