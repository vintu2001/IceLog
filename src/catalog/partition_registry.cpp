#include "partition_registry.h"
#include <iostream>

PartitionRegistry::PartitionRegistry(ConnectionPool& pool, PgClient& pg,
                                     LockManager& locks, MVCCManager& mvcc,
                                     size_t cache_capacity)
    : pool_(pool), pg_(pg), locks_(locks), mvcc_(mvcc),
      cache_(cache_capacity) {}

// ── Get Partitions (with LRU cache) ─────────────────────

std::optional<std::vector<PartitionRow>> PartitionRegistry::get_partitions(
    const std::string& table_name,
    uint64_t snapshot_id)
{
    auto guard = locks_.scoped_shared(table_name);

    // Resolve "latest" if snapshot_id == 0
    uint64_t read_snap = (snapshot_id == 0)
        ? mvcc_.get_latest_committed_snapshot()
        : snapshot_id;

    // L1: check the in-process cache (microsecond latency)
    std::string key = make_cache_key(table_name, read_snap);
    if (auto cached = cache_.get(key)) {
        return cached;
    }

    // L2: cache miss — query PostgreSQL (single-digit ms with partial index)
    auto conn = pool_.acquire();
    auto result = pg_.query_partitions(conn.get(), table_name, read_snap);
    if (!result.has_value()) {
        return std::nullopt;
    }

    // Populate cache for future requests
    cache_.put(key, *result);
    return result;
}

std::optional<std::vector<PartitionRow>> PartitionRegistry::get_partitions_paged(
    const std::string& table_name,
    uint64_t snapshot_id,
    int32_t page_size,
    int64_t last_partition_id)
{
    auto guard = locks_.scoped_shared(table_name);

    uint64_t read_snap = (snapshot_id == 0)
        ? mvcc_.get_latest_committed_snapshot()
        : snapshot_id;

    // Paginated queries bypass the cache (too many permutations to cache)
    auto conn = pool_.acquire();
    return pg_.query_partitions_paged(conn.get(), table_name, read_snap,
                                      page_size, last_partition_id);
}

// ── Partition Stats ─────────────────────────────────────

PartitionRegistry::PartitionStats PartitionRegistry::get_stats(
    const std::string& table_name)
{
    auto guard = locks_.scoped_shared(table_name);
    uint64_t snap = mvcc_.get_latest_committed_snapshot();

    auto conn = pool_.acquire();
    auto parts = pg_.query_partitions(conn.get(), table_name, snap);

    PartitionStats stats;
    if (!parts.has_value()) return stats;

    stats.total_partitions = static_cast<int64_t>(parts->size());
    for (const auto& p : *parts) {
        stats.total_rows  += p.row_count;
        stats.total_bytes += p.size_bytes;
    }
    if (stats.total_partitions > 0) {
        stats.avg_size_bytes = stats.total_bytes / stats.total_partitions;
    }
    return stats;
}

// ── Commit Snapshot ─────────────────────────────────────

PartitionRegistry::CommitResult PartitionRegistry::commit_snapshot(
    const std::string& table_name,
    uint64_t parent_snapshot_id,
    const std::string& operation,
    const std::vector<PartitionRow>& new_partitions,
    const std::vector<std::string>& deleted_partition_keys)
{
    // Exclusive lock — blocks all readers and other writers on this table
    auto guard = locks_.scoped_exclusive(table_name);

    auto conn = pool_.acquire();

    // 1. Optimistic concurrency check
    uint64_t current_snap = pg_.get_current_snapshot(conn.get(), table_name);
    if (!mvcc_.validate_parent_snapshot(table_name, parent_snapshot_id, current_snap)) {
        return {
            false, 0,
            "Conflict: snapshot " + std::to_string(parent_snapshot_id) +
            " is no longer current (current=" + std::to_string(current_snap) + ")"
        };
    }

    // 2. Begin atomic PostgreSQL transaction
    PQexec(conn.get(), "BEGIN");

    // 3. Insert new snapshot record
    uint64_t new_snap = pg_.insert_snapshot(
        conn.get(), table_name, parent_snapshot_id, operation,
        static_cast<int32_t>(new_partitions.size()),
        static_cast<int32_t>(deleted_partition_keys.size()));

    if (new_snap == 0) {
        PQexec(conn.get(), "ROLLBACK");
        return {false, 0, "Failed to insert snapshot record"};
    }

    // 4. Insert new partition records
    for (const auto& part : new_partitions) {
        if (!pg_.insert_partition(conn.get(), table_name, new_snap, part)) {
            PQexec(conn.get(), "ROLLBACK");
            return {false, 0, "Failed to insert partition: " + part.partition_key};
        }
    }

    // 5. Mark deleted partitions (for overwrite/delete operations)
    for (const auto& key : deleted_partition_keys) {
        if (!pg_.mark_partition_deleted(conn.get(), table_name, key, new_snap)) {
            PQexec(conn.get(), "ROLLBACK");
            return {false, 0, "Failed to mark partition deleted: " + key};
        }
    }

    // 6. Advance the table's current_snapshot_id
    if (!pg_.update_table_snapshot(conn.get(), table_name, new_snap)) {
        PQexec(conn.get(), "ROLLBACK");
        return {false, 0, "Failed to update table snapshot pointer"};
    }

    // 7. Commit the PostgreSQL transaction
    PQexec(conn.get(), "COMMIT");

    // 8. Invalidate stale cache entries for this table
    invalidate_table_cache(table_name);

    std::cout << "[PartitionRegistry] committed snapshot " << new_snap
              << " for " << table_name << " (+" << new_partitions.size()
              << " partitions, -" << deleted_partition_keys.size() << " deleted)\n";

    return {true, new_snap, ""};
}

// ── Cache Invalidation ──────────────────────────────────

void PartitionRegistry::invalidate_table_cache(const std::string& table_name) {
    std::string prefix = table_name + ":";
    size_t removed = cache_.invalidate_if([&prefix](const std::string& key) {
        return key.compare(0, prefix.size(), prefix) == 0;
    });
    if (removed > 0) {
        std::cout << "[PartitionRegistry] invalidated " << removed
                  << " cache entries for " << table_name << "\n";
    }
}
