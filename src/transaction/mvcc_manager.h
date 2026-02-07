#pragma once

#include <atomic>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <cstdint>
#include <optional>
#include <chrono>
#include <string>
#include <utility>

/**
 * In-memory state for a single active transaction.
 */
struct TransactionState {
    uint64_t txn_id;
    uint64_t read_snapshot_id;
    bool     is_committed = false;
    std::chrono::steady_clock::time_point started_at;
};

/**
 * MVCC (Multi-Version Concurrency Control) Manager.
 *
 * Provides Snapshot Isolation for metadata reads:
 *   - Readers see a consistent point-in-time snapshot.
 *   - Writers create new snapshots atomically.
 *   - Conflict detection uses optimistic concurrency (parent snapshot validation).
 *
 * Thread safety:
 *   - get_latest_committed_snapshot() is lock-free (atomic load).
 *   - begin/commit/abort acquire the write lock on the transaction map.
 *   - is_visible() is a pure comparison (no locking needed).
 */
class MVCCManager {
public:
    explicit MVCCManager(std::chrono::seconds txn_timeout = std::chrono::seconds(300));

    // ── Transaction lifecycle ───────────────────────

    /**
     * Begin a new transaction.
     * Returns {txn_id, read_snapshot_id} — the caller reads from this snapshot.
     */
    std::pair<uint64_t, uint64_t> begin_transaction();

    /**
     * Commit a transaction. Advances the global snapshot pointer.
     * Returns false if the transaction was already expired or not found.
     */
    bool commit_transaction(uint64_t txn_id);

    /**
     * Abort a transaction, releasing its pinned snapshot.
     */
    void abort_transaction(uint64_t txn_id);

    // ── Visibility ──────────────────────────────────

    /**
     * Determine whether a partition written at write_snapshot_id
     * is visible to a reader at read_snapshot_id.
     *
     * Visibility rule: W <= R (the write was committed before the read began).
     */
    bool is_visible(uint64_t write_snapshot_id, uint64_t read_snapshot_id) const;

    /**
     * Returns the latest committed snapshot (lock-free atomic read).
     */
    uint64_t get_latest_committed_snapshot() const;

    // ── Optimistic concurrency ──────────────────────

    /**
     * Validate that parent_snapshot_id is still the current snapshot for a table.
     * Used before CommitSnapshot to detect write-write conflicts.
     */
    bool validate_parent_snapshot(const std::string& table_name,
                                  uint64_t parent_snapshot_id,
                                  uint64_t current_snapshot_id) const;

    // ── Maintenance ─────────────────────────────────

    /**
     * Garbage-collect transactions that have exceeded their timeout.
     * Should be called periodically by a background thread.
     */
    size_t cleanup_expired_transactions();

    /**
     * Number of currently active (uncommitted, non-aborted) transactions.
     */
    size_t active_transaction_count() const;

    /**
     * Check whether a given snapshot is pinned by any active reader.
     * Pinned snapshots must not be garbage-collected.
     */
    bool is_snapshot_pinned(uint64_t snapshot_id) const;

private:
    std::atomic<uint64_t> next_txn_id_{1};
    std::atomic<uint64_t> latest_committed_snapshot_{0};

    mutable std::shared_mutex mutex_;
    std::unordered_map<uint64_t, TransactionState> active_transactions_;

    // Snapshot IDs currently being read — prevents GC of these snapshots
    std::unordered_set<uint64_t> pinned_snapshots_;

    std::chrono::seconds txn_timeout_;
};
