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

struct TransactionState {
    uint64_t txn_id;
    uint64_t read_snapshot_id;
    bool     is_committed = false;
    std::chrono::steady_clock::time_point started_at;
};

class MVCCManager {
public:
    explicit MVCCManager(std::chrono::seconds txn_timeout = std::chrono::seconds(300));


    std::pair<uint64_t, uint64_t> begin_transaction();

    bool commit_transaction(uint64_t txn_id);

    void abort_transaction(uint64_t txn_id);


    bool is_visible(uint64_t write_snapshot_id, uint64_t read_snapshot_id) const;

    uint64_t get_latest_committed_snapshot() const;


    bool validate_parent_snapshot(const std::string& table_name,
                                  uint64_t parent_snapshot_id,
                                  uint64_t current_snapshot_id) const;


    size_t cleanup_expired_transactions();

    size_t active_transaction_count() const;

    bool is_snapshot_pinned(uint64_t snapshot_id) const;

private:
    std::atomic<uint64_t> next_txn_id_{1};
    std::atomic<uint64_t> latest_committed_snapshot_{0};

    mutable std::shared_mutex mutex_;
    std::unordered_map<uint64_t, TransactionState> active_transactions_;
    std::unordered_set<uint64_t> pinned_snapshots_;

    std::chrono::seconds txn_timeout_;
};
