#include "mvcc_manager.h"
#include <stdexcept>

MVCCManager::MVCCManager(std::chrono::seconds txn_timeout)
    : txn_timeout_(txn_timeout) {}


std::pair<uint64_t, uint64_t> MVCCManager::begin_transaction() {
    uint64_t txn_id = next_txn_id_.fetch_add(1, std::memory_order_seq_cst);
    uint64_t read_snapshot = latest_committed_snapshot_.load(std::memory_order_acquire);

    std::unique_lock lock(mutex_);
    active_transactions_[txn_id] = TransactionState{
        .txn_id          = txn_id,
        .read_snapshot_id = read_snapshot,
        .is_committed    = false,
        .started_at      = std::chrono::steady_clock::now()
    };
    pinned_snapshots_.insert(read_snapshot);

    return {txn_id, read_snapshot};
}

bool MVCCManager::commit_transaction(uint64_t txn_id) {
    std::unique_lock lock(mutex_);

    auto it = active_transactions_.find(txn_id);
    if (it == active_transactions_.end()) {
        return false;
    }

    auto elapsed = std::chrono::steady_clock::now() - it->second.started_at;
    if (elapsed > txn_timeout_) {
        pinned_snapshots_.erase(it->second.read_snapshot_id);
        active_transactions_.erase(it);
        return false;
    }

    uint64_t new_snapshot = latest_committed_snapshot_.load(std::memory_order_relaxed) + 1;
    uint64_t current = latest_committed_snapshot_.load(std::memory_order_relaxed);
    while (current < new_snapshot) {
        if (latest_committed_snapshot_.compare_exchange_weak(
                current, new_snapshot,
                std::memory_order_release,
                std::memory_order_relaxed)) {
            break;
        }
        new_snapshot = current + 1;
    }

    pinned_snapshots_.erase(it->second.read_snapshot_id);
    active_transactions_.erase(it);
    return true;
}

void MVCCManager::abort_transaction(uint64_t txn_id) {
    std::unique_lock lock(mutex_);
    auto it = active_transactions_.find(txn_id);
    if (it != active_transactions_.end()) {
        pinned_snapshots_.erase(it->second.read_snapshot_id);
        active_transactions_.erase(it);
    }
}

bool MVCCManager::is_visible(uint64_t write_snapshot_id,
                              uint64_t read_snapshot_id) const {
    return write_snapshot_id <= read_snapshot_id;
}

uint64_t MVCCManager::get_latest_committed_snapshot() const {
    return latest_committed_snapshot_.load(std::memory_order_acquire);
}

bool MVCCManager::validate_parent_snapshot(const std::string& /*table_name*/,
                                            uint64_t parent_snapshot_id,
                                            uint64_t current_snapshot_id) const {
    return parent_snapshot_id == current_snapshot_id;
}


size_t MVCCManager::cleanup_expired_transactions() {
    std::unique_lock lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    size_t cleaned = 0;

    for (auto it = active_transactions_.begin(); it != active_transactions_.end(); ) {
        if (now - it->second.started_at > txn_timeout_) {
            pinned_snapshots_.erase(it->second.read_snapshot_id);
            it = active_transactions_.erase(it);
            ++cleaned;
        } else {
            ++it;
        }
    }
    return cleaned;
}

size_t MVCCManager::active_transaction_count() const {
    std::shared_lock lock(mutex_);
    return active_transactions_.size();
}

bool MVCCManager::is_snapshot_pinned(uint64_t snapshot_id) const {
    std::shared_lock lock(mutex_);
    return pinned_snapshots_.count(snapshot_id) > 0;
}
