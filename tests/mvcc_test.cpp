#include <gtest/gtest.h>
#include "transaction/mvcc_manager.h"
#include <thread>
#include <vector>


TEST(MVCCTest, BeginReturnsIncrementingTxnIds) {
    MVCCManager mvcc;

    auto [txn1, snap1] = mvcc.begin_transaction();
    auto [txn2, snap2] = mvcc.begin_transaction();

    EXPECT_LT(txn1, txn2);
    EXPECT_EQ(mvcc.active_transaction_count(), 2u);
}

TEST(MVCCTest, CommitAdvancesSnapshot) {
    MVCCManager mvcc;

    uint64_t snap_before = mvcc.get_latest_committed_snapshot();
    auto [txn_id, read_snap] = mvcc.begin_transaction();
    EXPECT_TRUE(mvcc.commit_transaction(txn_id));

    uint64_t snap_after = mvcc.get_latest_committed_snapshot();
    EXPECT_GT(snap_after, snap_before);
}

TEST(MVCCTest, AbortDoesNotAdvanceSnapshot) {
    MVCCManager mvcc;

    uint64_t snap_before = mvcc.get_latest_committed_snapshot();
    auto [txn_id, read_snap] = mvcc.begin_transaction();
    mvcc.abort_transaction(txn_id);

    EXPECT_EQ(mvcc.get_latest_committed_snapshot(), snap_before);
    EXPECT_EQ(mvcc.active_transaction_count(), 0u);
}

TEST(MVCCTest, CommitUnknownTxnReturnsFalse) {
    MVCCManager mvcc;
    EXPECT_FALSE(mvcc.commit_transaction(99999));
}

TEST(MVCCTest, DoubleCommitFails) {
    MVCCManager mvcc;
    auto [txn_id, snap] = mvcc.begin_transaction();
    EXPECT_TRUE(mvcc.commit_transaction(txn_id));
    EXPECT_FALSE(mvcc.commit_transaction(txn_id));
}

TEST(MVCCTest, VisibilityRule) {
    MVCCManager mvcc;

    EXPECT_TRUE(mvcc.is_visible(5, 10));
    EXPECT_TRUE(mvcc.is_visible(10, 10));
    EXPECT_FALSE(mvcc.is_visible(15, 10));
}


TEST(MVCCTest, ParentSnapshotValidation) {
    MVCCManager mvcc;

    EXPECT_TRUE(mvcc.validate_parent_snapshot("t", 42, 42));
    EXPECT_FALSE(mvcc.validate_parent_snapshot("t", 42, 43));
}


TEST(MVCCTest, ActiveTransactionPinsSnapshot) {
    MVCCManager mvcc;

    auto [txn1, _] = mvcc.begin_transaction();
    mvcc.commit_transaction(txn1);

    uint64_t snap = mvcc.get_latest_committed_snapshot();

    auto [txn2, read_snap] = mvcc.begin_transaction();
    EXPECT_TRUE(mvcc.is_snapshot_pinned(read_snap));

    mvcc.abort_transaction(txn2);
    EXPECT_FALSE(mvcc.is_snapshot_pinned(read_snap));
}


TEST(MVCCTest, CleanupExpiredTransactions) {
    // Use a very short timeout for testing
    MVCCManager mvcc(std::chrono::seconds(0));

    auto [txn_id, snap] = mvcc.begin_transaction();
    EXPECT_EQ(mvcc.active_transaction_count(), 1u);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    size_t cleaned = mvcc.cleanup_expired_transactions();
    EXPECT_EQ(cleaned, 1u);
    EXPECT_EQ(mvcc.active_transaction_count(), 0u);
}

TEST(MVCCTest, ExpiredTransactionCannotCommit) {
    MVCCManager mvcc(std::chrono::seconds(0));

    auto [txn_id, snap] = mvcc.begin_transaction();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    EXPECT_FALSE(mvcc.commit_transaction(txn_id));
}


TEST(MVCCTest, ConcurrentBeginCommit) {
    MVCCManager mvcc;
    constexpr int N = 100;

    std::vector<std::thread> threads;
    for (int i = 0; i < N; ++i) {
        threads.emplace_back([&mvcc]() {
            auto [txn_id, snap] = mvcc.begin_transaction();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            mvcc.commit_transaction(txn_id);
        });
    }

    for (auto& t : threads) t.join();

    EXPECT_EQ(mvcc.active_transaction_count(), 0u);
    EXPECT_GE(mvcc.get_latest_committed_snapshot(), static_cast<uint64_t>(N));
}
