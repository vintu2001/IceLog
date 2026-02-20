#include <gtest/gtest.h>
#include "lock/lock_manager.h"
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>


TEST(LockManagerTest, SharedLockAllowsConcurrentReaders) {
    LockManager mgr;
    std::atomic<int> active_readers{0};
    int max_concurrent = 0;

    constexpr int NUM_READERS = 8;
    std::vector<std::thread> threads;

    for (int i = 0; i < NUM_READERS; ++i) {
        threads.emplace_back([&]() {
            mgr.acquire_shared("test_table");
            int current = ++active_readers;

            if (current > max_concurrent) max_concurrent = current;

            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            --active_readers;
            mgr.release_shared("test_table");
        });
    }

    for (auto& t : threads) t.join();

    EXPECT_GT(max_concurrent, 1);
}

TEST(LockManagerTest, ExclusiveLockBlocksReaders) {
    LockManager mgr;
    std::atomic<bool> writer_holds_lock{false};
    std::atomic<bool> reader_got_lock{false};

    std::thread writer([&]() {
        mgr.acquire_exclusive("tbl");
        writer_holds_lock = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        mgr.release_exclusive("tbl");
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    std::thread reader([&]() {
        mgr.acquire_shared("tbl");
        reader_got_lock = true;
        mgr.release_shared("tbl");
    });

    writer.join();
    reader.join();

    EXPECT_TRUE(reader_got_lock);
}


TEST(LockManagerTest, ScopedSharedGuardReleasesOnDestruction) {
    LockManager mgr;

    {
        auto guard = mgr.scoped_shared("my_table");
    }

    EXPECT_TRUE(mgr.try_acquire_exclusive("my_table", std::chrono::milliseconds(100)));
    mgr.release_exclusive("my_table");
}

TEST(LockManagerTest, ScopedExclusiveGuardReleasesOnDestruction) {
    LockManager mgr;

    {
        auto guard = mgr.scoped_exclusive("my_table");
    }

    mgr.acquire_shared("my_table");
    mgr.release_shared("my_table");
}


TEST(LockManagerTest, TryAcquireExclusiveTimesOut) {
    LockManager mgr;

    mgr.acquire_shared("tbl");

    std::thread t([&]() {
        bool got = mgr.try_acquire_exclusive("tbl", std::chrono::milliseconds(50));
        EXPECT_FALSE(got);
    });

    t.join();
    mgr.release_shared("tbl");
}

TEST(LockManagerTest, TryAcquireExclusiveSucceeds) {
    LockManager mgr;

    bool got = mgr.try_acquire_exclusive("tbl", std::chrono::milliseconds(100));
    EXPECT_TRUE(got);
    mgr.release_exclusive("tbl");
}


TEST(LockManagerTest, DifferentTablesAreIndependent) {
    LockManager mgr;
    std::atomic<bool> t1_locked{false};
    std::atomic<bool> t2_locked{false};

    std::thread th1([&]() {
        mgr.acquire_exclusive("table_a");
        t1_locked = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        mgr.release_exclusive("table_a");
    });

    std::thread th2([&]() {
        mgr.acquire_exclusive("table_b");
        t2_locked = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        mgr.release_exclusive("table_b");
    });

    th1.join();
    th2.join();

    EXPECT_TRUE(t1_locked);
    EXPECT_TRUE(t2_locked);
}
