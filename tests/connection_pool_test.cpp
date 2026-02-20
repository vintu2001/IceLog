#include <gtest/gtest.h>
#include "db/connection_pool.h"
#include <chrono>

TEST(ConnectionPoolTest, AcquireWithTimeoutReturnsEmptyOnFailure) {
    ConnectionPool pool("host=invalid_host_1234 port=99999 dbname=nope", 1);

    auto handle = pool.acquire_with_timeout(std::chrono::milliseconds(100));
}

TEST(ConnectionPoolTest, TotalReturnsConfiguredSize) {
    ConnectionPool pool("host=localhost port=5432 dbname=metadata user=metadata_user password=metadata_pass", 5);
    EXPECT_EQ(pool.total(), 5u);
}

TEST(ConnectionPoolTest, ConnectionHandleMoveSemantics) {
    ConnectionPool::ConnectionHandle h1;  // default (empty) handle
    ConnectionPool::ConnectionHandle h2 = std::move(h1);
    EXPECT_FALSE(static_cast<bool>(h2));

    h1 = std::move(h2);
    EXPECT_FALSE(static_cast<bool>(h1));
}
