#pragma once

#include <libpq-fe.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <string>
#include <chrono>
#include <stdexcept>

/**
 * Fixed-size PostgreSQL connection pool using libpq.
 *
 * Connections are pre-established at construction time.
 * acquire() blocks if the pool is exhausted; connections are returned
 * automatically when the RAII ConnectionHandle goes out of scope.
 */
class ConnectionPool {
public:
    ConnectionPool(const std::string& conn_string, size_t pool_size);
    ~ConnectionPool();

    // Non-copyable
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    /**
     * RAII handle — returns the connection to the pool on destruction.
     */
    class ConnectionHandle {
    public:
        ConnectionHandle() : conn_(nullptr), pool_(nullptr) {}
        ConnectionHandle(PGconn* conn, ConnectionPool* pool);
        ~ConnectionHandle();

        // Move-only
        ConnectionHandle(ConnectionHandle&& other) noexcept;
        ConnectionHandle& operator=(ConnectionHandle&& other) noexcept;
        ConnectionHandle(const ConnectionHandle&) = delete;
        ConnectionHandle& operator=(const ConnectionHandle&) = delete;

        PGconn* get() const { return conn_; }
        PGconn* operator->() const { return conn_; }
        explicit operator bool() const { return conn_ != nullptr; }

    private:
        PGconn* conn_;
        ConnectionPool* pool_;
    };

    /**
     * Acquire a connection from the pool. Blocks if none available.
     */
    ConnectionHandle acquire();

    /**
     * Acquire with a timeout. Returns an empty handle on timeout.
     */
    ConnectionHandle acquire_with_timeout(std::chrono::milliseconds timeout);

    size_t available() const;
    size_t total() const { return pool_size_; }

private:
    void release(PGconn* conn);
    bool validate_connection(PGconn* conn);
    PGconn* create_connection();

    std::string conn_string_;
    size_t pool_size_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<PGconn*> available_connections_;
};
