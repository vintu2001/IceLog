#include "connection_pool.h"
#include <iostream>


ConnectionPool::ConnectionPool(const std::string& conn_string, size_t pool_size)
    : conn_string_(conn_string), pool_size_(pool_size)
{
    for (size_t i = 0; i < pool_size_; ++i) {
        PGconn* conn = create_connection();
        if (conn) {
            available_connections_.push(conn);
        }
    }
    std::cout << "[ConnectionPool] initialized with "
              << available_connections_.size() << "/" << pool_size_ << " connections\n";
}

ConnectionPool::~ConnectionPool() {
    std::lock_guard lock(mutex_);
    while (!available_connections_.empty()) {
        PGconn* conn = available_connections_.front();
        available_connections_.pop();
        PQfinish(conn);
    }
}

PGconn* ConnectionPool::create_connection() {
    PGconn* conn = PQconnectdb(conn_string_.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "[ConnectionPool] failed to connect: "
                  << PQerrorMessage(conn) << "\n";
        PQfinish(conn);
        return nullptr;
    }
    return conn;
}

bool ConnectionPool::validate_connection(PGconn* conn) {
    if (PQstatus(conn) != CONNECTION_OK) {
        // Try to reset the connection
        PQreset(conn);
        return PQstatus(conn) == CONNECTION_OK;
    }
    return true;
}

ConnectionPool::ConnectionHandle ConnectionPool::acquire() {
    std::unique_lock lock(mutex_);
    cv_.wait(lock, [this] { return !available_connections_.empty(); });

    PGconn* conn = available_connections_.front();
    available_connections_.pop();

    if (!validate_connection(conn)) {
        PQfinish(conn);
        conn = create_connection();
        if (!conn) {
            throw std::runtime_error("ConnectionPool: failed to create replacement connection");
        }
    }

    return ConnectionHandle(conn, this);
}

ConnectionPool::ConnectionHandle ConnectionPool::acquire_with_timeout(
    std::chrono::milliseconds timeout)
{
    std::unique_lock lock(mutex_);
    if (!cv_.wait_for(lock, timeout, [this] { return !available_connections_.empty(); })) {
        return ConnectionHandle();
    }

    PGconn* conn = available_connections_.front();
    available_connections_.pop();

    if (!validate_connection(conn)) {
        PQfinish(conn);
        conn = create_connection();
    }

    return ConnectionHandle(conn, this);
}

void ConnectionPool::release(PGconn* conn) {
    if (!conn) return;
    std::lock_guard lock(mutex_);
    available_connections_.push(conn);
    cv_.notify_one();
}

size_t ConnectionPool::available() const {
    std::lock_guard lock(mutex_);
    return available_connections_.size();
}


ConnectionPool::ConnectionHandle::ConnectionHandle(PGconn* conn, ConnectionPool* pool)
    : conn_(conn), pool_(pool) {}

ConnectionPool::ConnectionHandle::~ConnectionHandle() {
    if (conn_ && pool_) {
        pool_->release(conn_);
    }
}

ConnectionPool::ConnectionHandle::ConnectionHandle(ConnectionHandle&& other) noexcept
    : conn_(other.conn_), pool_(other.pool_)
{
    other.conn_ = nullptr;
    other.pool_ = nullptr;
}

ConnectionPool::ConnectionHandle& ConnectionPool::ConnectionHandle::operator=(
    ConnectionHandle&& other) noexcept
{
    if (this != &other) {
        if (conn_ && pool_) {
            pool_->release(conn_);
        }
        conn_ = other.conn_;
        pool_ = other.pool_;
        other.conn_ = nullptr;
        other.pool_ = nullptr;
    }
    return *this;
}
