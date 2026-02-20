#pragma once

#include <shared_mutex>
#include <unordered_map>
#include <string>
#include <mutex>
#include <memory>
#include <chrono>
#include <thread>

class LockManager {
public:

    void acquire_shared(const std::string& table_name);
    void release_shared(const std::string& table_name);


    void acquire_exclusive(const std::string& table_name);
    void release_exclusive(const std::string& table_name);

    bool try_acquire_exclusive(const std::string& table_name,
                                std::chrono::milliseconds timeout);


    struct SharedGuard {
        LockManager& mgr;
        std::string  table;
        SharedGuard(LockManager& m, std::string t) : mgr(m), table(std::move(t)) {}
        ~SharedGuard() { mgr.release_shared(table); }

        SharedGuard(const SharedGuard&) = delete;
        SharedGuard& operator=(const SharedGuard&) = delete;
    };

    struct ExclusiveGuard {
        LockManager& mgr;
        std::string  table;
        ExclusiveGuard(LockManager& m, std::string t) : mgr(m), table(std::move(t)) {}
        ~ExclusiveGuard() { mgr.release_exclusive(table); }

        ExclusiveGuard(const ExclusiveGuard&) = delete;
        ExclusiveGuard& operator=(const ExclusiveGuard&) = delete;
    };

    [[nodiscard]] SharedGuard    scoped_shared(const std::string& table_name);
    [[nodiscard]] ExclusiveGuard scoped_exclusive(const std::string& table_name);

private:
    std::mutex registry_mutex_;
    std::unordered_map<std::string, std::unique_ptr<std::shared_mutex>> table_locks_;

    std::shared_mutex& get_or_create(const std::string& table_name);
};
