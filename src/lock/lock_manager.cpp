#include "lock_manager.h"
#include <stdexcept>

std::shared_mutex& LockManager::get_or_create(const std::string& table_name) {
    std::lock_guard reg_lock(registry_mutex_);
    auto it = table_locks_.find(table_name);
    if (it == table_locks_.end()) {
        auto [inserted, ok] = table_locks_.emplace(
            table_name, std::make_unique<std::shared_mutex>());
        return *inserted->second;
    }
    return *it->second;
}

void LockManager::acquire_shared(const std::string& table_name) {
    get_or_create(table_name).lock_shared();
}

void LockManager::release_shared(const std::string& table_name) {
    std::lock_guard reg_lock(registry_mutex_);
    auto it = table_locks_.find(table_name);
    if (it == table_locks_.end()) {
        throw std::runtime_error("release_shared: no lock for table " + table_name);
    }
    it->second->unlock_shared();
}

void LockManager::acquire_exclusive(const std::string& table_name) {
    get_or_create(table_name).lock();
}

void LockManager::release_exclusive(const std::string& table_name) {
    std::lock_guard reg_lock(registry_mutex_);
    auto it = table_locks_.find(table_name);
    if (it == table_locks_.end()) {
        throw std::runtime_error("release_exclusive: no lock for table " + table_name);
    }
    it->second->unlock();
}

bool LockManager::try_acquire_exclusive(const std::string& table_name,
                                         std::chrono::milliseconds timeout) {
    auto& mtx = get_or_create(table_name);
    auto deadline = std::chrono::steady_clock::now() + timeout;

    // Spin with back-off since std::shared_mutex lacks try_lock_for
    while (std::chrono::steady_clock::now() < deadline) {
        if (mtx.try_lock()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

LockManager::SharedGuard LockManager::scoped_shared(const std::string& table_name) {
    acquire_shared(table_name);
    return SharedGuard{*this, table_name};
}

LockManager::ExclusiveGuard LockManager::scoped_exclusive(const std::string& table_name) {
    acquire_exclusive(table_name);
    return ExclusiveGuard{*this, table_name};
}
