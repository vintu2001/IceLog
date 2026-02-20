#pragma once

#include "db/pg_client.h"
#include "db/connection_pool.h"
#include "lock/lock_manager.h"

#include <string>
#include <optional>
#include <vector>
#include <cstdint>

class SchemaStore {
public:
    SchemaStore(ConnectionPool& pool, PgClient& pg, LockManager& locks);

    struct SchemaVersion {
        int32_t     version;
        std::string schema_json;
        std::string changed_at;
        std::string change_summary;
    };

    std::optional<SchemaVersion> get_current_schema(const std::string& table_name);

    std::optional<SchemaVersion> get_schema_at_version(const std::string& table_name,
                                                       int32_t version);

    std::vector<SchemaVersion> list_schema_history(const std::string& table_name);

    std::string validate_schema_change(const std::string& current_json,
                                       const std::string& proposed_json);

private:
    ConnectionPool& pool_;
    PgClient&       pg_;
    LockManager&    locks_;
};
