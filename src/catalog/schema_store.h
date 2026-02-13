#pragma once

#include "db/pg_client.h"
#include "db/connection_pool.h"
#include "lock/lock_manager.h"

#include <string>
#include <optional>
#include <vector>
#include <cstdint>

/**
 * Schema Store — manages schema versioning and evolution history.
 *
 * Every schema change is recorded in the schema_history table,
 * enabling time-travel queries to use the correct schema for any snapshot.
 *
 * Schema evolution rules (Iceberg-compatible):
 *   - Adding columns is always safe.
 *   - Dropping columns is allowed (old data returns NULL for dropped cols).
 *   - Renaming columns is allowed (tracked by field ID).
 *   - Type widening is allowed (int → long, float → double).
 */
class SchemaStore {
public:
    SchemaStore(ConnectionPool& pool, PgClient& pg, LockManager& locks);

    struct SchemaVersion {
        int32_t     version;
        std::string schema_json;
        std::string changed_at;
        std::string change_summary;
    };

    /**
     * Get the current schema for a table.
     */
    std::optional<SchemaVersion> get_current_schema(const std::string& table_name);

    /**
     * Get a specific historical schema version.
     */
    std::optional<SchemaVersion> get_schema_at_version(const std::string& table_name,
                                                       int32_t version);

    /**
     * List all schema versions for a table (newest first).
     */
    std::vector<SchemaVersion> list_schema_history(const std::string& table_name);

    /**
     * Validate a proposed schema change against the current schema.
     * Returns an error message if the change is incompatible, or empty string if OK.
     */
    std::string validate_schema_change(const std::string& current_json,
                                       const std::string& proposed_json);

private:
    ConnectionPool& pool_;
    PgClient&       pg_;
    LockManager&    locks_;
};
