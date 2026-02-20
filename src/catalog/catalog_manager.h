#pragma once

#include "db/pg_client.h"
#include "db/connection_pool.h"
#include "lock/lock_manager.h"
#include "transaction/mvcc_manager.h"

#include <string>
#include <vector>
#include <optional>
#include <cstdint>

class CatalogManager {
public:
    CatalogManager(ConnectionPool& pool, PgClient& pg,
                   LockManager& locks, MVCCManager& mvcc);


    struct CreateTableResult {
        bool success;
        std::string error_msg;
    };

    CreateTableResult create_table(const std::string& table_name,
                                   const std::string& schema_json,
                                   const std::string& partition_spec,
                                   const std::string& properties_json);

    std::optional<TableRow> get_table(const std::string& table_name);

    struct AlterResult {
        bool success;
        std::string error_msg;
    };

    AlterResult alter_table_schema(const std::string& table_name,
                                   const std::string& new_schema_json,
                                   const std::string& change_summary);

    AlterResult rename_table(const std::string& old_name,
                             const std::string& new_name);

    struct DropResult {
        bool success;
        std::string error_msg;
    };

    DropResult drop_table(const std::string& table_name, bool purge);

    std::vector<TableRow> list_tables(const std::string& ns,
                                      int32_t page_size,
                                      const std::string& page_token);

private:
    ConnectionPool& pool_;
    PgClient&       pg_;
    LockManager&    locks_;
    MVCCManager&    mvcc_;
};
