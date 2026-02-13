#include "catalog_manager.h"
#include <iostream>

CatalogManager::CatalogManager(ConnectionPool& pool, PgClient& pg,
                               LockManager& locks, MVCCManager& mvcc)
    : pool_(pool), pg_(pg), locks_(locks), mvcc_(mvcc) {}

// ── CreateTable ─────────────────────────────────────────

CatalogManager::CreateTableResult CatalogManager::create_table(
    const std::string& table_name,
    const std::string& schema_json,
    const std::string& partition_spec,
    const std::string& properties_json)
{
    // DDL: acquire exclusive lock so no concurrent reads see a half-created table
    auto guard = locks_.scoped_exclusive(table_name);

    auto conn = pool_.acquire();
    bool ok = pg_.create_table(conn.get(), table_name, schema_json,
                               partition_spec, properties_json);
    if (!ok) {
        return {false, "Failed to create table (may already exist): " + table_name};
    }

    std::cout << "[CatalogManager] created table: " << table_name << "\n";
    return {true, ""};
}

// ── GetTable ────────────────────────────────────────────

std::optional<TableRow> CatalogManager::get_table(const std::string& table_name) {
    // Read path: shared lock allows concurrent readers
    auto guard = locks_.scoped_shared(table_name);
    auto conn = pool_.acquire();
    return pg_.get_table(conn.get(), table_name);
}

// ── AlterTable (schema evolution) ───────────────────────

CatalogManager::AlterResult CatalogManager::alter_table_schema(
    const std::string& table_name,
    const std::string& new_schema_json,
    const std::string& change_summary)
{
    auto guard = locks_.scoped_exclusive(table_name);

    auto conn = pool_.acquire();

    // Fetch current version to increment
    auto existing = pg_.get_table(conn.get(), table_name);
    if (!existing.has_value()) {
        return {false, "Table not found: " + table_name};
    }

    int32_t new_version = existing->schema_version + 1;

    PQexec(conn.get(), "BEGIN");

    bool ok = pg_.update_table_schema(conn.get(), table_name,
                                       new_schema_json, new_version,
                                       change_summary);
    if (!ok) {
        PQexec(conn.get(), "ROLLBACK");
        return {false, "Failed to update schema for: " + table_name};
    }

    PQexec(conn.get(), "COMMIT");

    std::cout << "[CatalogManager] altered schema for " << table_name
              << " → v" << new_version << "\n";
    return {true, ""};
}

// ── RenameTable ─────────────────────────────────────────

CatalogManager::AlterResult CatalogManager::rename_table(
    const std::string& old_name,
    const std::string& new_name)
{
    // Lock both old and new names to prevent races
    auto guard_old = locks_.scoped_exclusive(old_name);
    auto guard_new = locks_.scoped_exclusive(new_name);

    auto conn = pool_.acquire();
    bool ok = pg_.rename_table(conn.get(), old_name, new_name);
    if (!ok) {
        return {false, "Failed to rename " + old_name + " → " + new_name};
    }

    std::cout << "[CatalogManager] renamed " << old_name << " → " << new_name << "\n";
    return {true, ""};
}

// ── DropTable ───────────────────────────────────────────

CatalogManager::DropResult CatalogManager::drop_table(const std::string& table_name,
                                                       bool purge)
{
    auto guard = locks_.scoped_exclusive(table_name);

    auto conn = pool_.acquire();

    PQexec(conn.get(), "BEGIN");
    bool ok = pg_.drop_table(conn.get(), table_name, purge);
    if (!ok) {
        PQexec(conn.get(), "ROLLBACK");
        return {false, "Failed to drop table: " + table_name};
    }
    PQexec(conn.get(), "COMMIT");

    std::cout << "[CatalogManager] dropped table: " << table_name
              << (purge ? " (purged)" : " (soft-deleted)") << "\n";
    return {true, ""};
}

// ── ListTables ──────────────────────────────────────────

std::vector<TableRow> CatalogManager::list_tables(const std::string& ns,
                                                   int32_t page_size,
                                                   const std::string& page_token)
{
    auto conn = pool_.acquire();
    return pg_.list_tables(conn.get(), ns, page_size, page_token);
}
