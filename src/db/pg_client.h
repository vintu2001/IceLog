#pragma once

#include <libpq-fe.h>
#include <string>
#include <vector>
#include <optional>
#include <cstdint>

/**
 * Row-level structures returned from PostgreSQL queries.
 */
struct PartitionRow {
    int64_t  partition_id;
    int64_t  table_id;
    int64_t  snapshot_id;
    std::string partition_key;
    std::string data_file_path;
    std::string file_format;
    int64_t  row_count;
    int64_t  size_bytes;
    std::string column_stats_json;
};

struct TableRow {
    int64_t     table_id;
    std::string table_name;
    std::string schema_json;
    int32_t     schema_version;
    std::string partition_spec;
    int64_t     current_snapshot_id;
    std::string properties_json;
};

struct SnapshotRow {
    int64_t     snapshot_id;
    int64_t     table_id;
    int64_t     parent_snapshot_id;
    std::string operation;
    int32_t     added_files_count;
    int32_t     deleted_files_count;
    std::string committed_at;
};

/**
 * Thin wrapper around libpq for executing parameterized queries.
 *
 * Each method takes a raw PGconn* (borrowed from ConnectionPool).
 * The caller is responsible for connection lifetime management.
 */
class PgClient {
public:
    // ── Table operations ────────────────────────────

    bool create_table(PGconn* conn, const std::string& table_name,
                      const std::string& schema_json,
                      const std::string& partition_spec,
                      const std::string& properties_json);

    std::optional<TableRow> get_table(PGconn* conn, const std::string& table_name);

    bool update_table_schema(PGconn* conn, const std::string& table_name,
                             const std::string& new_schema_json,
                             int32_t new_version,
                             const std::string& change_summary);

    bool rename_table(PGconn* conn, const std::string& old_name,
                      const std::string& new_name);

    bool drop_table(PGconn* conn, const std::string& table_name, bool purge);

    std::vector<TableRow> list_tables(PGconn* conn, const std::string& ns,
                                      int32_t page_size, const std::string& page_token);

    // ── Partition operations ────────────────────────

    std::optional<std::vector<PartitionRow>> query_partitions(
        PGconn* conn, const std::string& table_name, uint64_t snapshot_id);

    std::optional<std::vector<PartitionRow>> query_partitions_paged(
        PGconn* conn, const std::string& table_name, uint64_t snapshot_id,
        int32_t page_size, int64_t last_partition_id);

    bool insert_partition(PGconn* conn, const std::string& table_name,
                          uint64_t snapshot_id, const PartitionRow& part);

    bool mark_partition_deleted(PGconn* conn, const std::string& table_name,
                                const std::string& partition_key,
                                uint64_t deleted_snapshot_id);

    // ── Snapshot operations ─────────────────────────

    uint64_t insert_snapshot(PGconn* conn, const std::string& table_name,
                             uint64_t parent_snapshot_id,
                             const std::string& operation,
                             int32_t added_count, int32_t deleted_count);

    std::optional<SnapshotRow> get_snapshot(PGconn* conn, const std::string& table_name,
                                            uint64_t snapshot_id);

    std::vector<SnapshotRow> list_snapshots(PGconn* conn, const std::string& table_name,
                                            int32_t limit);

    uint64_t get_current_snapshot(PGconn* conn, const std::string& table_name);

    bool update_table_snapshot(PGconn* conn, const std::string& table_name,
                               uint64_t snapshot_id);

    // ── Transaction operations ──────────────────────

    uint64_t insert_transaction(PGconn* conn, const std::string& client_id,
                                uint64_t read_snapshot_id,
                                const std::string& isolation);

    bool update_transaction_status(PGconn* conn, uint64_t txn_id,
                                   const std::string& status);

    // ── Utility ─────────────────────────────────────

    int64_t get_table_id(PGconn* conn, const std::string& table_name);

private:
    // Helper to safely extract a string field from PGresult
    static std::string field_str(PGresult* res, int row, int col);
    static int64_t     field_int64(PGresult* res, int row, int col);
    static int32_t     field_int32(PGresult* res, int row, int col);
};
