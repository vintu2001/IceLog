#include "pg_client.h"
#include <cstring>
#include <stdexcept>
#include <iostream>


std::string PgClient::field_str(PGresult* res, int row, int col) {
    if (PQgetisnull(res, row, col)) return "";
    return PQgetvalue(res, row, col);
}

int64_t PgClient::field_int64(PGresult* res, int row, int col) {
    if (PQgetisnull(res, row, col)) return 0;
    return std::stoll(PQgetvalue(res, row, col));
}

int32_t PgClient::field_int32(PGresult* res, int row, int col) {
    if (PQgetisnull(res, row, col)) return 0;
    return std::stoi(PQgetvalue(res, row, col));
}


int64_t PgClient::get_table_id(PGconn* conn, const std::string& table_name) {
    const char* params[] = { table_name.c_str() };
    PGresult* res = PQexecParams(conn,
        "SELECT table_id FROM tables WHERE table_name = $1 AND is_deleted = false",
        1, nullptr, params, nullptr, nullptr, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        PQclear(res);
        return -1;
    }

    int64_t id = field_int64(res, 0, 0);
    PQclear(res);
    return id;
}


bool PgClient::create_table(PGconn* conn, const std::string& table_name,
                             const std::string& schema_json,
                             const std::string& partition_spec,
                             const std::string& properties_json)
{
    const char* params[] = {
        table_name.c_str(),
        schema_json.c_str(),
        partition_spec.c_str(),
        properties_json.c_str()
    };

    PGresult* res = PQexecParams(conn,
        "INSERT INTO tables (table_name, schema_json, partition_spec, properties) "
        "VALUES ($1, $2::jsonb, $3, $4::jsonb) ON CONFLICT (table_name) DO NOTHING",
        4, nullptr, params, nullptr, nullptr, 0);

    bool ok = PQresultStatus(res) == PGRES_COMMAND_OK;
    if (!ok) {
        std::cerr << "[PgClient] create_table failed: " << PQerrorMessage(conn) << "\n";
    }
    PQclear(res);
    return ok;
}

std::optional<TableRow> PgClient::get_table(PGconn* conn, const std::string& table_name) {
    const char* params[] = { table_name.c_str() };
    PGresult* res = PQexecParams(conn,
        "SELECT table_id, table_name, schema_json::text, schema_version, "
        "       partition_spec, current_snapshot_id, properties::text "
        "FROM tables WHERE table_name = $1 AND is_deleted = false",
        1, nullptr, params, nullptr, nullptr, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        PQclear(res);
        return std::nullopt;
    }

    TableRow row;
    row.table_id            = field_int64(res, 0, 0);
    row.table_name          = field_str(res, 0, 1);
    row.schema_json         = field_str(res, 0, 2);
    row.schema_version      = field_int32(res, 0, 3);
    row.partition_spec      = field_str(res, 0, 4);
    row.current_snapshot_id = field_int64(res, 0, 5);
    row.properties_json     = field_str(res, 0, 6);
    PQclear(res);
    return row;
}

bool PgClient::update_table_schema(PGconn* conn, const std::string& table_name,
                                    const std::string& new_schema_json,
                                    int32_t new_version,
                                    const std::string& change_summary)
{
    std::string ver_str = std::to_string(new_version);
    const char* params1[] = { new_schema_json.c_str(), ver_str.c_str(), table_name.c_str() };
    PGresult* res = PQexecParams(conn,
        "UPDATE tables SET schema_json = $1::jsonb, schema_version = $2, "
        "updated_at = now() WHERE table_name = $3 AND is_deleted = false",
        3, nullptr, params1, nullptr, nullptr, 0);

    bool ok = PQresultStatus(res) == PGRES_COMMAND_OK;
    PQclear(res);
    if (!ok) return false;

    int64_t table_id = get_table_id(conn, table_name);
    if (table_id < 0) return false;

    std::string tid_str = std::to_string(table_id);
    const char* params2[] = {
        tid_str.c_str(), ver_str.c_str(),
        new_schema_json.c_str(), change_summary.c_str()
    };
    res = PQexecParams(conn,
        "INSERT INTO schema_history (table_id, schema_version, schema_json, change_summary) "
        "VALUES ($1, $2, $3::jsonb, $4)",
        4, nullptr, params2, nullptr, nullptr, 0);

    ok = PQresultStatus(res) == PGRES_COMMAND_OK;
    PQclear(res);
    return ok;
}

bool PgClient::rename_table(PGconn* conn, const std::string& old_name,
                             const std::string& new_name)
{
    const char* params[] = { new_name.c_str(), old_name.c_str() };
    PGresult* res = PQexecParams(conn,
        "UPDATE tables SET table_name = $1, updated_at = now() "
        "WHERE table_name = $2 AND is_deleted = false",
        2, nullptr, params, nullptr, nullptr, 0);

    bool ok = PQresultStatus(res) == PGRES_COMMAND_OK;
    PQclear(res);
    return ok;
}

bool PgClient::drop_table(PGconn* conn, const std::string& table_name, bool purge) {
    if (purge) {
        int64_t table_id = get_table_id(conn, table_name);
        if (table_id < 0) return false;

        std::string tid_str = std::to_string(table_id);
        const char* p[] = { tid_str.c_str() };

        PQexecParams(conn, "DELETE FROM partitions WHERE table_id = $1", 1, nullptr, p, nullptr, nullptr, 0);
        PQexecParams(conn, "DELETE FROM snapshots WHERE table_id = $1", 1, nullptr, p, nullptr, nullptr, 0);
        PQexecParams(conn, "DELETE FROM schema_history WHERE table_id = $1", 1, nullptr, p, nullptr, nullptr, 0);

        const char* params[] = { table_name.c_str() };
        PGresult* res = PQexecParams(conn,
            "DELETE FROM tables WHERE table_name = $1", 1, nullptr, params, nullptr, nullptr, 0);
        bool ok = PQresultStatus(res) == PGRES_COMMAND_OK;
        PQclear(res);
        return ok;
    } else {
        const char* params[] = { table_name.c_str() };
        PGresult* res = PQexecParams(conn,
            "UPDATE tables SET is_deleted = true, updated_at = now() WHERE table_name = $1",
            1, nullptr, params, nullptr, nullptr, 0);
        bool ok = PQresultStatus(res) == PGRES_COMMAND_OK;
        PQclear(res);
        return ok;
    }
}

std::vector<TableRow> PgClient::list_tables(PGconn* conn, const std::string& /*ns*/,
                                             int32_t page_size, const std::string& page_token)
{
    std::string limit_str = std::to_string(page_size > 0 ? page_size : 100);
    std::string offset_str = page_token.empty() ? "0" : page_token;

    const char* params[] = { limit_str.c_str(), offset_str.c_str() };
    PGresult* res = PQexecParams(conn,
        "SELECT table_id, table_name, schema_json::text, schema_version, "
        "       partition_spec, current_snapshot_id, properties::text "
        "FROM tables WHERE is_deleted = false ORDER BY table_id LIMIT $1 OFFSET $2",
        2, nullptr, params, nullptr, nullptr, 0);

    std::vector<TableRow> rows;
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return rows;
    }

    int n = PQntuples(res);
    rows.reserve(n);
    for (int i = 0; i < n; ++i) {
        TableRow row;
        row.table_id            = field_int64(res, i, 0);
        row.table_name          = field_str(res, i, 1);
        row.schema_json         = field_str(res, i, 2);
        row.schema_version      = field_int32(res, i, 3);
        row.partition_spec      = field_str(res, i, 4);
        row.current_snapshot_id = field_int64(res, i, 5);
        row.properties_json     = field_str(res, i, 6);
        rows.push_back(std::move(row));
    }
    PQclear(res);
    return rows;
}


std::optional<std::vector<PartitionRow>> PgClient::query_partitions(
    PGconn* conn, const std::string& table_name, uint64_t snapshot_id)
{
    int64_t table_id = get_table_id(conn, table_name);
    if (table_id < 0) return std::nullopt;

    std::string tid_str  = std::to_string(table_id);
    std::string snap_str = std::to_string(snapshot_id);
    const char* params[] = { tid_str.c_str(), snap_str.c_str() };

    PGresult* res = PQexecParams(conn,
        "SELECT partition_id, table_id, snapshot_id, partition_key, "
        "       data_file_path, file_format, row_count, size_bytes, column_stats::text "
        "FROM partitions "
        "WHERE table_id = $1 AND snapshot_id <= $2 AND is_deleted = false "
        "ORDER BY partition_id",
        2, nullptr, params, nullptr, nullptr, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return std::nullopt;
    }

    int n = PQntuples(res);
    std::vector<PartitionRow> rows;
    rows.reserve(n);
    for (int i = 0; i < n; ++i) {
        PartitionRow r;
        r.partition_id      = field_int64(res, i, 0);
        r.table_id          = field_int64(res, i, 1);
        r.snapshot_id       = field_int64(res, i, 2);
        r.partition_key     = field_str(res, i, 3);
        r.data_file_path    = field_str(res, i, 4);
        r.file_format       = field_str(res, i, 5);
        r.row_count         = field_int64(res, i, 6);
        r.size_bytes        = field_int64(res, i, 7);
        r.column_stats_json = field_str(res, i, 8);
        rows.push_back(std::move(r));
    }
    PQclear(res);
    return rows;
}

std::optional<std::vector<PartitionRow>> PgClient::query_partitions_paged(
    PGconn* conn, const std::string& table_name, uint64_t snapshot_id,
    int32_t page_size, int64_t last_partition_id)
{
    int64_t table_id = get_table_id(conn, table_name);
    if (table_id < 0) return std::nullopt;

    std::string tid_str    = std::to_string(table_id);
    std::string snap_str   = std::to_string(snapshot_id);
    std::string last_str   = std::to_string(last_partition_id);
    std::string limit_str  = std::to_string(page_size > 0 ? page_size : 1000);

    const char* params[] = { tid_str.c_str(), snap_str.c_str(), last_str.c_str(), limit_str.c_str() };

    PGresult* res = PQexecParams(conn,
        "SELECT partition_id, table_id, snapshot_id, partition_key, "
        "       data_file_path, file_format, row_count, size_bytes, column_stats::text "
        "FROM partitions "
        "WHERE table_id = $1 AND snapshot_id <= $2 AND is_deleted = false "
        "  AND partition_id > $3 "
        "ORDER BY partition_id LIMIT $4",
        4, nullptr, params, nullptr, nullptr, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return std::nullopt;
    }

    int n = PQntuples(res);
    std::vector<PartitionRow> rows;
    rows.reserve(n);
    for (int i = 0; i < n; ++i) {
        PartitionRow r;
        r.partition_id      = field_int64(res, i, 0);
        r.table_id          = field_int64(res, i, 1);
        r.snapshot_id       = field_int64(res, i, 2);
        r.partition_key     = field_str(res, i, 3);
        r.data_file_path    = field_str(res, i, 4);
        r.file_format       = field_str(res, i, 5);
        r.row_count         = field_int64(res, i, 6);
        r.size_bytes        = field_int64(res, i, 7);
        r.column_stats_json = field_str(res, i, 8);
        rows.push_back(std::move(r));
    }
    PQclear(res);
    return rows;
}

bool PgClient::insert_partition(PGconn* conn, const std::string& table_name,
                                 uint64_t snapshot_id, const PartitionRow& part)
{
    int64_t table_id = get_table_id(conn, table_name);
    if (table_id < 0) return false;

    std::string tid_str  = std::to_string(table_id);
    std::string snap_str = std::to_string(snapshot_id);
    std::string rc_str   = std::to_string(part.row_count);
    std::string sz_str   = std::to_string(part.size_bytes);

    const char* params[] = {
        tid_str.c_str(), snap_str.c_str(),
        part.partition_key.c_str(), part.data_file_path.c_str(),
        part.file_format.c_str(), rc_str.c_str(), sz_str.c_str(),
        part.column_stats_json.empty() ? "{}" : part.column_stats_json.c_str()
    };

    PGresult* res = PQexecParams(conn,
        "INSERT INTO partitions (table_id, snapshot_id, partition_key, data_file_path, "
        "  file_format, row_count, size_bytes, column_stats) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)",
        8, nullptr, params, nullptr, nullptr, 0);

    bool ok = PQresultStatus(res) == PGRES_COMMAND_OK;
    PQclear(res);
    return ok;
}

bool PgClient::mark_partition_deleted(PGconn* conn, const std::string& table_name,
                                       const std::string& partition_key,
                                       uint64_t deleted_snapshot_id)
{
    int64_t table_id = get_table_id(conn, table_name);
    if (table_id < 0) return false;

    std::string tid_str  = std::to_string(table_id);
    std::string snap_str = std::to_string(deleted_snapshot_id);
    const char* params[] = { snap_str.c_str(), tid_str.c_str(), partition_key.c_str() };

    PGresult* res = PQexecParams(conn,
        "UPDATE partitions SET is_deleted = true, deleted_snapshot_id = $1 "
        "WHERE table_id = $2 AND partition_key = $3 AND is_deleted = false",
        3, nullptr, params, nullptr, nullptr, 0);

    bool ok = PQresultStatus(res) == PGRES_COMMAND_OK;
    PQclear(res);
    return ok;
}


uint64_t PgClient::insert_snapshot(PGconn* conn, const std::string& table_name,
                                    uint64_t parent_snapshot_id,
                                    const std::string& operation,
                                    int32_t added_count, int32_t deleted_count)
{
    int64_t table_id = get_table_id(conn, table_name);
    if (table_id < 0) return 0;

    std::string tid_str  = std::to_string(table_id);
    std::string par_str  = std::to_string(parent_snapshot_id);
    std::string add_str  = std::to_string(added_count);
    std::string del_str  = std::to_string(deleted_count);

    const char* params[] = {
        tid_str.c_str(), par_str.c_str(), operation.c_str(),
        add_str.c_str(), del_str.c_str()
    };

    PGresult* res = PQexecParams(conn,
        "INSERT INTO snapshots (table_id, parent_snapshot_id, operation, "
        "  added_files_count, deleted_files_count) "
        "VALUES ($1, $2, $3, $4, $5) RETURNING snapshot_id",
        5, nullptr, params, nullptr, nullptr, 0);

    uint64_t snap_id = 0;
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        snap_id = static_cast<uint64_t>(field_int64(res, 0, 0));
    }
    PQclear(res);
    return snap_id;
}

std::optional<SnapshotRow> PgClient::get_snapshot(PGconn* conn,
                                                   const std::string& table_name,
                                                   uint64_t snapshot_id)
{
    int64_t table_id = get_table_id(conn, table_name);
    if (table_id < 0) return std::nullopt;

    std::string tid_str  = std::to_string(table_id);
    std::string snap_str = std::to_string(snapshot_id);
    const char* params[] = { tid_str.c_str(), snap_str.c_str() };

    PGresult* res = PQexecParams(conn,
        "SELECT snapshot_id, table_id, parent_snapshot_id, operation, "
        "       added_files_count, deleted_files_count, committed_at::text "
        "FROM snapshots WHERE table_id = $1 AND snapshot_id = $2",
        2, nullptr, params, nullptr, nullptr, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        PQclear(res);
        return std::nullopt;
    }

    SnapshotRow row;
    row.snapshot_id        = field_int64(res, 0, 0);
    row.table_id           = field_int64(res, 0, 1);
    row.parent_snapshot_id = field_int64(res, 0, 2);
    row.operation          = field_str(res, 0, 3);
    row.added_files_count  = field_int32(res, 0, 4);
    row.deleted_files_count = field_int32(res, 0, 5);
    row.committed_at       = field_str(res, 0, 6);
    PQclear(res);
    return row;
}

std::vector<SnapshotRow> PgClient::list_snapshots(PGconn* conn,
                                                   const std::string& table_name,
                                                   int32_t limit)
{
    int64_t table_id = get_table_id(conn, table_name);
    std::vector<SnapshotRow> rows;
    if (table_id < 0) return rows;

    std::string tid_str   = std::to_string(table_id);
    std::string limit_str = std::to_string(limit > 0 ? limit : 50);
    const char* params[]  = { tid_str.c_str(), limit_str.c_str() };

    PGresult* res = PQexecParams(conn,
        "SELECT snapshot_id, table_id, parent_snapshot_id, operation, "
        "       added_files_count, deleted_files_count, committed_at::text "
        "FROM snapshots WHERE table_id = $1 ORDER BY committed_at DESC LIMIT $2",
        2, nullptr, params, nullptr, nullptr, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return rows;
    }

    int n = PQntuples(res);
    rows.reserve(n);
    for (int i = 0; i < n; ++i) {
        SnapshotRow row;
        row.snapshot_id        = field_int64(res, i, 0);
        row.table_id           = field_int64(res, i, 1);
        row.parent_snapshot_id = field_int64(res, i, 2);
        row.operation          = field_str(res, i, 3);
        row.added_files_count  = field_int32(res, i, 4);
        row.deleted_files_count = field_int32(res, i, 5);
        row.committed_at       = field_str(res, i, 6);
        rows.push_back(std::move(row));
    }
    PQclear(res);
    return rows;
}

uint64_t PgClient::get_current_snapshot(PGconn* conn, const std::string& table_name) {
    const char* params[] = { table_name.c_str() };
    PGresult* res = PQexecParams(conn,
        "SELECT current_snapshot_id FROM tables WHERE table_name = $1 AND is_deleted = false",
        1, nullptr, params, nullptr, nullptr, 0);

    uint64_t snap = 0;
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        snap = static_cast<uint64_t>(field_int64(res, 0, 0));
    }
    PQclear(res);
    return snap;
}

bool PgClient::update_table_snapshot(PGconn* conn, const std::string& table_name,
                                      uint64_t snapshot_id)
{
    std::string snap_str = std::to_string(snapshot_id);
    const char* params[] = { snap_str.c_str(), table_name.c_str() };

    PGresult* res = PQexecParams(conn,
        "UPDATE tables SET current_snapshot_id = $1, updated_at = now() "
        "WHERE table_name = $2 AND is_deleted = false",
        2, nullptr, params, nullptr, nullptr, 0);

    bool ok = PQresultStatus(res) == PGRES_COMMAND_OK;
    PQclear(res);
    return ok;
}

uint64_t PgClient::insert_transaction(PGconn* conn, const std::string& client_id,
                                       uint64_t read_snapshot_id,
                                       const std::string& isolation)
{
    std::string snap_str = std::to_string(read_snapshot_id);
    const char* params[] = { client_id.c_str(), snap_str.c_str(), isolation.c_str() };

    PGresult* res = PQexecParams(conn,
        "INSERT INTO transactions (client_id, read_snapshot_id, isolation_level) "
        "VALUES ($1, $2, $3) RETURNING txn_id",
        3, nullptr, params, nullptr, nullptr, 0);

    uint64_t txn_id = 0;
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        txn_id = static_cast<uint64_t>(field_int64(res, 0, 0));
    }
    PQclear(res);
    return txn_id;
}

bool PgClient::update_transaction_status(PGconn* conn, uint64_t txn_id,
                                          const std::string& status)
{
    std::string tid_str = std::to_string(txn_id);
    const char* params[] = { status.c_str(), tid_str.c_str() };

    std::string query = "UPDATE transactions SET status = $1";
    if (status == "committed") {
        query += ", committed_at = now()";
    }
    query += " WHERE txn_id = $2";

    PGresult* res = PQexecParams(conn, query.c_str(),
        2, nullptr, params, nullptr, nullptr, 0);

    bool ok = PQresultStatus(res) == PGRES_COMMAND_OK;
    PQclear(res);
    return ok;
}
