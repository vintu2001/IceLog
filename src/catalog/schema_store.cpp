#include "schema_store.h"
#include <iostream>

SchemaStore::SchemaStore(ConnectionPool& pool, PgClient& pg, LockManager& locks)
    : pool_(pool), pg_(pg), locks_(locks) {}

std::optional<SchemaStore::SchemaVersion> SchemaStore::get_current_schema(
    const std::string& table_name)
{
    auto guard = locks_.scoped_shared(table_name);
    auto conn = pool_.acquire();

    auto table = pg_.get_table(conn.get(), table_name);
    if (!table.has_value()) return std::nullopt;

    return SchemaVersion{
        .version        = table->schema_version,
        .schema_json    = table->schema_json,
        .changed_at     = "",
        .change_summary = "current"
    };
}

std::optional<SchemaStore::SchemaVersion> SchemaStore::get_schema_at_version(
    const std::string& table_name, int32_t version)
{
    auto guard = locks_.scoped_shared(table_name);
    auto conn = pool_.acquire();

    int64_t table_id = pg_.get_table_id(conn.get(), table_name);
    if (table_id < 0) return std::nullopt;

    std::string tid_str = std::to_string(table_id);
    std::string ver_str = std::to_string(version);
    const char* params[] = { tid_str.c_str(), ver_str.c_str() };

    PGresult* res = PQexecParams(conn.get(),
        "SELECT schema_version, schema_json::text, changed_at::text, change_summary "
        "FROM schema_history WHERE table_id = $1 AND schema_version = $2",
        2, nullptr, params, nullptr, nullptr, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        PQclear(res);
        return std::nullopt;
    }

    SchemaVersion sv;
    sv.version        = std::stoi(PQgetvalue(res, 0, 0));
    sv.schema_json    = PQgetvalue(res, 0, 1);
    sv.changed_at     = PQgetvalue(res, 0, 2);
    sv.change_summary = PQgetisnull(res, 0, 3) ? "" : PQgetvalue(res, 0, 3);
    PQclear(res);
    return sv;
}

std::vector<SchemaStore::SchemaVersion> SchemaStore::list_schema_history(
    const std::string& table_name)
{
    auto guard = locks_.scoped_shared(table_name);
    auto conn = pool_.acquire();

    int64_t table_id = pg_.get_table_id(conn.get(), table_name);
    std::vector<SchemaVersion> history;
    if (table_id < 0) return history;

    std::string tid_str = std::to_string(table_id);
    const char* params[] = { tid_str.c_str() };

    PGresult* res = PQexecParams(conn.get(),
        "SELECT schema_version, schema_json::text, changed_at::text, change_summary "
        "FROM schema_history WHERE table_id = $1 ORDER BY schema_version DESC",
        1, nullptr, params, nullptr, nullptr, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return history;
    }

    int n = PQntuples(res);
    history.reserve(n);
    for (int i = 0; i < n; ++i) {
        SchemaVersion sv;
        sv.version        = std::stoi(PQgetvalue(res, i, 0));
        sv.schema_json    = PQgetvalue(res, i, 1);
        sv.changed_at     = PQgetvalue(res, i, 2);
        sv.change_summary = PQgetisnull(res, i, 3) ? "" : PQgetvalue(res, i, 3);
        history.push_back(std::move(sv));
    }
    PQclear(res);
    return history;
}

std::string SchemaStore::validate_schema_change(const std::string& current_json,
                                                 const std::string& proposed_json)
{
    if (proposed_json.empty() || proposed_json == "{}") {
        return "Proposed schema cannot be empty";
    }
    if (proposed_json.front() != '{' || proposed_json.back() != '}') {
        return "Proposed schema must be valid JSON object";
    }

    return "";
}
