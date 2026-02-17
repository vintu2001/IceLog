#include "metadata_server.h"
#include <iostream>

// ── Constructor ─────────────────────────────────────────

MetadataServer::MetadataServer(const ServerConfig& config)
    : pg_pool_(std::make_unique<ConnectionPool>(config.pg_conn_string, config.pool_size)),
      mvcc_manager_(std::chrono::seconds(config.txn_timeout_s))
{
    catalog_    = std::make_unique<CatalogManager>(*pg_pool_, pg_client_, lock_manager_, mvcc_manager_);
    partitions_ = std::make_unique<PartitionRegistry>(*pg_pool_, pg_client_, lock_manager_, mvcc_manager_, config.cache_capacity);
    schemas_    = std::make_unique<SchemaStore>(*pg_pool_, pg_client_, lock_manager_);

    std::cout << "[MetadataServer] initialized (pool=" << config.pool_size
              << ", cache=" << config.cache_capacity << ")\n";
}

// ═══════════════════════════════════════════════════════
// TABLE OPERATIONS
// ═══════════════════════════════════════════════════════

grpc::Status MetadataServer::CreateTable(
    grpc::ServerContext* /*ctx*/,
    const metadata::CreateTableRequest* req,
    metadata::OperationResponse* resp)
{
    std::string props = "{}";
    if (req->properties_size() > 0) {
        // Serialize map to JSON — simple approach
        props = "{";
        bool first = true;
        for (const auto& [k, v] : req->properties()) {
            if (!first) props += ",";
            props += "\"" + k + "\":\"" + v + "\"";
            first = false;
        }
        props += "}";
    }

    auto result = catalog_->create_table(req->table_name(), req->schema_json(),
                                          req->partition_spec(), props);
    resp->set_success(result.success);
    resp->set_error_msg(result.error_msg);
    return grpc::Status::OK;
}

grpc::Status MetadataServer::GetTableMetadata(
    grpc::ServerContext* /*ctx*/,
    const metadata::TableRequest* req,
    metadata::TableMetadataResponse* resp)
{
    const std::string& table_name = req->table_name();
    uint64_t requested_snapshot = req->snapshot_id();

    // 1. Fetch table metadata
    auto table = catalog_->get_table(table_name);
    if (!table.has_value()) {
        return grpc::Status(grpc::NOT_FOUND, "Table not found: " + table_name);
    }

    // 2. Resolve snapshot: 0 = latest committed
    uint64_t read_snapshot = (requested_snapshot == 0)
        ? mvcc_manager_.get_latest_committed_snapshot()
        : requested_snapshot;

    // 3. Fetch partitions visible at this snapshot (cache-accelerated)
    auto parts = partitions_->get_partitions(table_name, read_snapshot);

    // 4. Build response
    resp->set_table_name(table->table_name);
    resp->set_schema_json(table->schema_json);
    resp->set_current_snapshot_id(read_snapshot);
    resp->set_schema_version(table->schema_version);

    int64_t total_rows = 0, total_bytes = 0;
    if (parts.has_value()) {
        for (const auto& p : *parts) {
            auto* pi = resp->add_partitions();
            pi->set_partition_key(p.partition_key);
            pi->set_data_file_path(p.data_file_path);
            pi->set_row_count(p.row_count);
            pi->set_size_bytes(p.size_bytes);
            pi->set_snapshot_id(static_cast<uint64_t>(p.snapshot_id));
            pi->set_file_format(p.file_format);
            total_rows  += p.row_count;
            total_bytes += p.size_bytes;
        }
    }
    resp->set_total_row_count(total_rows);
    resp->set_total_size_bytes(total_bytes);

    return grpc::Status::OK;
}

grpc::Status MetadataServer::AlterTable(
    grpc::ServerContext* /*ctx*/,
    const metadata::AlterTableRequest* req,
    metadata::OperationResponse* resp)
{
    const std::string& table_name = req->table_name();

    switch (req->alteration_case()) {
        case metadata::AlterTableRequest::kNewSchemaJson: {
            // Validate schema change
            auto current = schemas_->get_current_schema(table_name);
            if (current.has_value()) {
                auto err = schemas_->validate_schema_change(
                    current->schema_json, req->new_schema_json());
                if (!err.empty()) {
                    resp->set_success(false);
                    resp->set_error_msg(err);
                    return grpc::Status::OK;
                }
            }
            auto result = catalog_->alter_table_schema(
                table_name, req->new_schema_json(), "schema evolution via AlterTable");
            resp->set_success(result.success);
            resp->set_error_msg(result.error_msg);
            break;
        }
        case metadata::AlterTableRequest::kRename: {
            auto result = catalog_->rename_table(table_name, req->rename().new_name());
            resp->set_success(result.success);
            resp->set_error_msg(result.error_msg);
            break;
        }
        case metadata::AlterTableRequest::kNewPartitionSpec: {
            // Repartitioning — for now just update the spec
            resp->set_success(true);
            resp->set_error_msg("partition spec update not yet implemented");
            break;
        }
        default:
            resp->set_success(false);
            resp->set_error_msg("No alteration specified");
    }

    // Invalidate partition cache after DDL
    partitions_->invalidate_table_cache(table_name);
    return grpc::Status::OK;
}

grpc::Status MetadataServer::DropTable(
    grpc::ServerContext* /*ctx*/,
    const metadata::DropTableRequest* req,
    metadata::OperationResponse* resp)
{
    auto result = catalog_->drop_table(req->table_name(), req->purge());
    resp->set_success(result.success);
    resp->set_error_msg(result.error_msg);

    if (result.success) {
        partitions_->invalidate_table_cache(req->table_name());
    }
    return grpc::Status::OK;
}

grpc::Status MetadataServer::ListTables(
    grpc::ServerContext* /*ctx*/,
    const metadata::ListTablesRequest* req,
    metadata::ListTablesResponse* resp)
{
    auto tables = catalog_->list_tables(req->namespace_(), req->page_size(), req->page_token());
    for (const auto& t : tables) {
        auto* ts = resp->add_tables();
        ts->set_table_name(t.table_name);
        ts->set_current_snapshot_id(static_cast<uint64_t>(t.current_snapshot_id));
    }
    return grpc::Status::OK;
}

// ═══════════════════════════════════════════════════════
// PARTITION OPERATIONS
// ═══════════════════════════════════════════════════════

grpc::Status MetadataServer::GetPartitions(
    grpc::ServerContext* /*ctx*/,
    const metadata::PartitionRequest* req,
    metadata::PartitionListResponse* resp)
{
    int64_t last_id = 0;
    if (!req->page_token().empty()) {
        try { last_id = std::stoll(req->page_token()); } catch (...) {}
    }

    std::optional<std::vector<PartitionRow>> result;
    if (req->page_size() > 0) {
        result = partitions_->get_partitions_paged(
            req->table_name(), req->snapshot_id(), req->page_size(), last_id);
    } else {
        result = partitions_->get_partitions(req->table_name(), req->snapshot_id());
    }

    if (!result.has_value()) {
        return grpc::Status(grpc::NOT_FOUND, "Table not found: " + req->table_name());
    }

    for (const auto& p : *result) {
        auto* pi = resp->add_partitions();
        pi->set_partition_key(p.partition_key);
        pi->set_data_file_path(p.data_file_path);
        pi->set_row_count(p.row_count);
        pi->set_size_bytes(p.size_bytes);
        pi->set_snapshot_id(static_cast<uint64_t>(p.snapshot_id));
        pi->set_file_format(p.file_format);
    }

    resp->set_total_count(static_cast<int64_t>(result->size()));

    // Set next page token for pagination
    if (req->page_size() > 0 && !result->empty()) {
        resp->set_next_page_token(std::to_string(result->back().partition_id));
    }

    return grpc::Status::OK;
}

grpc::Status MetadataServer::GetPartitionStats(
    grpc::ServerContext* /*ctx*/,
    const metadata::PartitionStatsRequest* req,
    metadata::PartitionStatsResponse* resp)
{
    auto stats = partitions_->get_stats(req->table_name());
    resp->set_total_partitions(stats.total_partitions);
    resp->set_total_rows(stats.total_rows);
    resp->set_total_bytes(stats.total_bytes);
    resp->set_avg_partition_size_bytes(stats.avg_size_bytes);
    return grpc::Status::OK;
}

// ═══════════════════════════════════════════════════════
// SNAPSHOT OPERATIONS
// ═══════════════════════════════════════════════════════

grpc::Status MetadataServer::CommitSnapshot(
    grpc::ServerContext* /*ctx*/,
    const metadata::SnapshotRequest* req,
    metadata::SnapshotResponse* resp)
{
    // Convert protobuf partitions to internal rows
    std::vector<PartitionRow> new_parts;
    new_parts.reserve(req->new_partitions_size());
    for (const auto& p : req->new_partitions()) {
        PartitionRow row;
        row.partition_key     = p.partition_key();
        row.data_file_path    = p.data_file_path();
        row.file_format       = p.file_format().empty() ? "parquet" : p.file_format();
        row.row_count         = p.row_count();
        row.size_bytes        = p.size_bytes();
        row.column_stats_json = "{}";
        new_parts.push_back(std::move(row));
    }

    std::vector<std::string> deleted_keys(
        req->deleted_partition_keys().begin(),
        req->deleted_partition_keys().end());

    auto result = partitions_->commit_snapshot(
        req->table_name(), req->parent_snapshot_id(),
        req->operation().empty() ? "append" : req->operation(),
        new_parts, deleted_keys);

    resp->set_success(result.success);
    resp->set_snapshot_id(result.snapshot_id);
    resp->set_error_msg(result.error_msg);
    return grpc::Status::OK;
}

grpc::Status MetadataServer::GetSnapshot(
    grpc::ServerContext* /*ctx*/,
    const metadata::GetSnapshotRequest* req,
    metadata::SnapshotDetail* resp)
{
    auto conn = pg_pool_->acquire();
    auto snap = pg_client_.get_snapshot(conn.get(), req->table_name(), req->snapshot_id());
    if (!snap.has_value()) {
        return grpc::Status(grpc::NOT_FOUND, "Snapshot not found");
    }

    resp->set_snapshot_id(static_cast<uint64_t>(snap->snapshot_id));
    resp->set_parent_snapshot_id(static_cast<uint64_t>(snap->parent_snapshot_id));
    resp->set_operation(snap->operation);
    resp->set_added_partitions(snap->added_files_count);
    resp->set_deleted_partitions(snap->deleted_files_count);
    resp->set_committed_at(snap->committed_at);
    return grpc::Status::OK;
}

grpc::Status MetadataServer::ListSnapshots(
    grpc::ServerContext* /*ctx*/,
    const metadata::ListSnapshotsRequest* req,
    metadata::ListSnapshotsResponse* resp)
{
    auto conn = pg_pool_->acquire();
    auto snapshots = pg_client_.list_snapshots(conn.get(), req->table_name(), req->limit());

    for (const auto& s : snapshots) {
        auto* detail = resp->add_snapshots();
        detail->set_snapshot_id(static_cast<uint64_t>(s.snapshot_id));
        detail->set_parent_snapshot_id(static_cast<uint64_t>(s.parent_snapshot_id));
        detail->set_operation(s.operation);
        detail->set_added_partitions(s.added_files_count);
        detail->set_deleted_partitions(s.deleted_files_count);
        detail->set_committed_at(s.committed_at);
    }
    return grpc::Status::OK;
}

// ═══════════════════════════════════════════════════════
// TRANSACTION OPERATIONS
// ═══════════════════════════════════════════════════════

grpc::Status MetadataServer::BeginTransaction(
    grpc::ServerContext* /*ctx*/,
    const metadata::TransactionRequest* req,
    metadata::TransactionResponse* resp)
{
    auto [txn_id, read_snapshot] = mvcc_manager_.begin_transaction();

    // Persist to PostgreSQL for crash recovery
    auto conn = pg_pool_->acquire();
    std::string isolation = (req->isolation() == metadata::READ_COMMITTED)
        ? "read_committed" : "snapshot";
    pg_client_.insert_transaction(conn.get(), req->client_id(), read_snapshot, isolation);

    resp->set_txn_id(txn_id);
    resp->set_read_snapshot_id(read_snapshot);

    std::cout << "[MetadataServer] begin txn " << txn_id
              << " (read_snap=" << read_snapshot << ")\n";
    return grpc::Status::OK;
}

grpc::Status MetadataServer::CommitTransaction(
    grpc::ServerContext* /*ctx*/,
    const metadata::CommitRequest* req,
    metadata::OperationResponse* resp)
{
    bool ok = mvcc_manager_.commit_transaction(req->txn_id());

    if (ok) {
        auto conn = pg_pool_->acquire();
        pg_client_.update_transaction_status(conn.get(), req->txn_id(), "committed");
    }

    resp->set_success(ok);
    if (!ok) {
        resp->set_error_msg("Transaction " + std::to_string(req->txn_id()) +
                            " not found or expired");
    }
    return grpc::Status::OK;
}

grpc::Status MetadataServer::AbortTransaction(
    grpc::ServerContext* /*ctx*/,
    const metadata::AbortRequest* req,
    metadata::OperationResponse* resp)
{
    mvcc_manager_.abort_transaction(req->txn_id());

    auto conn = pg_pool_->acquire();
    pg_client_.update_transaction_status(conn.get(), req->txn_id(), "aborted");

    resp->set_success(true);
    std::cout << "[MetadataServer] aborted txn " << req->txn_id() << "\n";
    return grpc::Status::OK;
}
