#pragma once

#include <grpcpp/grpcpp.h>
#include "metadata_service.grpc.pb.h"

#include "catalog/catalog_manager.h"
#include "catalog/partition_registry.h"
#include "catalog/schema_store.h"
#include "transaction/mvcc_manager.h"
#include "lock/lock_manager.h"
#include "db/pg_client.h"
#include "db/connection_pool.h"
#include "cache/lru_cache.h"
#include "config/server_config.h"

#include <memory>

/**
 * gRPC service implementation for the metadata control plane.
 *
 * Delegates each RPC to the appropriate domain manager:
 *   - Table ops    → CatalogManager
 *   - Partitions   → PartitionRegistry
 *   - Snapshots    → PartitionRegistry (commit) + PgClient (query)
 *   - Transactions → MVCCManager + PgClient
 */
class MetadataServer final : public metadata::MetadataService::Service {
public:
    explicit MetadataServer(const ServerConfig& config);

    // ── Table Operations ────────────────────────────

    grpc::Status CreateTable(grpc::ServerContext* ctx,
                             const metadata::CreateTableRequest* req,
                             metadata::OperationResponse* resp) override;

    grpc::Status GetTableMetadata(grpc::ServerContext* ctx,
                                  const metadata::TableRequest* req,
                                  metadata::TableMetadataResponse* resp) override;

    grpc::Status AlterTable(grpc::ServerContext* ctx,
                            const metadata::AlterTableRequest* req,
                            metadata::OperationResponse* resp) override;

    grpc::Status DropTable(grpc::ServerContext* ctx,
                           const metadata::DropTableRequest* req,
                           metadata::OperationResponse* resp) override;

    grpc::Status ListTables(grpc::ServerContext* ctx,
                            const metadata::ListTablesRequest* req,
                            metadata::ListTablesResponse* resp) override;

    // ── Partition Operations ────────────────────────

    grpc::Status GetPartitions(grpc::ServerContext* ctx,
                               const metadata::PartitionRequest* req,
                               metadata::PartitionListResponse* resp) override;

    grpc::Status GetPartitionStats(grpc::ServerContext* ctx,
                                   const metadata::PartitionStatsRequest* req,
                                   metadata::PartitionStatsResponse* resp) override;

    // ── Snapshot Operations ─────────────────────────

    grpc::Status CommitSnapshot(grpc::ServerContext* ctx,
                                const metadata::SnapshotRequest* req,
                                metadata::SnapshotResponse* resp) override;

    grpc::Status GetSnapshot(grpc::ServerContext* ctx,
                             const metadata::GetSnapshotRequest* req,
                             metadata::SnapshotDetail* resp) override;

    grpc::Status ListSnapshots(grpc::ServerContext* ctx,
                               const metadata::ListSnapshotsRequest* req,
                               metadata::ListSnapshotsResponse* resp) override;

    // ── Transaction Operations ──────────────────────

    grpc::Status BeginTransaction(grpc::ServerContext* ctx,
                                  const metadata::TransactionRequest* req,
                                  metadata::TransactionResponse* resp) override;

    grpc::Status CommitTransaction(grpc::ServerContext* ctx,
                                   const metadata::CommitRequest* req,
                                   metadata::OperationResponse* resp) override;

    grpc::Status AbortTransaction(grpc::ServerContext* ctx,
                                  const metadata::AbortRequest* req,
                                  metadata::OperationResponse* resp) override;

private:
    // Owned infrastructure
    std::unique_ptr<ConnectionPool> pg_pool_;
    PgClient         pg_client_;
    LockManager      lock_manager_;
    MVCCManager      mvcc_manager_;

    // Domain managers
    std::unique_ptr<CatalogManager>    catalog_;
    std::unique_ptr<PartitionRegistry> partitions_;
    std::unique_ptr<SchemaStore>       schemas_;
};
