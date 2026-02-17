#include <grpcpp/grpcpp.h>
#include <csignal>
#include <iostream>
#include <thread>
#include <atomic>

#include "config/server_config.h"
#include "server/metadata_server.h"

static std::atomic<bool> running{true};
static std::unique_ptr<grpc::Server> grpc_server;

void signal_handler(int signum) {
    std::cout << "\n[main] received signal " << signum << ", shutting down...\n";
    running.store(false);
    if (grpc_server) {
        grpc_server->Shutdown();
    }
}

/**
 * Background thread that periodically cleans up expired transactions.
 */
void cleanup_loop(MVCCManager& mvcc, std::chrono::seconds interval) {
    while (running.load()) {
        std::this_thread::sleep_for(interval);
        size_t cleaned = mvcc.cleanup_expired_transactions();
        if (cleaned > 0) {
            std::cout << "[cleanup] garbage-collected " << cleaned
                      << " expired transactions\n";
        }
    }
}

int main(int argc, char* argv[]) {
    // Register signal handlers for graceful shutdown
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Load configuration from environment / defaults
    auto config = ServerConfig::from_env();

    std::cout << "╔══════════════════════════════════════════╗\n";
    std::cout << "║   IceLog — Metadata Control Plane        ║\n";
    std::cout << "╠══════════════════════════════════════════╣\n";
    std::cout << "║  gRPC address : " << config.grpc_address << "\n";
    std::cout << "║  PG pool size : " << config.pool_size << "\n";
    std::cout << "║  Cache cap    : " << config.cache_capacity << "\n";
    std::cout << "║  Txn timeout  : " << config.txn_timeout_s << "s\n";
    std::cout << "╚══════════════════════════════════════════╝\n";

    // Initialize the gRPC service
    MetadataServer service(config);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(config.grpc_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    // Tune gRPC thread pool for high concurrency
    builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::NUM_CQS, 4);
    builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MIN_POLLERS, 2);
    builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, 8);

    grpc_server = builder.BuildAndStart();
    if (!grpc_server) {
        std::cerr << "[main] failed to start gRPC server on " << config.grpc_address << "\n";
        return 1;
    }

    std::cout << "[main] server listening on " << config.grpc_address << "\n";

    // Start background cleanup thread
    // (In production, this would also report metrics to Prometheus)
    // Note: We'd need to pass the MVCCManager from MetadataServer;
    // for now this is a placeholder showing the architecture.

    grpc_server->Wait();

    std::cout << "[main] server stopped\n";
    return 0;
}
