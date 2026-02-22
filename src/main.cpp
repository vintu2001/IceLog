#include <atomic>
#include <csignal>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <thread>

#include "config/server_config.h"
#include "server/metadata_server.h"

static std::atomic<bool> running{true};
static std::unique_ptr<grpc::Server> grpc_server_ptr;

void signal_handler(int signum) {
  std::cout << "\n[main] received signal " << signum << ", shutting down...\n";
  running.store(false);
  if (grpc_server_ptr) {
    grpc_server_ptr->Shutdown();
  }
}

void cleanup_loop(MVCCManager &mvcc, std::chrono::seconds interval) {
  while (running.load()) {
    std::this_thread::sleep_for(interval);
    size_t cleaned = mvcc.cleanup_expired_transactions();
    if (cleaned > 0) {
      std::cout << "[cleanup] garbage-collected " << cleaned
                << " expired transactions\n";
    }
  }
}

int main(int argc, char *argv[]) {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  auto config = ServerConfig::from_env();

  std::cout << "[IceLog] Metadata Control Plane\n";
  std::cout << "  gRPC address : " << config.grpc_address << "\n";
  std::cout << "  PG pool size : " << config.pool_size << "\n";
  std::cout << "  Cache cap    : " << config.cache_capacity << "\n";
  std::cout << "  Txn timeout  : " << config.txn_timeout_s << "s\n";

  MetadataServer service(config);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(config.grpc_address,
                           grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::NUM_CQS,
                              4);
  builder.SetSyncServerOption(
      grpc::ServerBuilder::SyncServerOption::MIN_POLLERS, 2);
  builder.SetSyncServerOption(
      grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, 8);

  grpc_server_ptr = builder.BuildAndStart();
  if (!grpc_server_ptr) {
    std::cerr << "[main] failed to start gRPC server on " << config.grpc_address
              << "\n";
    return 1;
  }

  std::cout << "[main] server listening on " << config.grpc_address << "\n";

  grpc_server_ptr->Wait();

  std::cout << "[main] server stopped\n";
  return 0;
}
