#pragma once

#include <string>
#include <cstdint>
#include <cstdlib>

struct ServerConfig {
    std::string pg_conn_string = "host=localhost port=5432 dbname=metadata user=metadata_user password=metadata_pass";
    std::string grpc_address   = "0.0.0.0:50051";
    size_t      cache_capacity = 10000;
    size_t      pool_size      = 20;
    int         txn_timeout_s  = 300;

    // Load config from environment variables, falling back to defaults
    static ServerConfig from_env() {
        ServerConfig cfg;

        if (auto* v = std::getenv("PG_CONN_STRING"))
            cfg.pg_conn_string = v;

        if (auto* v = std::getenv("GRPC_PORT")) {
            cfg.grpc_address = std::string("0.0.0.0:") + v;
        } else if (auto* v2 = std::getenv("GRPC_ADDRESS")) {
            cfg.grpc_address = v2;
        }

        if (auto* v = std::getenv("CACHE_CAPACITY"))
            cfg.cache_capacity = static_cast<size_t>(std::stoul(v));

        if (auto* v = std::getenv("POOL_SIZE"))
            cfg.pool_size = static_cast<size_t>(std::stoul(v));

        if (auto* v = std::getenv("TXN_TIMEOUT_SEC"))
            cfg.txn_timeout_s = std::stoi(v);

        return cfg;
    }
};
