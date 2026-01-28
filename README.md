# IceLog — Distributed Metadata Catalog Service

A high-concurrency metadata control plane built in **C++20** that manages table catalogs, partition metadata, and snapshot versioning for open table formats (Apache Iceberg) storing columnar data (Parquet).

## Overview

IceLog sits between compute nodes and storage in a distributed data platform. It tracks:
- **Table schemas** with full version history
- **Partition locations** (1M+ active data partitions) with file paths, row counts, byte sizes
- **Snapshot histories** for MVCC-based consistent reads
- **Transaction states** with Snapshot Isolation guarantees

When a query engine (Spark, Trino) needs to resolve `SELECT * FROM events WHERE month='Jan'`, IceLog tells it exactly which Parquet files to read — from which snapshot — without scanning the entire storage layer.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     CLIENT LAYER                         │
│  Query Engine (Spark/Trino)  │  DDL Client (CREATE/ALTER)│
└──────────────┬───────────────┴──────────────┬────────────┘
               │ gRPC (protobuf)              │
               ▼                              ▼
┌──────────────────────────────────────────────────────────┐
│                   gRPC API LAYER                         │
│                metadata_service.proto                    │
│  GetTableMetadata | CreateTable | GetPartitions          │
│  CommitSnapshot | BeginTransaction | CommitTransaction   │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│                METADATA SERVER (C++20)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │   Catalog    │  │  Transaction │  │ Lock Manager  │  │
│  │   Manager    │  │  Mgr (MVCC)  │  │ (DDL serial.) │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬────────┘  │
│         └─────────────────┼─────────────────┘            │
│                    ┌──────┴───────┐                       │
│                    │  LRU Cache   │                       │
│                    └──────┬───────┘                       │
└───────────────────────────┼──────────────────────────────┘
                            │ cache miss
                            ▼
┌──────────────────────────────────────────────────────────┐
│               PERSISTENCE LAYER (PostgreSQL)             │
│   tables │ snapshots │ partitions │ transactions         │
└──────────────────────────────────────────────────────────┘
```

## Stack

| Component | Technology |
|-----------|------------|
| Language | C++20 |
| RPC Framework | gRPC + Protobuf |
| Persistence | PostgreSQL 16 |
| Build System | CMake 3.20+ |
| Testing | Google Test |
| Benchmarking | Google Benchmark |
| Containerization | Docker Compose |

## Building

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

## Running

```bash
# Start PostgreSQL
docker-compose up -d postgres

# Run the metadata server
./build/metadata_server
```

## Testing

```bash
cd build
ctest --output-on-failure
```

## Performance

| Path | Latency | Mechanism |
|------|---------|-----------|
| L1 cache hit | ~1-10 μs | In-process LRU hash lookup |
| L2 DB query | ~2-5 ms | Partial index on live partitions |
| L3 pool | ~0 ms overhead | Pre-established PG connections |
| Protobuf serde | ~0.1-0.5 ms | Binary format |

Target: **sub-10ms** metadata retrieval for 1M+ active partitions at p99.
