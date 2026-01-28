-- IceLog: Metadata Control Plane — PostgreSQL Schema
-- Run this on first boot to bootstrap all required tables and indexes.

-- ══════════════════════════════════════════════════
-- TABLE CATALOG
-- ══════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS tables (
    table_id            BIGSERIAL PRIMARY KEY,
    table_name          TEXT UNIQUE NOT NULL,
    schema_json         JSONB NOT NULL,
    schema_version      INT NOT NULL DEFAULT 1,
    partition_spec      TEXT,
    current_snapshot_id BIGINT DEFAULT 0,
    properties          JSONB DEFAULT '{}',
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),
    is_deleted          BOOLEAN DEFAULT false
);

-- Schema evolution history — every ALTER TABLE is recorded
CREATE TABLE IF NOT EXISTS schema_history (
    schema_history_id BIGSERIAL PRIMARY KEY,
    table_id          BIGINT REFERENCES tables(table_id),
    schema_version    INT NOT NULL,
    schema_json       JSONB NOT NULL,
    changed_at        TIMESTAMPTZ DEFAULT now(),
    change_summary    TEXT
);

-- ══════════════════════════════════════════════════
-- SNAPSHOT MANAGEMENT
-- ══════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id         BIGSERIAL PRIMARY KEY,
    table_id            BIGINT REFERENCES tables(table_id),
    parent_snapshot_id  BIGINT,
    committed_at        TIMESTAMPTZ DEFAULT now(),
    operation           TEXT NOT NULL,
    added_files_count   INT DEFAULT 0,
    deleted_files_count INT DEFAULT 0,
    summary             JSONB DEFAULT '{}'
);

CREATE INDEX idx_snapshots_table_time
    ON snapshots(table_id, committed_at DESC);

-- ══════════════════════════════════════════════════
-- PARTITION REGISTRY
-- ══════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS partitions (
    partition_id        BIGSERIAL PRIMARY KEY,
    table_id            BIGINT REFERENCES tables(table_id),
    snapshot_id         BIGINT REFERENCES snapshots(snapshot_id),
    partition_key       TEXT NOT NULL,
    data_file_path      TEXT NOT NULL,
    file_format         TEXT NOT NULL DEFAULT 'parquet',
    row_count           BIGINT,
    size_bytes          BIGINT,
    column_stats        JSONB,
    is_deleted          BOOLEAN DEFAULT false,
    deleted_snapshot_id BIGINT
);

-- Critical index for sub-10ms lookups — partial index skips soft-deleted rows
CREATE INDEX idx_partitions_table_snapshot
    ON partitions(table_id, snapshot_id)
    WHERE is_deleted = false;

-- Index for partition pruning by key
CREATE INDEX idx_partitions_table_key
    ON partitions(table_id, partition_key)
    WHERE is_deleted = false;

-- ══════════════════════════════════════════════════
-- TRANSACTION TRACKING
-- ══════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS transactions (
    txn_id            BIGSERIAL PRIMARY KEY,
    client_id         TEXT NOT NULL,
    read_snapshot_id  BIGINT,
    status            TEXT DEFAULT 'active',
    isolation_level   TEXT DEFAULT 'snapshot',
    started_at        TIMESTAMPTZ DEFAULT now(),
    committed_at      TIMESTAMPTZ,
    timeout_at        TIMESTAMPTZ DEFAULT now() + INTERVAL '5 minutes'
);

CREATE INDEX idx_txn_active
    ON transactions(status)
    WHERE status = 'active';
