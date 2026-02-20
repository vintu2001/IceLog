#!/usr/bin/env python3
"""Seed script: generate test partition data for benchmarking."""

import argparse
import os
import random
import sys
import time

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)


def get_connection():
    conn_str = os.environ.get(
        "PG_CONN_STRING",
        "host=localhost port=5432 dbname=metadata user=metadata_user password=metadata_pass"
    )
    return psycopg2.connect(conn_str)


def ensure_table_exists(conn, table_name, schema_json):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO tables (table_name, schema_json) "
            "VALUES (%s, %s::jsonb) ON CONFLICT (table_name) DO NOTHING",
            (table_name, schema_json)
        )
        conn.commit()

        cur.execute("SELECT table_id FROM tables WHERE table_name = %s", (table_name,))
        row = cur.fetchone()
        return row[0] if row else None


def create_snapshot(conn, table_id, parent_snap_id, operation, added_count):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO snapshots (table_id, parent_snapshot_id, operation, added_files_count) "
            "VALUES (%s, %s, %s, %s) RETURNING snapshot_id",
            (table_id, parent_snap_id, operation, added_count)
        )
        snap_id = cur.fetchone()[0]
        conn.commit()
        return snap_id


def seed_partitions(conn, table_id, snapshot_id, num_partitions, batch_size=5000):
    total_inserted = 0
    start = time.time()

    months = [f"2025-{m:02d}" for m in range(1, 13)]
    file_formats = ["parquet"]

    with conn.cursor() as cur:
        batch = []
        for i in range(num_partitions):
            month = months[i % 12]
            partition_key = f"month={month}"
            data_file_path = f"s3://icelog-data/events/month={month}/part-{i:08d}.parquet"
            row_count = random.randint(50_000, 500_000)
            size_bytes = row_count * random.randint(80, 150)  # ~80-150 bytes per row
            column_stats = '{"event_time": {"min": "2025-01-01", "max": "2025-12-31"}}'

            batch.append((
                table_id, snapshot_id, partition_key, data_file_path,
                file_formats[0], row_count, size_bytes, column_stats
            ))

            if len(batch) >= batch_size:
                execute_values(
                    cur,
                    "INSERT INTO partitions "
                    "(table_id, snapshot_id, partition_key, data_file_path, "
                    " file_format, row_count, size_bytes, column_stats) "
                    "VALUES %s",
                    batch,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s::jsonb)"
                )
                conn.commit()
                total_inserted += len(batch)
                elapsed = time.time() - start
                rate = total_inserted / elapsed if elapsed > 0 else 0
                print(f"  inserted {total_inserted:,}/{num_partitions:,} "
                      f"({rate:,.0f} rows/sec)")
                batch = []

        if batch:
            execute_values(
                cur,
                "INSERT INTO partitions "
                "(table_id, snapshot_id, partition_key, data_file_path, "
                " file_format, row_count, size_bytes, column_stats) "
                "VALUES %s",
                batch,
                template="(%s, %s, %s, %s, %s, %s, %s, %s::jsonb)"
            )
            conn.commit()
            total_inserted += len(batch)

    elapsed = time.time() - start
    print(f"\nDone: inserted {total_inserted:,} partitions in {elapsed:.1f}s "
          f"({total_inserted / elapsed:,.0f} rows/sec)")
    return total_inserted


def update_table_snapshot(conn, table_name, snapshot_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tables SET current_snapshot_id = %s WHERE table_name = %s",
            (snapshot_id, table_name)
        )
        conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Seed partition data for benchmarking")
    parser.add_argument("--table", default="events", help="Table name to seed")
    parser.add_argument("--partitions", type=int, default=1_000_000,
                        help="Number of partitions to generate")
    parser.add_argument("--schema", default='{"fields": [{"name": "event_id", "type": "long"}, '
                                            '{"name": "event_time", "type": "timestamp"}, '
                                            '{"name": "user_id", "type": "long"}, '
                                            '{"name": "event_type", "type": "string"}]}',
                        help="JSON schema for the table")
    args = parser.parse_args()

    print(f"Seeding {args.partitions:,} partitions for table '{args.table}'...")
    print()

    conn = get_connection()

    table_id = ensure_table_exists(conn, args.table, args.schema)
    if table_id is None:
        print(f"Error: could not create or find table '{args.table}'")
        sys.exit(1)
    print(f"Table ID: {table_id}")

    snapshot_id = create_snapshot(conn, table_id, 0, "append", args.partitions)
    print(f"Snapshot ID: {snapshot_id}")

    seed_partitions(conn, table_id, snapshot_id, args.partitions)

    update_table_snapshot(conn, args.table, snapshot_id)
    print(f"\nTable '{args.table}' current_snapshot_id = {snapshot_id}")

    conn.close()


if __name__ == "__main__":
    main()
