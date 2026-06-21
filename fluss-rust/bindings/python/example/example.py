# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import asyncio
import traceback
from datetime import date, datetime
from datetime import time as dt_time
from decimal import Decimal

import pandas as pd
import pyarrow as pa

import fluss


async def main():
    # Create connection configuration
    config_spec = {
        "bootstrap.servers": "127.0.0.1:9123",
        # Add other configuration options as needed
        "writer.request-max-size": "10485760",  # 10 MB
        "writer.acks": "all",  # Wait for all replicas to acknowledge
        "writer.retries": "3",  # Retry up to 3 times on failure
        "writer.batch-size": "1000",  # Batch size for writes
    }
    config = fluss.Config(config_spec)

    # Create connection using the static create method
    conn = await fluss.FlussConnection.create(config)

    # Define fields for PyArrow
    fields = [
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float32()),
        pa.field("age", pa.int32()),
        pa.field("birth_date", pa.date32()),
        pa.field("check_in_time", pa.time32("ms")),
        pa.field("created_at", pa.timestamp("us")),  # TIMESTAMP (NTZ)
        pa.field("updated_at", pa.timestamp("us", tz="UTC")),  # TIMESTAMP_LTZ
        pa.field("salary", pa.decimal128(10, 2)),
    ]

    # Create a PyArrow schema
    schema = pa.schema(fields)

    # Create a Fluss Schema first (this is what TableDescriptor expects)
    fluss_schema = fluss.Schema(schema)

    # Create a Fluss TableDescriptor
    table_descriptor = fluss.TableDescriptor(fluss_schema)

    # Get the admin for Fluss
    admin = conn.get_admin()

    # Create a Fluss table
    table_path = fluss.TablePath("fluss", "sample_table_types")

    try:
        await admin.create_table(table_path, table_descriptor, True)
        print(f"Created table: {table_path}")
    except Exception as e:
        print(f"Table creation failed: {e}")

    # Get table information via admin
    try:
        table_info = await admin.get_table_info(table_path)
        print(f"Table info: {table_info}")
        print(f"Table ID: {table_info.table_id}")
        print(f"Schema ID: {table_info.schema_id}")
        print(f"Created time: {table_info.created_time}")
        print(f"Primary keys: {table_info.get_primary_keys()}")
    except Exception as e:
        print(f"Failed to get table info: {e}")

    # Demo: List offsets
    print("\n--- Testing list_offsets() ---")
    try:
        # Query latest offsets using OffsetSpec factory method
        offsets = await admin.list_offsets(
            table_path,
            bucket_ids=[0],
            offset_spec=fluss.OffsetSpec.latest()
        )
        print(f"Latest offsets for table (before writes): {offsets}")
    except Exception as e:
        print(f"Failed to list offsets: {e}")

    # Get the table instance
    table = await conn.get_table(table_path)
    print(f"Got table: {table}")

    # Create a writer for the table
    append_writer = table.new_append().create_writer()
    print(f"Created append writer: {append_writer}")

    try:
        # Demo: Write PyArrow Table
        print("\n--- Testing PyArrow Table write ---")
        pa_table = pa.Table.from_arrays(
            [
                pa.array([1, 2, 3], type=pa.int32()),
                pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
                pa.array([95.2, 87.2, 92.1], type=pa.float32()),
                pa.array([25, 30, 35], type=pa.int32()),
                pa.array(
                    [date(1999, 5, 15), date(1994, 3, 20), date(1989, 11, 8)],
                    type=pa.date32(),
                ),
                pa.array(
                    [dt_time(9, 0, 0), dt_time(9, 30, 0), dt_time(10, 0, 0)],
                    type=pa.time32("ms"),
                ),
                pa.array(
                    [
                        datetime(2024, 1, 15, 10, 30),
                        datetime(2024, 1, 15, 11, 0),
                        datetime(2024, 1, 15, 11, 30),
                    ],
                    type=pa.timestamp("us"),
                ),
                pa.array(
                    [
                        datetime(2024, 1, 15, 10, 30),
                        datetime(2024, 1, 15, 11, 0),
                        datetime(2024, 1, 15, 11, 30),
                    ],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                pa.array(
                    [Decimal("75000.00"), Decimal("82000.50"), Decimal("95000.75")],
                    type=pa.decimal128(10, 2),
                ),
            ],
            schema=schema,
        )

        append_writer.write_arrow(pa_table)
        print("Successfully wrote PyArrow Table")

        # Demo: Write PyArrow RecordBatch
        print("\n--- Testing PyArrow RecordBatch write ---")
        pa_record_batch = pa.RecordBatch.from_arrays(
            [
                pa.array([4, 5], type=pa.int32()),
                pa.array(["David", "Eve"], type=pa.string()),
                pa.array([88.5, 91.0], type=pa.float32()),
                pa.array([28, 32], type=pa.int32()),
                pa.array([date(1996, 7, 22), date(1992, 12, 1)], type=pa.date32()),
                pa.array([dt_time(14, 15, 0), dt_time(8, 45, 0)], type=pa.time32("ms")),
                pa.array(
                    [datetime(2024, 1, 16, 9, 0), datetime(2024, 1, 16, 9, 30)],
                    type=pa.timestamp("us"),
                ),
                pa.array(
                    [datetime(2024, 1, 16, 9, 0), datetime(2024, 1, 16, 9, 30)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                pa.array(
                    [Decimal("68000.00"), Decimal("72500.25")],
                    type=pa.decimal128(10, 2),
                ),
            ],
            schema=schema,
        )

        append_writer.write_arrow_batch(pa_record_batch)
        print("Successfully wrote PyArrow RecordBatch")

        # Test 3: Append single rows with Date, Time, Timestamp, Decimal
        print("\n--- Testing single row append with temporal/decimal types ---")
        # Dict input with all types including Date, Time, Timestamp, Decimal
        append_writer.append(
            {
                "id": 8,
                "name": "Helen",
                "score": 93.5,
                "age": 26,
                "birth_date": date(1998, 4, 10),
                "check_in_time": dt_time(11, 30, 45),
                "created_at": datetime(2024, 1, 17, 14, 0, 0),
                "updated_at": datetime(2024, 1, 17, 14, 0, 0),
                "salary": Decimal("88000.00"),
            }
        )
        print("Successfully appended row (dict with Date, Time, Timestamp, Decimal)")

        # List input with all types
        append_writer.append(
            [
                9,
                "Ivan",
                90.0,
                31,
                date(1993, 8, 25),
                dt_time(16, 45, 0),
                datetime(2024, 1, 17, 15, 30, 0),
                datetime(2024, 1, 17, 15, 30, 0),
                Decimal("91500.50"),
            ]
        )
        print("Successfully appended row (list with Date, Time, Timestamp, Decimal)")

        # Demo: Write Pandas DataFrame
        print("\n--- Testing Pandas DataFrame write ---")
        df = pd.DataFrame(
            {
                "id": [10, 11],
                "name": ["Frank", "Grace"],
                "score": [89.3, 94.7],
                "age": [29, 27],
                "birth_date": [date(1995, 2, 14), date(1997, 9, 30)],
                "check_in_time": [dt_time(10, 0, 0), dt_time(10, 30, 0)],
                "created_at": [
                    datetime(2024, 1, 18, 8, 0),
                    datetime(2024, 1, 18, 8, 30),
                ],
                "updated_at": [
                    datetime(2024, 1, 18, 8, 0),
                    datetime(2024, 1, 18, 8, 30),
                ],
                "salary": [Decimal("79000.00"), Decimal("85500.75")],
            }
        )

        append_writer.write_pandas(df)
        print("Successfully wrote Pandas DataFrame")

        # Flush all pending data
        print("\n--- Flushing data ---")
        await append_writer.flush()
        print("Successfully flushed data")

        # Demo: Check offsets after writes
        print("\n--- Checking offsets after writes ---")
        try:
            offsets = await admin.list_offsets(
                table_path,
                bucket_ids=[0],
                offset_spec=fluss.OffsetSpec.latest()
            )
            print(f"Latest offsets after writing 7 records: {offsets}")
        except Exception as e:
            print(f"Failed to list offsets: {e}")

    except Exception as e:
        print(f"Error during writing: {e}")

    # Now scan the table to verify data was written
    print("\n--- Scanning table (batch scanner) ---")
    try:
        # Use new_scan().create_record_batch_log_scanner() for batch-based operations
        batch_scanner = await table.new_scan().create_record_batch_log_scanner()
        print(f"Created batch scanner: {batch_scanner}")

        # Subscribe to buckets (required before to_arrow/to_pandas)
        # Use subscribe_buckets to subscribe all buckets from EARLIEST_OFFSET
        num_buckets = (await admin.get_table_info(table_path)).num_buckets
        batch_scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})
        print(f"Subscribed to {num_buckets} buckets from EARLIEST_OFFSET")

        # Read all data using to_arrow()
        print("Scanning results using to_arrow():")

        # Try to get as PyArrow Table
        try:
            pa_table_result = await batch_scanner.to_arrow()
            print(f"\nAs PyArrow Table: {pa_table_result}")
        except Exception as e:
            print(f"Could not convert to PyArrow: {e}")

        # Create a new batch scanner for to_pandas() test
        batch_scanner2 = await table.new_scan().create_record_batch_log_scanner()
        batch_scanner2.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

        # Try to get as Pandas DataFrame
        try:
            df_result = await batch_scanner2.to_pandas()
            print(f"\nAs Pandas DataFrame:\n{df_result}")
        except Exception as e:
            print(f"Could not convert to Pandas: {e}")

        # to_arrow_batch_reader() — returns a lazy PyArrow RecordBatchReader
        batch_scanner_reader = await table.new_scan().create_record_batch_log_scanner()
        batch_scanner_reader.subscribe_buckets(
            {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
        )
        arrow_reader = batch_scanner_reader.to_arrow_batch_reader()
        reader_table = pa.Table.from_batches(list(arrow_reader), schema=arrow_reader.schema)
        print(f"\nVia to_arrow_batch_reader(): {reader_table.num_rows} rows")

        # TODO: support to_duckdb()

        # Test poll_arrow() method for incremental reading as Arrow Table
        print("\n--- Testing poll_arrow() method ---")
        batch_scanner3 = await table.new_scan().create_record_batch_log_scanner()
        batch_scanner3.subscribe(bucket_id=0, start_offset=fluss.EARLIEST_OFFSET)
        print(f"Subscribed to bucket 0 at EARLIEST_OFFSET ({fluss.EARLIEST_OFFSET})")

        # Poll with a timeout of 5000ms (5 seconds)
        # Note: poll_arrow() returns an empty table (not an error) on timeout
        try:
            poll_result = await batch_scanner3.poll_arrow(5000)
            print(f"Number of rows: {poll_result.num_rows}")

            if poll_result.num_rows > 0:
                poll_df = poll_result.to_pandas()
                print(f"Polled data:\n{poll_df}")
            else:
                print("Empty result (no records available)")
                # Empty table still has schema - this is useful!
                print(f"Schema: {poll_result.schema}")

        except Exception as e:
            print(f"Error during poll_arrow: {e}")

        # Test poll_record_batch() method for batches with metadata
        print("\n--- Testing poll_record_batch() method ---")
        batch_scanner4 = await table.new_scan().create_record_batch_log_scanner()
        batch_scanner4.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

        try:
            batches = await batch_scanner4.poll_record_batch(5000)
            print(f"Number of batches: {len(batches)}")

            for i, batch in enumerate(batches):
                print(f"  Batch {i}: bucket={batch.bucket}, "
                      f"offsets={batch.base_offset}-{batch.last_offset}, "
                      f"rows={batch.batch.num_rows}")

        except Exception as e:
            print(f"Error during poll_record_batch: {e}")

    except Exception as e:
        print(f"Error during batch scanning: {e}")

    # Test record-based scanning with poll()
    print("\n--- Scanning table (record scanner) ---")
    try:
        # Use new_scan().create_log_scanner() for record-based operations
        record_scanner = await table.new_scan().create_log_scanner()
        print(f"Created record scanner: {record_scanner}")

        record_scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

        # Poll returns ScanRecords — records grouped by bucket
        print("\n--- Testing poll() method (record-by-record) ---")
        try:
            scan_records = await record_scanner.poll(5000)
            print(f"Total records: {scan_records.count()}, buckets: {len(scan_records.buckets())}")

            # Flat iteration over all records (regardless of bucket)
            print(f"  Flat iteration: {scan_records.count()} records")
            for record in scan_records:
                print(f"    offset={record.offset}, timestamp={record.timestamp}")

            # Per-bucket access
            for bucket in scan_records.buckets():
                bucket_recs = scan_records.records(bucket)
                print(f"  Bucket {bucket}: {len(bucket_recs)} records")
                for record in bucket_recs[:3]:
                    print(f"    offset={record.offset}, "
                          f"timestamp={record.timestamp}, "
                          f"change_type={record.change_type}, "
                          f"row={record.row}")

        except Exception as e:
            print(f"Error during poll: {e}")

    except Exception as e:
        print(f"Error during record scanning: {e}")

    # Demo: unsubscribe — unsubscribe from a bucket (non-partitioned tables)
    print("\n--- Testing unsubscribe ---")
    try:
        unsub_scanner = await table.new_scan().create_record_batch_log_scanner()
        unsub_scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})
        print(f"Subscribed to {num_buckets} buckets")
        # Unsubscribe from bucket 0 — future polls will skip this bucket
        unsub_scanner.unsubscribe(bucket_id=0)
        print("Unsubscribed from bucket 0")
        remaining = await unsub_scanner.poll_arrow(5000)
        print(f"After unsubscribe, got {remaining.num_rows} records (from remaining buckets)")
    except Exception as e:
        print(f"Error during unsubscribe test: {e}")

    # =====================================================
    # Demo: Primary Key Table with Lookup and Upsert
    # =====================================================
    print("\n" + "=" * 60)
    print("--- Testing Primary Key Table (Lookup & Upsert) ---")
    print("=" * 60)

    # Create a primary key table for lookup/upsert tests
    # Include temporal and decimal types to test full conversion
    pk_table_fields = [
        pa.field("user_id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("email", pa.string()),
        pa.field("age", pa.int32()),
        pa.field("birth_date", pa.date32()),
        pa.field("login_time", pa.time32("ms")),
        pa.field("created_at", pa.timestamp("us")),  # TIMESTAMP (NTZ)
        pa.field("updated_at", pa.timestamp("us", tz="UTC")),  # TIMESTAMP_LTZ
        pa.field("balance", pa.decimal128(10, 2)),
    ]
    pk_schema = pa.schema(pk_table_fields)
    fluss_pk_schema = fluss.Schema(pk_schema, primary_keys=["user_id"])

    # Create table descriptor
    pk_table_descriptor = fluss.TableDescriptor(
        fluss_pk_schema,
        bucket_count=3,
    )

    pk_table_path = fluss.TablePath("fluss", "users_pk_table_v3")

    try:
        await admin.create_table(pk_table_path, pk_table_descriptor, True)
        print(f"Created PK table: {pk_table_path}")
    except Exception as e:
        print(f"PK Table creation failed (may already exist): {e}")

    # Get the PK table
    pk_table = await conn.get_table(pk_table_path)
    print(f"Got PK table: {pk_table}")
    print(f"Has primary key: {pk_table.has_primary_key()}")

    # --- Test Upsert ---
    print("\n--- Testing Upsert (fire-and-forget) ---")
    try:
        upsert_writer = pk_table.new_upsert().create_writer()
        print(f"Created upsert writer: {upsert_writer}")

        # Fire-and-forget: queue writes synchronously, flush at end.
        # Records are batched internally for efficiency.
        upsert_writer.upsert(
            {
                "user_id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "age": 25,
                "birth_date": date(1999, 5, 15),
                "login_time": dt_time(9, 30, 45, 123000),  # 09:30:45.123
                "created_at": datetime(
                    2024, 1, 15, 10, 30, 45, 123456
                ),  # with microseconds
                "updated_at": datetime(2024, 1, 15, 10, 30, 45, 123456),
                "balance": Decimal("1234.56"),
            }
        )
        print("Queued user_id=1 (Alice)")

        upsert_writer.upsert(
            {
                "user_id": 2,
                "name": "Bob",
                "email": "bob@example.com",
                "age": 30,
                "birth_date": date(1994, 3, 20),
                "login_time": dt_time(14, 15, 30, 500000),  # 14:15:30.500
                "created_at": datetime(2024, 1, 16, 11, 22, 33, 444555),
                "updated_at": datetime(2024, 1, 16, 11, 22, 33, 444555),
                "balance": Decimal("5678.91"),
            }
        )
        print("Queued user_id=2 (Bob)")

        upsert_writer.upsert(
            {
                "user_id": 3,
                "name": "Charlie",
                "email": "charlie@example.com",
                "age": 35,
                "birth_date": date(1989, 11, 8),
                "login_time": dt_time(16, 45, 59, 999000),  # 16:45:59.999
                "created_at": datetime(2024, 1, 17, 23, 59, 59, 999999),
                "updated_at": datetime(2024, 1, 17, 23, 59, 59, 999999),
                "balance": Decimal("9876.54"),
            }
        )
        print("Queued user_id=3 (Charlie)")

        # flush() waits for all queued writes to be acknowledged by the server
        await upsert_writer.flush()
        print("Flushed — all 3 rows acknowledged by server")

        # Per-record acknowledgment: await the returned handle to block until
        # the server confirms this specific write, useful when you need to
        # read-after-write or verify critical updates.
        print("\n--- Testing Upsert (per-record acknowledgment) ---")
        handle = upsert_writer.upsert(
            {
                "user_id": 1,
                "name": "Alice Updated",
                "email": "alice.new@example.com",
                "age": 26,
                "birth_date": date(1999, 5, 15),
                "login_time": dt_time(10, 11, 12, 345000),  # 10:11:12.345
                "created_at": datetime(2024, 1, 15, 10, 30, 45, 123456),  # unchanged
                "updated_at": datetime(
                    2024, 1, 20, 15, 45, 30, 678901
                ),  # new update time
                "balance": Decimal("2345.67"),
            }
        )
        await handle.wait()  # wait for server acknowledgment
        print("Updated user_id=1 (Alice -> Alice Updated) — server acknowledged")

    except Exception as e:
        print(f"Error during upsert: {e}")
        traceback.print_exc()

    # --- Test Lookup ---
    print("\n--- Testing Lookup ---")
    try:
        lookuper = pk_table.new_lookup().create_lookuper()
        print(f"Created lookuper: {lookuper}")

        result = await lookuper.lookup({"user_id": 1})
        if result:
            print("Lookup user_id=1: Found!")
            print(f"  name: {result['name']}")
            print(f"  email: {result['email']}")
            print(f"  age: {result['age']}")
            print(
                f"  birth_date: {result['birth_date']} (type: {type(result['birth_date']).__name__})"
            )
            print(
                f"  login_time: {result['login_time']} (type: {type(result['login_time']).__name__})"
            )
            print(
                f"  created_at: {result['created_at']} (type: {type(result['created_at']).__name__})"
            )
            print(
                f"  updated_at: {result['updated_at']} (type: {type(result['updated_at']).__name__})"
            )
            print(
                f"  balance: {result['balance']} (type: {type(result['balance']).__name__})"
            )
        else:
            print("Lookup user_id=1: Not found")

        # Lookup another row
        result = await lookuper.lookup({"user_id": 2})
        if result:
            print(f"Lookup user_id=2: Found! -> {result}")
        else:
            print("Lookup user_id=2: Not found")

        # Lookup non-existent row
        result = await lookuper.lookup({"user_id": 999})
        if result:
            print(f"Lookup user_id=999: Found! -> {result}")
        else:
            print("Lookup user_id=999: Not found (as expected)")

    except Exception as e:
        print(f"Error during lookup: {e}")
        traceback.print_exc()

    # --- Test Delete ---
    print("\n--- Testing Delete ---")
    try:
        upsert_writer = pk_table.new_upsert().create_writer()

        handle = upsert_writer.delete({"user_id": 3})
        await handle.wait()
        print("Deleted user_id=3 — server acknowledged")

        lookuper = pk_table.new_lookup().create_lookuper()
        result = await lookuper.lookup({"user_id": 3})
        if result:
            print(f"Lookup user_id=3 after delete: Still found! -> {result}")
        else:
            print("Lookup user_id=3 after delete: Not found (deletion confirmed)")

    except Exception as e:
        print(f"Error during delete: {e}")
        traceback.print_exc()

    # --- Test Partial Update by column names ---
    print("\n--- Testing Partial Update (by column names) ---")
    try:
        partial_writer = pk_table.new_upsert().partial_update_by_name(["user_id", "balance"]).create_writer()
        handle = partial_writer.upsert({"user_id": 1, "balance": Decimal("9999.99")})
        await handle.wait()
        print("Partial update: set balance=9999.99 for user_id=1")

        lookuper = pk_table.new_lookup().create_lookuper()
        result = await lookuper.lookup({"user_id": 1})
        if result:
            print(f"Partial update verified:"
                  f"\n  name={result['name']} (unchanged)"
                  f"\n  balance={result['balance']} (updated)")
        else:
            print("ERROR: Expected to find user_id=1")

    except Exception as e:
        print(f"Error during partial update by names: {e}")
        traceback.print_exc()

    # --- Test Partial Update by column indices ---
    print("\n--- Testing Partial Update (by column indices) ---")
    try:
        # Columns: 0=user_id (PK), 1=name — update name only
        partial_writer_idx = pk_table.new_upsert().partial_update_by_index([0, 1]).create_writer()
        handle = partial_writer_idx.upsert([1, "Alice Renamed"])
        await handle.wait()
        print("Partial update by indices: set name='Alice Renamed' for user_id=1")

        lookuper = pk_table.new_lookup().create_lookuper()
        result = await lookuper.lookup({"user_id": 1})
        if result:
            print(f"Partial update by indices verified:"
                  f"\n  name={result['name']} (updated)"
                  f"\n  balance={result['balance']} (unchanged)")
        else:
            print("ERROR: Expected to find user_id=1")

    except Exception as e:
        print(f"Error during partial update by indices: {e}")
        traceback.print_exc()

    # Demo: Column projection using builder pattern
    print("\n--- Testing Column Projection ---")
    try:
        # Get bucket count for subscriptions
        num_buckets = (await admin.get_table_info(table_path)).num_buckets

        # Project specific columns by index (using batch scanner for to_pandas)
        print("\n1. Projection by index [0, 1] (id, name):")
        scanner_index = await table.new_scan().project([0, 1]).create_record_batch_log_scanner()
        scanner_index.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})
        df_projected = await scanner_index.to_pandas()
        print(df_projected.head())
        print(
            f"   Projected {df_projected.shape[1]} columns: {list(df_projected.columns)}"
        )

        # Project specific columns by name (Pythonic!)
        print("\n2. Projection by name ['name', 'score'] (Pythonic):")
        scanner_names = await table.new_scan() \
            .project_by_name(["name", "score"]) \
            .create_record_batch_log_scanner()
        scanner_names.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})
        df_named = await scanner_names.to_pandas()
        print(df_named.head())
        print(f"   Projected {df_named.shape[1]} columns: {list(df_named.columns)}")

        # Test empty result schema with projection
        print("\n3. Testing empty result schema with projection:")
        scanner_proj = await table.new_scan().project([0, 2]).create_record_batch_log_scanner()
        scanner_proj.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})
        # Quick poll that may return empty
        result = await scanner_proj.poll_arrow(100)
        print(f"   Schema columns: {result.schema.names}")

    except Exception as e:
        print(f"Error during projection: {e}")


    print("\n--- New: async context manager demo ---")
    async with await fluss.FlussConnection.create(config) as demo_conn:
        demo_table = await demo_conn.get_table(table_path)
        async with demo_table.new_append().create_writer() as writer:
            writer.append(
                {
                    "id": 1,
                    "name": "demo",
                    "score": 1.0,
                    "age": 25,
                    "birth_date": date(2000, 1, 1),
                    "check_in_time": dt_time(12, 0, 0),
                    "created_at": datetime(2024, 1, 1, 12, 0, 0),
                    "updated_at": datetime(2024, 1, 1, 12, 0, 0),
                    "salary": Decimal("100.00"),
                }
            )
            # auto-flushes on exit

    # Demo: Drop tables
    print("\n--- Testing drop_table() ---")
    try:
        # Drop the log table
        await admin.drop_table(table_path, ignore_if_not_exists=True)
        print(f"Successfully dropped table: {table_path}")
        # Drop the PK table
        await admin.drop_table(pk_table_path, ignore_if_not_exists=True)
        print(f"Successfully dropped table: {pk_table_path}")
    except Exception as e:
        print(f"Failed to drop table: {e}")

    # =====================================================
    # Demo: Partitioned Table with list_partition_offsets
    # =====================================================
    print("\n" + "=" * 60)
    print("--- Testing Partitioned Table ---")
    print("=" * 60)

    # Create a partitioned log table
    partitioned_fields = [
        pa.field("id", pa.int32()),
        pa.field("region", pa.string()),  # partition key
        pa.field("value", pa.int64()),
    ]
    partitioned_schema = pa.schema(partitioned_fields)
    fluss_partitioned_schema = fluss.Schema(partitioned_schema)

    partitioned_table_descriptor = fluss.TableDescriptor(
        fluss_partitioned_schema,
        partition_keys=["region"],  # Partition by region
        bucket_count=1,
    )

    partitioned_table_path = fluss.TablePath("fluss", "partitioned_log_table_py")

    try:
        # Drop if exists first
        await admin.drop_table(partitioned_table_path, ignore_if_not_exists=True)
        print(f"Dropped existing table: {partitioned_table_path}")

        # Create the partitioned table
        await admin.create_table(partitioned_table_path, partitioned_table_descriptor, False)
        print(f"Created partitioned table: {partitioned_table_path}")

        # Create partitions for US and EU regions
        print("\n--- Creating partitions ---")
        await admin.create_partition(partitioned_table_path, {"region": "US"}, ignore_if_exists=True)
        print("Created partition: region=US")
        await admin.create_partition(partitioned_table_path, {"region": "EU"}, ignore_if_exists=True)
        print("Created partition: region=EU")

        # List partitions
        print("\n--- Listing partitions ---")
        partition_infos = await admin.list_partition_infos(partitioned_table_path)
        for p in partition_infos:
            print(f"  {p}")  # PartitionInfo(partition_id=..., partition_name='region=...')

        # Get the table and write some data
        partitioned_table = await conn.get_table(partitioned_table_path)
        partitioned_writer = partitioned_table.new_append().create_writer()

        # Append data to US partition
        partitioned_writer.append({"id": 1, "region": "US", "value": 100})
        partitioned_writer.append({"id": 2, "region": "US", "value": 200})
        # Append data to EU partition
        partitioned_writer.append({"id": 3, "region": "EU", "value": 300})
        partitioned_writer.append({"id": 4, "region": "EU", "value": 400})
        await partitioned_writer.flush()
        print("\nWrote 4 records (2 to US, 2 to EU)")

        # Demo: list_partition_infos with partial spec filter
        print("\n--- Testing list_partition_infos with spec ---")
        us_partitions = await admin.list_partition_infos(
            partitioned_table_path, partition_spec={"region": "US"}
        )
        print(f"Filtered partitions (region=US): {us_partitions}")

        # Demo: list_partition_offsets
        print("\n--- Testing list_partition_offsets ---")

        # Query offsets for US partition
        # Note: partition_name is just the value (e.g., "US"), not "region=US"
        us_offsets = await admin.list_partition_offsets(
            partitioned_table_path,
            partition_name="US",
            bucket_ids=[0],
            offset_spec=fluss.OffsetSpec.latest()
        )
        print(f"US partition latest offsets: {us_offsets}")

        # Query offsets for EU partition
        eu_offsets = await admin.list_partition_offsets(
            partitioned_table_path,
            partition_name="EU",
            bucket_ids=[0],
            offset_spec=fluss.OffsetSpec.latest()
        )
        print(f"EU partition latest offsets: {eu_offsets}")

        # Demo: subscribe_partition for reading partitioned data
        print("\n--- Testing subscribe_partition + to_arrow() ---")
        partitioned_scanner = await partitioned_table.new_scan().create_record_batch_log_scanner()

        # Subscribe to each partition using partition_id
        for p in partition_infos:
            partitioned_scanner.subscribe_partition(
                partition_id=p.partition_id,
                bucket_id=0,
                start_offset=fluss.EARLIEST_OFFSET
            )
            print(f"Subscribed to partition {p.partition_name} (id={p.partition_id})")

        # Use to_arrow() - now works for partitioned tables!
        partitioned_arrow = await partitioned_scanner.to_arrow()
        print(f"\nto_arrow() returned {partitioned_arrow.num_rows} records from partitioned table:")
        print(partitioned_arrow.to_pandas())

        # Demo: subscribe_partition_buckets for batch subscribing to multiple partitions at once
        print("\n--- Testing subscribe_partition_buckets + to_arrow() ---")
        partitioned_scanner_batch = await partitioned_table.new_scan().create_record_batch_log_scanner()
        partition_bucket_offsets = {
            (p.partition_id, 0): fluss.EARLIEST_OFFSET for p in partition_infos
        }
        partitioned_scanner_batch.subscribe_partition_buckets(partition_bucket_offsets)
        print(f"Batch subscribed to {len(partition_bucket_offsets)} partition+bucket combinations")
        partitioned_batch_arrow = await partitioned_scanner_batch.to_arrow()
        print(f"to_arrow() returned {partitioned_batch_arrow.num_rows} records:")
        print(partitioned_batch_arrow.to_pandas())

        # Demo: unsubscribe_partition - unsubscribe from one partition, read remaining
        print("\n--- Testing unsubscribe_partition ---")
        partitioned_scanner3 = await partitioned_table.new_scan().create_record_batch_log_scanner()
        for p in partition_infos:
            partitioned_scanner3.subscribe_partition(p.partition_id, 0, fluss.EARLIEST_OFFSET)
        # Unsubscribe from the first partition
        first_partition = partition_infos[0]
        partitioned_scanner3.unsubscribe_partition(first_partition.partition_id, 0)
        print(f"Unsubscribed from partition {first_partition.partition_name} (id={first_partition.partition_id})")
        remaining_arrow = await partitioned_scanner3.to_arrow()
        print(f"After unsubscribe, to_arrow() returned {remaining_arrow.num_rows} records (from remaining partitions):")
        print(remaining_arrow.to_pandas())

        # Demo: to_pandas() also works for partitioned tables
        print("\n--- Testing to_pandas() on partitioned table ---")
        partitioned_scanner2 = await partitioned_table.new_scan().create_record_batch_log_scanner()
        for p in partition_infos:
            partitioned_scanner2.subscribe_partition(p.partition_id, 0, fluss.EARLIEST_OFFSET)
        partitioned_df = await partitioned_scanner2.to_pandas()
        print(f"to_pandas() returned {len(partitioned_df)} records:")
        print(partitioned_df)

        # Cleanup
        await admin.drop_table(partitioned_table_path, ignore_if_not_exists=True)
        print(f"\nDropped partitioned table: {partitioned_table_path}")

    except Exception as e:
        print(f"Error with partitioned table: {e}")
        traceback.print_exc()

    # =====================================================
    # Demo: Partitioned KV Table (Upsert, Lookup, Delete)
    # =====================================================
    print("\n" + "=" * 60)
    print("--- Testing Partitioned KV Table ---")
    print("=" * 60)

    partitioned_kv_fields = [
        pa.field("region", pa.string()),   # partition key + part of PK
        pa.field("user_id", pa.int32()),   # part of PK
        pa.field("name", pa.string()),
        pa.field("score", pa.int64()),
    ]
    partitioned_kv_schema = pa.schema(partitioned_kv_fields)
    fluss_partitioned_kv_schema = fluss.Schema(
        partitioned_kv_schema, primary_keys=["region", "user_id"]
    )

    partitioned_kv_descriptor = fluss.TableDescriptor(
        fluss_partitioned_kv_schema,
        partition_keys=["region"],
    )

    partitioned_kv_path = fluss.TablePath("fluss", "partitioned_kv_table_py")

    try:
        await admin.drop_table(partitioned_kv_path, ignore_if_not_exists=True)
        await admin.create_table(partitioned_kv_path, partitioned_kv_descriptor, False)
        print(f"Created partitioned KV table: {partitioned_kv_path}")

        # Create partitions
        await admin.create_partition(partitioned_kv_path, {"region": "US"})
        await admin.create_partition(partitioned_kv_path, {"region": "EU"})
        await admin.create_partition(partitioned_kv_path, {"region": "APAC"})
        print("Created partitions: US, EU, APAC")

        partitioned_kv_table = await conn.get_table(partitioned_kv_path)
        upsert_writer = partitioned_kv_table.new_upsert().create_writer()

        # Upsert rows across partitions
        test_data = [
            ("US", 1, "Gustave", 100),
            ("US", 2, "Lune", 200),
            ("EU", 1, "Sciel", 150),
            ("EU", 2, "Maelle", 250),
            ("APAC", 1, "Noco", 300),
        ]
        for region, user_id, name, score in test_data:
            upsert_writer.upsert({
                "region": region, "user_id": user_id,
                "name": name, "score": score,
            })
        await upsert_writer.flush()
        print(f"Upserted {len(test_data)} rows across 3 partitions")

        # Lookup all rows across partitions
        print("\n--- Lookup across partitions ---")
        lookuper = partitioned_kv_table.new_lookup().create_lookuper()
        for region, user_id, name, score in test_data:
            result = await lookuper.lookup({"region": region, "user_id": user_id})
            assert result is not None, f"Expected to find region={region} user_id={user_id}"
            assert result["name"] == name, f"Name mismatch: {result['name']} != {name}"
            assert result["score"] == score, f"Score mismatch: {result['score']} != {score}"
        print(f"All {len(test_data)} rows verified across partitions")

        # Update within a partition
        print("\n--- Update within partition ---")
        handle = upsert_writer.upsert({
            "region": "US", "user_id": 1,
            "name": "Gustave Updated", "score": 999,
        })
        await handle.wait()
        result = await lookuper.lookup({"region": "US", "user_id": 1})
        assert result is not None, "Expected to find region=US user_id=1 after update"
        assert result["name"] == "Gustave Updated"
        assert result["score"] == 999
        print(f"Update verified: US/1 name={result['name']} score={result['score']}")

        # Lookup in non-existent partition
        print("\n--- Lookup in non-existent partition ---")
        result = await lookuper.lookup({"region": "UNKNOWN", "user_id": 1})
        assert result is None, "Expected UNKNOWN partition lookup to return None"
        print("UNKNOWN partition lookup: not found (expected)")

        # Delete within a partition
        print("\n--- Delete within partition ---")
        handle = upsert_writer.delete({"region": "EU", "user_id": 1})
        await handle.wait()
        result = await lookuper.lookup({"region": "EU", "user_id": 1})
        assert result is None, "Expected EU/1 to be deleted"
        print("Delete verified: EU/1 not found")

        # Verify sibling record still exists
        result = await lookuper.lookup({"region": "EU", "user_id": 2})
        assert result is not None, "Expected EU/2 to still exist"
        assert result["name"] == "Maelle"
        print(f"EU/2 still exists: name={result['name']}")

        # Cleanup
        await admin.drop_table(partitioned_kv_path, ignore_if_not_exists=True)
        print(f"\nDropped partitioned KV table: {partitioned_kv_path}")

    except Exception as e:
        print(f"Error with partitioned KV table: {e}")
        traceback.print_exc()



    # Close connection
    await conn.close()
    print("\nConnection closed")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
