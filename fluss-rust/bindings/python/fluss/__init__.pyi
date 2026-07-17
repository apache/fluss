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

"""Type stubs for Fluss Python bindings."""

from enum import IntEnum
from types import TracebackType
from typing import (
    AsyncIterator,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    final,
    overload,
)

import pandas as pd
import pyarrow as pa

@final
class ChangeType(IntEnum):
    """Represents the type of change for a record in a log."""

    AppendOnly = 0
    """Append-only operation"""
    Insert = 1
    """Insert operation"""
    UpdateBefore = 2
    """Update operation containing the previous content of the updated row"""
    UpdateAfter = 3
    """Update operation containing the new content of the updated row"""
    Delete = 4
    """Delete operation"""

    def short_string(self) -> str:
        """Returns a short string representation (+A, +I, -U, +U, -D)."""
        ...

@final
class ScanRecord:
    """Represents a single scan record with metadata.

    The bucket is the key in ScanRecords, not on the individual record
    (matches Rust/Java).
    """

    @property
    def offset(self) -> int:
        """The position of this record in the log."""
        ...
    @property
    def timestamp(self) -> int:
        """The timestamp of this record."""
        ...
    @property
    def change_type(self) -> ChangeType:
        """The type of change (insert, update, delete, etc.)."""
        ...
    @property
    def row(self) -> Dict[str, object]:
        """The row data as a dictionary mapping column names to values."""
        ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

@final
class RecordBatch:
    """Represents a batch of records with metadata."""

    @property
    def batch(self) -> pa.RecordBatch:
        """The Arrow RecordBatch containing the data."""
        ...
    @property
    def bucket(self) -> TableBucket:
        """The bucket this batch belongs to."""
        ...
    @property
    def base_offset(self) -> int:
        """The offset of the first record in this batch."""
        ...
    @property
    def last_offset(self) -> int:
        """The offset of the last record in this batch."""
        ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

@final
class ScanRecords:
    """A collection of scan records grouped by bucket.

    Returned by ``LogScanner.poll()``. Supports flat iteration
    (``for rec in records``) and per-bucket access (``records.records(bucket)``).
    """

    def buckets(self) -> List[TableBucket]:
        """List of distinct buckets that have records."""
        ...
    def records(self, bucket: TableBucket) -> List[ScanRecord]:
        """Get records for a specific bucket. Returns empty list if bucket not present."""
        ...
    def count(self) -> int:
        """Total number of records across all buckets."""
        ...
    def is_empty(self) -> bool:
        """Whether the result set is empty."""
        ...
    def keys(self) -> List[TableBucket]:
        """Mapping protocol: alias for ``buckets()``."""
        ...
    def values(self) -> Iterator[List[ScanRecord]]:
        """Mapping protocol: lazy iterator over record lists, one per bucket."""
        ...
    def items(self) -> Iterator[Tuple[TableBucket, List[ScanRecord]]]:
        """Mapping protocol: lazy iterator over ``(bucket, records)`` pairs."""
        ...
    def __len__(self) -> int: ...
    @overload
    def __getitem__(self, index: int, /) -> ScanRecord: ...
    @overload
    def __getitem__(self, index: slice, /) -> List[ScanRecord]: ...
    @overload
    def __getitem__(self, bucket: TableBucket, /) -> List[ScanRecord]: ...
    def __contains__(self, bucket: TableBucket, /) -> bool: ...
    def __iter__(self) -> Iterator[ScanRecord]: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

@final
class Config:
    def __new__(cls, properties: Optional[Dict[str, str]] = None) -> Config: ...
    @property
    def bootstrap_servers(self) -> str: ...
    @bootstrap_servers.setter
    def bootstrap_servers(self, server: str) -> None: ...
    @property
    def writer_request_max_size(self) -> int: ...
    @writer_request_max_size.setter
    def writer_request_max_size(self, size: int) -> None: ...
    @property
    def writer_acks(self) -> str: ...
    @writer_acks.setter
    def writer_acks(self, acks: str) -> None: ...
    @property
    def writer_retries(self) -> int: ...
    @writer_retries.setter
    def writer_retries(self, retries: int) -> None: ...
    @property
    def writer_batch_size(self) -> int: ...
    @writer_batch_size.setter
    def writer_batch_size(self, size: int) -> None: ...
    @property
    def writer_dynamic_batch_size_enabled(self) -> bool: ...
    @writer_dynamic_batch_size_enabled.setter
    def writer_dynamic_batch_size_enabled(self, enabled: bool) -> None: ...
    @property
    def writer_dynamic_batch_size_min(self) -> int: ...
    @writer_dynamic_batch_size_min.setter
    def writer_dynamic_batch_size_min(self, size: int) -> None: ...
    @property
    def writer_bucket_no_key_assigner(self) -> str: ...
    @writer_bucket_no_key_assigner.setter
    def writer_bucket_no_key_assigner(self, value: str) -> None: ...
    @property
    def scanner_remote_log_prefetch_num(self) -> int: ...
    @scanner_remote_log_prefetch_num.setter
    def scanner_remote_log_prefetch_num(self, num: int) -> None: ...
    @property
    def remote_file_download_thread_num(self) -> int: ...
    @remote_file_download_thread_num.setter
    def remote_file_download_thread_num(self, num: int) -> None: ...
    @property
    def scanner_remote_log_read_concurrency(self) -> int: ...
    @scanner_remote_log_read_concurrency.setter
    def scanner_remote_log_read_concurrency(self, num: int) -> None: ...
    @property
    def scanner_log_max_poll_records(self) -> int: ...
    @scanner_log_max_poll_records.setter
    def scanner_log_max_poll_records(self, num: int) -> None: ...
    @property
    def scanner_log_fetch_max_bytes(self) -> int: ...
    @scanner_log_fetch_max_bytes.setter
    def scanner_log_fetch_max_bytes(self, bytes: int) -> None: ...
    @property
    def scanner_log_fetch_min_bytes(self) -> int: ...
    @scanner_log_fetch_min_bytes.setter
    def scanner_log_fetch_min_bytes(self, bytes: int) -> None: ...
    @property
    def scanner_log_fetch_wait_max_time_ms(self) -> int: ...
    @scanner_log_fetch_wait_max_time_ms.setter
    def scanner_log_fetch_wait_max_time_ms(self, ms: int) -> None: ...
    @property
    def scanner_log_fetch_max_bytes_for_bucket(self) -> int: ...
    @scanner_log_fetch_max_bytes_for_bucket.setter
    def scanner_log_fetch_max_bytes_for_bucket(self, bytes: int) -> None: ...
    @property
    def writer_batch_timeout_ms(self) -> int: ...
    @writer_batch_timeout_ms.setter
    def writer_batch_timeout_ms(self, timeout: int) -> None: ...
    @property
    def writer_enable_idempotence(self) -> bool: ...
    @writer_enable_idempotence.setter
    def writer_enable_idempotence(self, enabled: bool) -> None: ...
    @property
    def writer_max_inflight_requests_per_bucket(self) -> int: ...
    @writer_max_inflight_requests_per_bucket.setter
    def writer_max_inflight_requests_per_bucket(self, num: int) -> None: ...
    @property
    def writer_buffer_memory_size(self) -> int: ...
    @writer_buffer_memory_size.setter
    def writer_buffer_memory_size(self, size: int) -> None: ...
    @property
    def writer_buffer_wait_timeout_ms(self) -> int: ...
    @writer_buffer_wait_timeout_ms.setter
    def writer_buffer_wait_timeout_ms(self, timeout: int) -> None: ...
    @property
    def connect_timeout_ms(self) -> int: ...
    @connect_timeout_ms.setter
    def connect_timeout_ms(self, timeout: int) -> None: ...
    @property
    def security_protocol(self) -> str: ...
    @security_protocol.setter
    def security_protocol(self, protocol: str) -> None: ...
    @property
    def security_sasl_mechanism(self) -> str: ...
    @security_sasl_mechanism.setter
    def security_sasl_mechanism(self, mechanism: str) -> None: ...
    @property
    def security_sasl_username(self) -> str: ...
    @security_sasl_username.setter
    def security_sasl_username(self, username: str) -> None: ...
    @property
    def security_sasl_password(self) -> str: ...
    @security_sasl_password.setter
    def security_sasl_password(self, password: str) -> None: ...

@final
class FlussConnection:
    @staticmethod
    async def create(config: Config) -> FlussConnection: ...
    def get_admin(self) -> FlussAdmin: ...
    async def get_table(self, table_path: TablePath) -> FlussTable: ...
    async def close(self) -> None: ...
    def __enter__(self) -> FlussConnection: ...
    def __exit__(
        self,
        exc_type: Optional[type] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> bool: ...
    async def __aenter__(self) -> FlussConnection: ...
    async def __aexit__(
        self,
        exc_type: Optional[type] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> bool: ...
    def __repr__(self) -> str: ...

@final
class ServerNode:
    """Information about a server node in the Fluss cluster."""

    @property
    def id(self) -> int:
        """The server node ID."""
        ...
    @property
    def host(self) -> str:
        """The hostname of the server."""
        ...
    @property
    def port(self) -> int:
        """The port number of the server."""
        ...
    @property
    def server_type(self) -> str:
        """The type of server ('CoordinatorServer', 'TabletServer', or 'Unknown')."""
        ...
    @property
    def uid(self) -> str:
        """The unique identifier of the server (e.g. 'cs-0', 'ts-1')."""
        ...
    def __repr__(self) -> str: ...

@final
class FlussAdmin:
    async def create_database(
        self,
        database_name: str,
        database_descriptor: Optional["DatabaseDescriptor"] = None,
        ignore_if_exists: bool = False,
    ) -> None:
        """Create a database."""
        ...
    async def drop_database(
        self,
        database_name: str,
        ignore_if_not_exists: bool = False,
        cascade: bool = True,
    ) -> None:
        """Drop a database."""
        ...
    async def list_databases(self) -> List[str]:
        """List all databases."""
        ...
    async def database_exists(self, database_name: str) -> bool:
        """Check if a database exists."""
        ...
    async def get_database_info(self, database_name: str) -> "DatabaseInfo":
        """Get database information."""
        ...
    async def list_tables(self, database_name: str) -> List[str]:
        """List all tables in a database."""
        ...
    async def table_exists(self, table_path: TablePath) -> bool:
        """Check if a table exists."""
        ...
    async def drop_partition(
        self,
        table_path: TablePath,
        partition_spec: Dict[str, str],
        ignore_if_not_exists: bool = False,
    ) -> None:
        """Drop a partition from a partitioned table."""
        ...
    async def create_table(
        self,
        table_path: TablePath,
        table_descriptor: TableDescriptor,
        ignore_if_exists: Optional[bool] = None,
    ) -> None: ...
    async def get_table_info(self, table_path: TablePath) -> TableInfo: ...
    async def get_latest_lake_snapshot(self, table_path: TablePath) -> LakeSnapshot: ...
    async def drop_table(
        self,
        table_path: TablePath,
        ignore_if_not_exists: bool = False,
    ) -> None: ...
    async def list_offsets(
        self,
        table_path: TablePath,
        bucket_ids: List[int],
        offset_spec: "OffsetSpec",
    ) -> Dict[int, int]:
        """List offsets for the specified buckets.

        Args:
            table_path: Path to the table
            bucket_ids: List of bucket IDs to query
            offset_spec: Offset specification (OffsetSpec.earliest(), OffsetSpec.latest(),
                or OffsetSpec.timestamp(ts))

        Returns:
            Dict mapping bucket_id -> offset
        """
        ...
    async def list_partition_offsets(
        self,
        table_path: TablePath,
        partition_name: str,
        bucket_ids: List[int],
        offset_spec: "OffsetSpec",
    ) -> Dict[int, int]:
        """List offsets for buckets in a specific partition.

        Args:
            table_path: Path to the table
            partition_name: Partition value (e.g., "US" not "region=US")
            bucket_ids: List of bucket IDs to query
            offset_spec: Offset specification (OffsetSpec.earliest(), OffsetSpec.latest(),
                or OffsetSpec.timestamp(ts))

        Returns:
            Dict mapping bucket_id -> offset
        """
        ...
    async def create_partition(
        self,
        table_path: TablePath,
        partition_spec: Dict[str, str],
        ignore_if_exists: bool = False,
    ) -> None:
        """Create a partition for a partitioned table.

        Args:
            table_path: Path to the table
            partition_spec: Dict mapping partition column name to value (e.g., {"region": "US"})
            ignore_if_exists: If True, don't raise error if partition already exists
        """
        ...
    async def list_partition_infos(
        self,
        table_path: TablePath,
        partition_spec: Optional[Dict[str, str]] = None,
    ) -> List["PartitionInfo"]:
        """List partitions for a partitioned table.

        Args:
            table_path: Path to the table
            partition_spec: Optional partial partition spec to filter results.
                Dict mapping partition column name to value (e.g., {"region": "US"}).
                If None, returns all partitions.

        Returns:
            List of PartitionInfo objects
        """
        ...
    async def get_server_nodes(self) -> List[ServerNode]:
        """Get all alive server nodes in the cluster.

        Returns:
            List of ServerNode objects (coordinator and tablet servers)
        """
        ...
    def __repr__(self) -> str: ...


@final
class DatabaseDescriptor:
    """Descriptor for a Fluss database (comment and custom properties)."""

    def __new__(
        cls,
        comment: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> DatabaseDescriptor: ...
    @property
    def comment(self) -> Optional[str]: ...
    def get_custom_properties(self) -> Dict[str, str]: ...
    def __repr__(self) -> str: ...


@final
class DatabaseInfo:
    """Information about a Fluss database."""

    @property
    def database_name(self) -> str: ...
    def get_database_descriptor(self) -> DatabaseDescriptor: ...
    @property
    def created_time(self) -> int: ...
    @property
    def modified_time(self) -> int: ...
    def __repr__(self) -> str: ...

@final
class TableScan:
    """Builder for creating log scanners with flexible configuration.

    Use this builder to configure projection before creating a log scanner.
    Obtain a TableScan instance via `FlussTable.new_scan()`.

    Example:
        ```python
        # Record-based scanning with projection
        scanner = await table.new_scan() \\
            .project([0, 1, 2]) \\
            .create_log_scanner()

        # Batch-based scanning with column names
        scanner = await table.new_scan() \\
            .project_by_name(["id", "name"]) \\
            .create_record_batch_log_scanner()
        ```
    """

    def project(self, indices: List[int]) -> "TableScan":
        """Project to specific columns by their indices.

        Args:
            indices: List of column indices (0-based) to include in the scan.

        Returns:
            Self for method chaining.
        """
        ...
    def project_by_name(self, names: List[str]) -> "TableScan":
        """Project to specific columns by their names.

        Args:
            names: List of column names to include in the scan.

        Returns:
            Self for method chaining.
        """
        ...
    def with_fixed_schema(self, fixed_schema: bool) -> "TableScan":
        """Control whether schema-evolved batches are aligned to the scanner schema.

        When enabled, older-schema batches are padded/projected to the scanner
        creation schema. When disabled, batches keep their write-time schema.
        """
        ...
    def limit(self, n: int) -> "TableScan":
        """Set a positive row limit for the scan.

        A limit enables ``create_bucket_batch_scanner()`` for a one-shot
        bounded scan. The log scanners do not support limit pushdown and reject
        a configured limit.

        Args:
            n: The maximum number of rows to scan. Must be positive.

        Returns:
            Self for method chaining.
        """
        ...
    def create_bucket_batch_scanner(self, bucket: TableBucket) -> BatchScanner:
        """Create a one-shot bounded scanner over a single bucket.

        Requires a limit configured via ``limit()``. Creation is cheap; the
        scan RPC runs lazily on the first ``next_batch()``.

        Args:
            bucket: The bucket to scan. Its ``table_id`` must match this table
                and its ``bucket_id`` must be in range.

        Returns:
            BatchScanner for a single bounded scan of ``bucket``.
        """
        ...
    async def create_log_scanner(self) -> LogScanner:
        """Create a record-based log scanner.

        Use this scanner with `poll()` to get individual records with metadata
        (offset, timestamp, change_type).

        Works on log tables and on primary-key (KV) tables. For a primary-key
        table it subscribes to the CDC changelog: each record's ``change_type``
        is ``+I`` (insert), ``-U`` (update-before), ``+U`` (update-after) or
        ``-D`` (delete). A log table yields ``+A`` (append-only).

        Returns:
            LogScanner for record-by-record scanning with `poll()`
        """
        ...
    async def create_record_batch_log_scanner(self) -> LogScanner:
        """Create a batch-based log scanner.

        Use this scanner with `poll_arrow()` to get Arrow Tables, or with
        `poll_record_batch()` to get individual batches with metadata.

        Log tables only. Primary-key tables are rejected because the Arrow batch
        path carries no per-record change types; read a primary-key table's
        changelog with `create_log_scanner()` instead.

        Returns:
            LogScanner for batch-based scanning with `poll_arrow()` or `poll_record_batch()`
        """
        ...
    def __repr__(self) -> str: ...

@final
class FlussTable:
    def new_scan(self) -> TableScan:
        """Create a new table scan builder for configuring and creating log scanners.

        Use this method to create scanners with the builder pattern:

        Example:
            ```python
            # Record-based scanning
            scanner = await table.new_scan() \\
                .project([0, 1]) \\
                .create_log_scanner()

            # Batch-based scanning
            scanner = await table.new_scan() \\
                .project_by_name(["id", "name"]) \\
                .create_record_batch_log_scanner()
            ```

        Returns:
            TableScan builder for configuring the scanner.
        """
        ...
    def new_append(self) -> TableAppend: ...
    def new_upsert(self) -> TableUpsert: ...
    def new_lookup(self) -> TableLookup: ...
    def get_table_info(self) -> TableInfo: ...
    def get_table_path(self) -> TablePath: ...
    def has_primary_key(self) -> bool: ...
    def __repr__(self) -> str: ...

@final
class TableAppend:
    """Builder for creating an AppendWriter.

    Obtain via `FlussTable.new_append()`, then call `create_writer()`.

    Example:
        writer = table.new_append().create_writer()
    """

    def create_writer(self) -> AppendWriter: ...
    def __repr__(self) -> str: ...

@final
class TableUpsert:
    """Builder for creating an UpsertWriter, with optional partial update.

    Obtain via `FlussTable.new_upsert()`, then optionally call
    `partial_update_by_name()` or `partial_update_by_index()`,
    then call `create_writer()`.

    Example:
        # Full row upsert
        writer = table.new_upsert().create_writer()

        # Partial update by column names
        writer = table.new_upsert().partial_update_by_name(["col1", "col2"]).create_writer()

        # Partial update by column indices
        writer = table.new_upsert().partial_update_by_index([0, 1]).create_writer()
    """

    def partial_update_by_name(self, columns: List[str]) -> "TableUpsert": ...
    def partial_update_by_index(self, column_indices: List[int]) -> "TableUpsert": ...
    def create_writer(self) -> UpsertWriter: ...
    def __repr__(self) -> str: ...

@final
class TableLookup:
    """Builder for creating a Lookuper or PrefixLookuper.

    Obtain via `FlussTable.new_lookup()`, then call `create_lookuper()`
    for primary key lookup, or `lookup_by(columns).create_lookuper()`
    for prefix key lookup.

    Example:
        lookuper = table.new_lookup().create_lookuper()
        prefix_lookuper = table.new_lookup().lookup_by(["a", "b"]).create_lookuper()
    """

    def create_lookuper(self) -> Lookuper: ...
    def lookup_by(self, column_names: List[str]) -> "TablePrefixLookup":
        """Switch to prefix-scan mode for the given lookup columns.

        The columns must be the table's partition keys (if any) plus the
        bucket keys, in that order.

        Args:
            column_names: List of column names forming the prefix key.

        Returns:
            TablePrefixLookup builder. Call `create_lookuper()` to get a PrefixLookuper.
        """
        ...
    def __repr__(self) -> str: ...

@final
class TablePrefixLookup:
    """Builder for creating a PrefixLookuper.

    Obtain via `TableLookup.lookup_by(columns)`, then call `create_lookuper()`.

    Example:
        prefix_lookuper = table.new_lookup().lookup_by(["a", "b"]).create_lookuper()
    """

    def create_lookuper(self) -> "PrefixLookuper": ...
    def __repr__(self) -> str: ...

@final
class AppendWriter:
    def append(self, row: dict | list | tuple) -> WriteResultHandle:
        """Append a single row to the table.

        Args:
            row: Dictionary mapping field names to values, or
                 list/tuple of values in schema order

        Returns:
            WriteResultHandle: Ignore for fire-and-forget, or await handle.wait() for acknowledgement.

        Supported Types:
            - Boolean, TinyInt, SmallInt, Int, BigInt (integers)
            - Float, Double (floating point)
            - String, Char (text)
            - Bytes, Binary (binary data)
            - Date, Time, Timestamp, TimestampLTZ (temporal)
            - Decimal (arbitrary precision)
            - Array (Python list)
            - Map (dict, or list of (key, value) tuples)
            - Row (dict keyed by field name, or list/tuple by position)
            - Null values

        Nested combinations of Array, Map, and Row are supported. On read,
        Array -> list, Map -> list of (key, value) tuples, Row -> dict.

        When the row is a dict, a nullable column may be omitted (it defaults to
        null); a non-nullable or primary-key column must be present.

        Example:
            writer.append({'id': 1, 'name': 'Alice', 'score': 95.5})
            writer.append([1, 'Alice', 95.5])
            writer.append({'id': 2, 'tags': ['a', 'b'],
                           'attrs': {'k': 1}, 'profile': {'age': 30}})

        Note:
            For high-throughput bulk loading, prefer write_arrow_batch().
            Use flush() to ensure all queued records are sent and acknowledged.
        """
        ...
    def write_arrow(self, table: pa.Table) -> None: ...
    def write_arrow_batch(self, batch: pa.RecordBatch) -> WriteResultHandle: ...
    def write_pandas(self, df: pd.DataFrame) -> None: ...
    async def flush(self) -> None: ...
    async def __aenter__(self) -> AppendWriter:
        """
        Enter the async context manager.

        Returns:
            The AppendWriter instance.
        """
        ...
    async def __aexit__(
        self,
        exc_type: Optional[type] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> bool:
        """
        Exit the async context manager.

        On exit, the writer is automatically flushed to ensure
        all pending records are sent and acknowledged.
        """
        ...
    def __repr__(self) -> str: ...

@final
class UpsertWriter:
    """Writer for upserting and deleting data in a Fluss primary key table."""

    def upsert(self, row: dict | list | tuple) -> WriteResultHandle:
        """Upsert a row into the table.

        If a row with the same primary key exists, it will be updated.
        Otherwise, a new row will be inserted.

        Args:
            row: Dictionary mapping field names to values, or
                 list/tuple of values in schema order

        Returns:
            WriteResultHandle: Ignore for fire-and-forget, or await handle.wait() for ack.
        """
        ...
    def delete(self, pk: dict | list | tuple) -> WriteResultHandle:
        """Delete a row from the table by primary key.

        Args:
            pk: Dictionary with PK column names as keys, or
                list/tuple of PK values in PK column order

        Returns:
            WriteResultHandle: Ignore for fire-and-forget, or await handle.wait() for ack.
        """
        ...
    async def flush(self) -> None:
        """Flush all pending upsert/delete operations to the server."""
        ...
    async def __aenter__(self) -> UpsertWriter:
        """
        Enter the async context manager.

        Returns:
            The UpsertWriter instance.
        """
        ...
    async def __aexit__(
        self,
        exc_type: Optional[type] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> bool:
        """
        Exit the async context manager.

        On exit, the writer is automatically flushed to ensure
        all pending records are sent and acknowledged.
        """
        ...
    def __repr__(self) -> str: ...


@final
class WriteResultHandle:
    """Handle for a pending write (append/upsert/delete). Ignore for fire-and-forget, or await handle.wait() for ack."""

    async def wait(self) -> None:
        """Wait for server acknowledgment of this write."""
        ...
    def __repr__(self) -> str: ...


@final
class Lookuper:
    """Lookuper for performing primary key lookups on a Fluss table."""

    async def lookup(self, pk: dict | list | tuple) -> Optional[Dict[str, object]]:
        """Lookup a row by its primary key.

        Args:
            pk: Dictionary with PK column names as keys, or
                list/tuple of PK values in PK column order

        Returns:
            A dict containing the row data if found, None otherwise.
        """
        ...
    def __repr__(self) -> str: ...

@final
class PrefixLookuper:
    """Lookuper for performing prefix key lookups on a Fluss table.

    Returns all rows whose primary key starts with the given prefix.
    Create via `table.new_lookup().lookup_by(columns).create_lookuper()`.
    """

    async def lookup(self, prefix: dict | list | tuple) -> List[Dict[str, object]]:
        """Lookup all rows matching a prefix key.

        Args:
            prefix: A dict, list, or tuple containing only the prefix key values
                (the columns specified in lookup_by()).
                For dict: keys are prefix column names.
                For list/tuple: values in prefix column order.

        Returns:
            A list of dicts, each containing the full row data.
            Empty list if no matches.
        """
        ...
    def __repr__(self) -> str: ...

@final
class LogScanner:
    """Scanner for reading log data from a Fluss table.

    This scanner supports two modes:
    - Record-based scanning via `poll()` - returns individual records with metadata
    - Batch-based scanning via `poll_arrow()` / `poll_record_batch()` - returns Arrow batches

    Create scanners using the builder pattern:
        # Record-based scanning
        scanner = await table.new_scan().create_log_scanner()

        # Batch-based scanning
        scanner = await table.new_scan().create_record_batch_log_scanner()

        # With projection
        scanner = await table.new_scan().project([0, 1]).create_log_scanner()
    """

    def subscribe(self, bucket_id: int, start_offset: int) -> None:
        """Subscribe to a single bucket at a specific offset (non-partitioned tables).

        Args:
            bucket_id: The bucket ID to subscribe to
            start_offset: The offset to start reading from (use EARLIEST_OFFSET for beginning)
        """
        ...
    def subscribe_buckets(self, bucket_offsets: Dict[int, int]) -> None:
        """Subscribe to multiple buckets at specified offsets (non-partitioned tables).

        Args:
            bucket_offsets: Dict mapping bucket_id -> start_offset
        """
        ...
    def subscribe_partition(
        self, partition_id: int, bucket_id: int, start_offset: int
    ) -> None:
        """Subscribe to a bucket within a specific partition (partitioned tables only).

        Args:
            partition_id: The partition ID (from PartitionInfo.partition_id)
            bucket_id: The bucket ID within the partition
            start_offset: The offset to start reading from (use EARLIEST_OFFSET for beginning)
        """
        ...
    def subscribe_partition_buckets(
        self, partition_bucket_offsets: Dict[Tuple[int, int], int]
    ) -> None:
        """Subscribe to multiple partition+bucket combinations at once (partitioned tables only).

        Args:
            partition_bucket_offsets: Dict mapping (partition_id, bucket_id) tuples to start_offsets.
                Example: {(partition_id_1, 0): EARLIEST_OFFSET, (partition_id_2, 1): 100}
        """
        ...
    def unsubscribe(self, bucket_id: int) -> None:
        """Unsubscribe from a specific bucket (non-partitioned tables only).

        Args:
            bucket_id: The bucket ID to unsubscribe from
        """
        ...
    def unsubscribe_partition(self, partition_id: int, bucket_id: int) -> None:
        """Unsubscribe from a specific partition bucket (partitioned tables only).

        Args:
            partition_id: The partition ID to unsubscribe from
            bucket_id: The bucket ID within the partition
        """
        ...
    async def poll(self, timeout_ms: int) -> ScanRecords:
        """Poll for individual records with metadata.

        Requires a record-based scanner (created with new_scan().create_log_scanner()).

        Args:
            timeout_ms: Timeout in milliseconds to wait for records.

        Returns:
            ScanRecords grouped by bucket. Supports flat iteration
            (``for rec in records``) and per-bucket access
            (``records.buckets()``, ``records.records(bucket)``).

        Note:
            Returns an empty ScanRecords if no records are available or timeout expires.
        """
        ...
    async def poll_record_batch(self, timeout_ms: int) -> List[RecordBatch]:
        """Poll for batches with metadata.

        Requires a batch-based scanner (created with new_scan().create_record_batch_log_scanner()).

        Args:
            timeout_ms: Timeout in milliseconds to wait for batches.

        Returns:
            List of RecordBatch objects, each containing the Arrow batch along with
            bucket, base_offset, and last_offset metadata.

        Note:
            Returns an empty list if no batches are available or timeout expires.
        """
        ...
    async def poll_arrow(self, timeout_ms: int) -> pa.Table:
        """Poll for records as an Arrow Table.

        Requires a batch-based scanner (created with new_scan().create_record_batch_log_scanner()).

        Args:
            timeout_ms: Timeout in milliseconds to wait for records.

        Returns:
            PyArrow Table containing the polled records (batches merged).

        Note:
            Returns an empty table (with correct schema) if no records are available
            or timeout expires.
        """
        ...
    def to_arrow_batch_reader(self) -> pa.RecordBatchReader:
        """Create a lazy Arrow RecordBatchReader that reads until latest offsets.

        Returns a ``pyarrow.RecordBatchReader`` that lazily polls batches one at
        a time (streaming). Prefer this when you want to process batches without
        holding the full result in memory at once.

        Do not call ``poll_arrow`` / ``poll_record_batch`` on this scanner while
        iterating the reader; they share the same underlying scanner state.
        Overlapping calls are not supported. Use one active
        polling/consumption path at a time.

        Requires a batch-based scanner (created with ``new_scan().create_record_batch_log_scanner()``).
        You must call ``subscribe()``, ``subscribe_buckets()``, ``subscribe_partition()``,
        or ``subscribe_partition_buckets()`` first.

        Returns:
            ``pyarrow.RecordBatchReader`` yielding ``RecordBatch`` objects.
        """
        ...
    async def to_pandas(self) -> pd.DataFrame:
        """Convert all data to Pandas DataFrame.

        Requires a batch-based scanner (created with new_scan().create_record_batch_log_scanner()).
        Reads from currently subscribed buckets until reaching their latest offsets.

        You must call subscribe(), subscribe_buckets(), or subscribe_partition() first.
        """
        ...
    async def to_arrow(self) -> pa.Table:
        """Convert all data to Arrow Table.

        Batches are collected in Rust then combined into one table (no per-batch
        Python iteration). Do not interleave with ``poll_arrow`` / ``poll_record_batch``
        for the same subscription session; overlapping use is not supported.

        Requires a batch-based scanner (created with new_scan().create_record_batch_log_scanner()).
        Reads from currently subscribed buckets until reaching their latest offsets.

        You must call subscribe(), subscribe_buckets(), or subscribe_partition() first.
        """
        ...

    def __repr__(self) -> str: ...
    def __aiter__(self) -> AsyncIterator[Union[ScanRecord, RecordBatch]]: ...

@final
class BatchScanner:
    """One-shot bounded scanner over a single bucket.

    Obtain via ``table.new_scan().limit(n).create_bucket_batch_scanner(bucket)``.
    The scan runs lazily on the first ``next_batch()`` (or ``collect_all_batches()``
    / ``to_arrow()`` / ``to_pandas()``), yields its single batch once, then is
    spent. Honors the configured limit and any projection.

    Example:
        ```python
        table_id = table.get_table_info().table_id
        scanner = table.new_scan().limit(100).create_bucket_batch_scanner(
            fluss.TableBucket(table_id, 0)
        )
        table_data = await scanner.to_arrow()
        ```
    """

    @property
    def bucket(self) -> TableBucket:
        """The bucket scanned by this batch scanner."""
        ...
    async def next_batch(self) -> Optional[RecordBatch]:
        """Run the scan and return its batch, or ``None`` once the scanner is spent.

        The scan RPC runs on the first call; subsequent calls return ``None``.
        The scan is not retried — an error leaves the scanner spent, so create a
        new one to retry.

        Returns:
            A RecordBatch on the first call, then ``None``.
        """
        ...
    async def collect_all_batches(self) -> List[RecordBatch]:
        """Drain the scanner into all of its batches.

        Returns:
            List of RecordBatch objects (a single element for a limit scan).
        """
        ...
    async def to_arrow(self) -> pa.Table:
        """Drain the scanner into a single PyArrow Table.

        Returns:
            PyArrow Table with the scanned rows, or an empty table with the
            projected schema when the scan yields nothing.
        """
        ...
    async def to_pandas(self) -> pd.DataFrame:
        """Drain the scanner into a Pandas DataFrame."""
        ...
    def __repr__(self) -> str: ...

@final
class Schema:
    def __new__(
        cls, schema: pa.Schema, primary_keys: Optional[List[str]] = None
    ) -> Schema: ...
    def get_column_names(self) -> List[str]: ...
    def get_column_types(self) -> List[str]: ...
    def get_columns(self) -> List[Tuple[str, str]]: ...
    def get_primary_keys(self) -> List[str]: ...
    def __str__(self) -> str: ...

@final
class TableDescriptor:
    # Runtime accepts ``schema`` plus ``**kwargs``; the keyword-only parameters
    # below document the supported options for type checkers and IDEs.
    def __new__(
        cls,
        schema: Schema,
        *,
        partition_keys: Optional[List[str]] = None,
        bucket_count: Optional[int] = None,
        bucket_keys: Optional[List[str]] = None,
        comment: Optional[str] = None,
        log_format: Optional[str] = None,
        kv_format: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> TableDescriptor: ...
    def get_schema(self) -> Schema: ...

@final
class TablePath:
    def __new__(cls, database_name: str, table_name: str) -> TablePath: ...
    @property
    def database_name(self) -> str: ...
    @property
    def table_name(self) -> str: ...
    def table_path_str(self) -> str: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object, /) -> bool: ...

@final
class TableInfo:
    @property
    def table_id(self) -> int: ...
    @property
    def schema_id(self) -> int: ...
    @property
    def created_time(self) -> int: ...
    @property
    def modified_time(self) -> int: ...
    @property
    def table_path(self) -> TablePath: ...
    @property
    def num_buckets(self) -> int: ...
    @property
    def comment(self) -> Optional[str]: ...
    def get_primary_keys(self) -> List[str]: ...
    def get_bucket_keys(self) -> List[str]: ...
    def get_partition_keys(self) -> List[str]: ...
    def has_primary_key(self) -> bool: ...
    def is_partitioned(self) -> bool: ...
    def get_properties(self) -> Dict[str, str]: ...
    def get_custom_properties(self) -> Dict[str, str]: ...
    def get_schema(self) -> Schema: ...
    def get_column_names(self) -> List[str]: ...
    def get_column_count(self) -> int: ...

@final
class FlussError(Exception):
    message: str
    error_code: int
    def __new__(cls, message: str, error_code: int = -2) -> FlussError: ...
    def __str__(self) -> str: ...
    @property
    def is_retriable(self) -> bool: ...

@final
class LakeSnapshot:
    def __new__(cls, snapshot_id: int) -> LakeSnapshot: ...
    @property
    def snapshot_id(self) -> int: ...
    @property
    def table_buckets_offset(self) -> Dict[TableBucket, int]: ...
    def get_bucket_offset(self, bucket: TableBucket) -> Optional[int]: ...
    def get_table_buckets(self) -> List[TableBucket]: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

@final
class TableBucket:
    def __new__(cls, table_id: int, bucket: int) -> TableBucket: ...
    @staticmethod
    def with_partition(
        table_id: int, partition_id: int, bucket: int
    ) -> TableBucket: ...
    @property
    def table_id(self) -> int: ...
    @property
    def bucket_id(self) -> int: ...
    @property
    def partition_id(self) -> Optional[int]: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

@final
class PartitionInfo:
    """Information about a partition."""

    @property
    def partition_id(self) -> int:
        """Get the partition ID (globally unique in the cluster)."""
        ...
    @property
    def partition_name(self) -> str:
        """Get the partition name."""
        ...
    def __repr__(self) -> str: ...

@final
class ErrorCode:
    """Named constants for Fluss API error codes.

    Server API errors have error_code > 0 or == -1.
    Client-side errors have error_code == CLIENT_ERROR (-2).
    These constants are convenience names — new server error codes work
    automatically since error_code is a raw int, not a closed enum.
    """

    CLIENT_ERROR: int
    NONE: int
    UNKNOWN_SERVER_ERROR: int
    NETWORK_EXCEPTION: int
    UNSUPPORTED_VERSION: int
    CORRUPT_MESSAGE: int
    DATABASE_NOT_EXIST: int
    DATABASE_NOT_EMPTY: int
    DATABASE_ALREADY_EXIST: int
    TABLE_NOT_EXIST: int
    TABLE_ALREADY_EXIST: int
    SCHEMA_NOT_EXIST: int
    LOG_STORAGE_EXCEPTION: int
    KV_STORAGE_EXCEPTION: int
    NOT_LEADER_OR_FOLLOWER: int
    RECORD_TOO_LARGE_EXCEPTION: int
    CORRUPT_RECORD_EXCEPTION: int
    INVALID_TABLE_EXCEPTION: int
    INVALID_DATABASE_EXCEPTION: int
    INVALID_REPLICATION_FACTOR: int
    INVALID_REQUIRED_ACKS: int
    LOG_OFFSET_OUT_OF_RANGE_EXCEPTION: int
    NON_PRIMARY_KEY_TABLE_EXCEPTION: int
    UNKNOWN_TABLE_OR_BUCKET_EXCEPTION: int
    INVALID_UPDATE_VERSION_EXCEPTION: int
    INVALID_COORDINATOR_EXCEPTION: int
    FENCED_LEADER_EPOCH_EXCEPTION: int
    REQUEST_TIME_OUT: int
    STORAGE_EXCEPTION: int
    OPERATION_NOT_ATTEMPTED_EXCEPTION: int
    NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION: int
    NOT_ENOUGH_REPLICAS_EXCEPTION: int
    SECURITY_TOKEN_EXCEPTION: int
    OUT_OF_ORDER_SEQUENCE_EXCEPTION: int
    DUPLICATE_SEQUENCE_EXCEPTION: int
    UNKNOWN_WRITER_ID_EXCEPTION: int
    INVALID_COLUMN_PROJECTION: int
    INVALID_TARGET_COLUMN: int
    PARTITION_NOT_EXISTS: int
    TABLE_NOT_PARTITIONED_EXCEPTION: int
    INVALID_TIMESTAMP_EXCEPTION: int
    INVALID_CONFIG_EXCEPTION: int
    LAKE_STORAGE_NOT_CONFIGURED_EXCEPTION: int
    KV_SNAPSHOT_NOT_EXIST: int
    PARTITION_ALREADY_EXISTS: int
    PARTITION_SPEC_INVALID_EXCEPTION: int
    LEADER_NOT_AVAILABLE_EXCEPTION: int
    PARTITION_MAX_NUM_EXCEPTION: int
    AUTHENTICATE_EXCEPTION: int
    SECURITY_DISABLED_EXCEPTION: int
    AUTHORIZATION_EXCEPTION: int
    BUCKET_MAX_NUM_EXCEPTION: int
    FENCED_TIERING_EPOCH_EXCEPTION: int
    RETRIABLE_AUTHENTICATE_EXCEPTION: int
    INVALID_SERVER_RACK_INFO_EXCEPTION: int
    LAKE_SNAPSHOT_NOT_EXIST: int
    LAKE_TABLE_ALREADY_EXIST: int
    INELIGIBLE_REPLICA_EXCEPTION: int
    INVALID_ALTER_TABLE_EXCEPTION: int
    DELETION_DISABLED_EXCEPTION: int

@final
class OffsetSpec:
    """Offset specification for list_offsets(), matching Java's OffsetSpec.

    Use factory methods to create instances:
        OffsetSpec.earliest()
        OffsetSpec.latest()
        OffsetSpec.timestamp(ts)
    """

    @staticmethod
    def earliest() -> "OffsetSpec":
        """Create an OffsetSpec for the earliest available offset."""
        ...
    @staticmethod
    def latest() -> "OffsetSpec":
        """Create an OffsetSpec for the latest available offset."""
        ...
    @staticmethod
    def timestamp(ts: int) -> "OffsetSpec":
        """Create an OffsetSpec for the offset at or after the given timestamp."""
        ...
    def __repr__(self) -> str: ...

# Constant for earliest offset (-2)
EARLIEST_OFFSET: int

__version__: str
