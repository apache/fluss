/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#[cfg(test)]
mod table_test {
    use crate::integration::utils::{
        ColumnPlan, array_dt_basics_columns, as_row_type, create_partitions, create_table,
        dt_array_int, dt_map_string_int, dt_row_seq_label, extract_ids_from_batches,
        get_shared_cluster, make_int_array, make_string_array, map_dt_basics_columns,
        row_dt_basics_columns, scalar_dt_columns, wait_for_partitions_ready, wait_for_table_ready,
    };
    use arrow::array::record_batch;
    use fluss::client::{EARLIEST_OFFSET, FlussTable, TableScan};
    use fluss::metadata::{DataField, DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::record::ScanRecord;
    use fluss::row::binary_array::FlussArrayWriter;
    use fluss::row::binary_map::FlussMapWriter;
    use fluss::row::{
        DataGetters, Date, Datum, Decimal, GenericRow, InternalArray, InternalMap, Time,
        TimestampLtz, TimestampNtz,
    };
    use fluss::rpc::message::OffsetSpec;
    use std::collections::HashMap;
    use std::time::Duration;

    #[tokio::test]
    async fn append_record_batch_and_scan() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_append_record_batch_and_scan");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .distributed_by(Some(3), vec!["c1".to_string()])
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        let append_writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        let batch1 =
            record_batch!(("c1", Int32, [1, 2, 3]), ("c2", Utf8, ["a1", "a2", "a3"])).unwrap();
        append_writer
            .append_arrow_batch(batch1)
            .expect("Failed to append batch");

        let batch2 =
            record_batch!(("c1", Int32, [4, 5, 6]), ("c2", Utf8, ["a4", "a5", "a6"])).unwrap();
        append_writer
            .append_arrow_batch(batch2)
            .expect("Failed to append batch");

        // Flush to ensure all writes are acknowledged
        append_writer.flush().await.expect("Failed to flush");

        // Create scanner to verify appended records
        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let num_buckets = table.get_table_info().get_num_buckets();
        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        for bucket_id in 0..num_buckets {
            log_scanner
                .subscribe(bucket_id, EARLIEST_OFFSET)
                .await
                .expect("Failed to subscribe with EARLIEST_OFFSET");
        }

        // Poll for records across all buckets
        let mut collected: Vec<(i32, String)> = Vec::new();
        let start_time = std::time::Instant::now();
        while collected.len() < 6 && start_time.elapsed() < Duration::from_secs(10) {
            let scan_records = log_scanner
                .poll(Duration::from_millis(500))
                .await
                .expect("Failed to poll records");
            for rec in scan_records {
                let row = rec.row();
                collected.push((
                    row.get_int(0).unwrap(),
                    row.get_string(1).unwrap().to_string(),
                ));
            }
        }

        assert_eq!(collected.len(), 6, "Expected 6 records");

        // Sort and verify record contents
        collected.sort();
        let expected: Vec<(i32, String)> = vec![
            (1, "a1".to_string()),
            (2, "a2".to_string()),
            (3, "a3".to_string()),
            (4, "a4".to_string()),
            (5, "a5".to_string()),
            (6, "a6".to_string()),
        ];
        assert_eq!(collected, expected);

        // Test unsubscribe: unsubscribe from bucket 0, verify no error
        log_scanner
            .unsubscribe(0)
            .await
            .expect("Failed to unsubscribe from bucket 0");

        // Verify unsubscribe_partition fails on a non-partitioned table
        assert!(
            log_scanner.unsubscribe_partition(0, 0).await.is_err(),
            "unsubscribe_partition should fail on a non-partitioned table"
        );
    }

    #[tokio::test]
    async fn list_offsets() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_list_offsets");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        wait_for_table_ready(&admin, &table_path).await;

        // Test earliest offset (should be 0 for empty table)
        let earliest_offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Earliest)
            .await
            .expect("Failed to list earliest offsets");

        assert_eq!(
            earliest_offsets.get(&0),
            Some(&0),
            "Earliest offset should be 0 for bucket 0"
        );

        // Test latest offset (should be 0 for empty table)
        let latest_offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Latest)
            .await
            .expect("Failed to list latest offsets");

        assert_eq!(
            latest_offsets.get(&0),
            Some(&0),
            "Latest offset should be 0 for empty table"
        );

        // Append some records
        let append_writer = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table")
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        let batch = record_batch!(
            ("id", Int32, [1, 2, 3]),
            ("name", Utf8, ["alice", "bob", "charlie"])
        )
        .unwrap();
        append_writer
            .append_arrow_batch(batch)
            .expect("Failed to append batch");

        // Flush to ensure all writes are acknowledged
        append_writer.flush().await.expect("Failed to flush");

        // Test latest offset after appending (should be 3)
        let latest_offsets_after = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Latest)
            .await
            .expect("Failed to list latest offsets after append");

        assert_eq!(
            latest_offsets_after.get(&0),
            Some(&3),
            "Latest offset should be 3 after appending 3 records"
        );

        // Test earliest offset after appending (should still be 0)
        let earliest_offsets_after = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Earliest)
            .await
            .expect("Failed to list earliest offsets after append");

        assert_eq!(
            earliest_offsets_after.get(&0),
            Some(&0),
            "Earliest offset should still be 0"
        );

        // Scan records back to get server-assigned timestamps (avoids host/container
        // clock skew issues that make host-based timestamps unreliable).
        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe");

        let mut record_timestamps: Vec<i64> = Vec::new();
        let scan_start = std::time::Instant::now();
        while record_timestamps.len() < 3 && scan_start.elapsed() < Duration::from_secs(10) {
            let scan_records = log_scanner
                .poll(Duration::from_millis(500))
                .await
                .expect("Failed to poll records");
            for rec in scan_records {
                record_timestamps.push(rec.timestamp());
            }
        }
        assert_eq!(record_timestamps.len(), 3, "Expected 3 record timestamps");

        let min_ts = *record_timestamps.iter().min().unwrap();
        let max_ts = *record_timestamps.iter().max().unwrap();

        // Timestamp before all records should resolve to offset 0
        let before_offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Timestamp(min_ts - 1))
            .await
            .expect("Failed to list offsets by timestamp (before)");

        assert_eq!(
            before_offsets.get(&0),
            Some(&0),
            "Timestamp before first record should resolve to offset 0"
        );

        // Timestamp after all records should resolve to offset 3
        let after_offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Timestamp(max_ts + 1))
            .await
            .expect("Failed to list offsets by timestamp (after)");

        assert_eq!(
            after_offsets.get(&0),
            Some(&3),
            "Timestamp after last record should resolve to offset 3"
        );
    }

    #[tokio::test]
    async fn test_project() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_project");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("col_a", DataTypes::int())
                    .column("col_b", DataTypes::string())
                    .column("col_c", DataTypes::int())
                    .build()
                    .expect("Failed to build schema"),
            )
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        // Append 3 records
        let append_writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        let batch = record_batch!(
            ("col_a", Int32, [1, 2, 3]),
            ("col_b", Utf8, ["x", "y", "z"]),
            ("col_c", Int32, [10, 20, 30])
        )
        .unwrap();
        append_writer
            .append_arrow_batch(batch)
            .expect("Failed to append batch");
        append_writer.flush().await.expect("Failed to flush");

        // Test project_by_name: select col_b and col_c only
        let records = scan_table(&table, |scan| {
            scan.project_by_name(&["col_b", "col_c"])
                .expect("Failed to project by name")
        })
        .await;

        assert_eq!(
            records.len(),
            3,
            "Should have 3 records with project_by_name"
        );

        // Verify projected columns are in the correct order (col_b, col_c)
        let expected_col_b = ["x", "y", "z"];
        let expected_col_c = [10, 20, 30];

        for (i, record) in records.iter().enumerate() {
            let row = record.row();
            // col_b is now at index 0, col_c is at index 1
            assert_eq!(
                row.get_string(0).unwrap(),
                expected_col_b[i],
                "col_b mismatch at index {}",
                i
            );
            assert_eq!(
                row.get_int(1).unwrap(),
                expected_col_c[i],
                "col_c mismatch at index {}",
                i
            );
        }

        // test project by column indices
        let records = scan_table(&table, |scan| {
            scan.project(&[1, 0]).expect("Failed to project by indices")
        })
        .await;

        assert_eq!(
            records.len(),
            3,
            "Should have 3 records with project_by_name"
        );
        // Verify projected columns are in the correct order (col_b, col_a)
        let expected_col_b = ["x", "y", "z"];
        let expected_col_a = [1, 2, 3];

        for (i, record) in records.iter().enumerate() {
            let row = record.row();
            // col_b is now at index 0, col_c is at index 1
            assert_eq!(
                row.get_string(0).unwrap(),
                expected_col_b[i],
                "col_b mismatch at index {}",
                i
            );
            assert_eq!(
                row.get_int(1).unwrap(),
                expected_col_a[i],
                "col_c mismatch at index {}",
                i
            );
        }

        // Test error case: empty column names should fail
        let result = table.new_scan().project_by_name(&[]);
        assert!(
            result.is_err(),
            "project_by_name with empty names should fail"
        );

        // Test error case: non-existent column should fail
        let result = table.new_scan().project_by_name(&["nonexistent_column"]);
        assert!(
            result.is_err(),
            "project_by_name with non-existent column should fail"
        );
    }

    async fn scan_table<'a>(
        table: &FlussTable<'a>,
        setup_scan: impl FnOnce(TableScan) -> TableScan,
    ) -> Vec<ScanRecord> {
        // 1. build log scanner
        let log_scanner = setup_scan(table.new_scan())
            .create_log_scanner()
            .expect("Failed to create log scanner");

        // 2. subscribe
        let mut bucket_offsets = HashMap::new();
        bucket_offsets.insert(0, 0);
        log_scanner
            .subscribe_buckets(&bucket_offsets)
            .await
            .expect("Failed to subscribe");

        // 3. poll records
        let scan_records = log_scanner
            .poll(Duration::from_secs(10))
            .await
            .expect("Failed to poll");

        // 4. collect and sort
        let mut records: Vec<_> = scan_records.into_iter().collect();
        records.sort_by_key(|r| r.offset());
        records
    }

    #[tokio::test]
    async fn test_poll_batches() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_poll_batches");
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .build()
            .unwrap();

        create_table(
            &admin,
            &table_path,
            &TableDescriptor::builder().schema(schema).build().unwrap(),
        )
        .await;
        wait_for_table_ready(&admin, &table_path).await;

        let table = connection.get_table(&table_path).await.unwrap();
        let scanner = table.new_scan().create_record_batch_log_scanner().unwrap();
        scanner.subscribe(0, 0).await.unwrap();

        // Test 1: Empty table should return empty result
        assert!(
            scanner
                .poll(Duration::from_millis(500))
                .await
                .unwrap()
                .is_empty()
        );

        let writer = table.new_append().unwrap().create_writer().unwrap();
        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [1, 2]), ("name", Utf8, ["a", "b"])).unwrap(),
            )
            .unwrap();
        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [3, 4]), ("name", Utf8, ["c", "d"])).unwrap(),
            )
            .unwrap();
        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [5, 6]), ("name", Utf8, ["e", "f"])).unwrap(),
            )
            .unwrap();
        writer.flush().await.unwrap();

        // poll may return partial results if not all batches are available yet,
        // so we accumulate across multiple polls until we have the expected count.
        let mut all_ids = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while all_ids.len() < 6 && tokio::time::Instant::now() < deadline {
            let batches = scanner.poll(Duration::from_secs(5)).await.unwrap();
            all_ids.extend(extract_ids_from_batches(&batches));
        }

        // Test 2: Order should be preserved across multiple batches
        assert_eq!(all_ids, vec![1, 2, 3, 4, 5, 6]);

        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [7, 8]), ("name", Utf8, ["g", "h"])).unwrap(),
            )
            .unwrap();
        writer.flush().await.unwrap();

        let mut new_ids = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while new_ids.len() < 2 && tokio::time::Instant::now() < deadline {
            let more = scanner.poll(Duration::from_secs(5)).await.unwrap();
            new_ids.extend(extract_ids_from_batches(&more));
        }

        // Test 3: Subsequent polls should not return duplicate data (offset continuation)
        assert_eq!(new_ids, vec![7, 8]);

        // Test 4: Subscribing from mid-offset should truncate batch (Arrow batch slicing)
        // Server returns all records from start of batch, but client truncates to subscription offset
        let trunc_scanner = table.new_scan().create_record_batch_log_scanner().unwrap();
        trunc_scanner.subscribe(0, 3).await.unwrap();
        let mut trunc_ids = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while trunc_ids.len() < 5 && tokio::time::Instant::now() < deadline {
            let trunc_batches = trunc_scanner.poll(Duration::from_secs(5)).await.unwrap();
            trunc_ids.extend(extract_ids_from_batches(&trunc_batches));
        }

        // Subscribing from offset 3 should return [4,5,6,7,8], not [1,2,3,4,5,6,7,8]
        assert_eq!(trunc_ids, vec![4, 5, 6, 7, 8]);

        // Test 5: Projection should only return requested columns
        let proj = table
            .new_scan()
            .project_by_name(&["id"])
            .unwrap()
            .create_record_batch_log_scanner()
            .unwrap();
        proj.subscribe(0, 0).await.unwrap();
        let proj_batches = proj.poll(Duration::from_secs(10)).await.unwrap();

        // Projected batch should have 1 column (id), not 2 (id, name)
        assert_eq!(proj_batches[0].batch().num_columns(), 1);
    }

    /// Integration test covering produce and scan operations for all supported datatypes
    /// in log tables.
    #[tokio::test]
    async fn partitioned_table_append_scan() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_partitioned_log_append");

        // Create a partitioned log table
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("region", DataTypes::string())
                    .column("value", DataTypes::bigint())
                    .build()
                    .expect("Failed to build schema"),
            )
            .partitioned_by(vec!["region"])
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        // Create partitions
        create_partitions(&admin, &table_path, "region", &["US", "EU"]).await;

        // Wait for partition bucket leaders to be available.
        wait_for_partitions_ready(&admin, &table_path, &["US", "EU"]).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        // Create append writer - this should now work for partitioned tables
        let append_writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        // Append records with different partitions
        let test_data = [
            (1, "US", 100i64),
            (2, "US", 200i64),
            (3, "EU", 300i64),
            (4, "EU", 400i64),
        ];

        for (id, region, value) in &test_data {
            let mut row = GenericRow::new(3);
            row.set_field(0, *id);
            row.set_field(1, *region);
            row.set_field(2, *value);
            append_writer.append(&row).expect("Failed to append row");
        }

        append_writer.flush().await.expect("Failed to flush");

        // Test append_arrow_batch for partitioned tables
        // Each batch must contain rows from the same partition
        let us_batch = record_batch!(
            ("id", Int32, [5, 6]),
            ("region", Utf8, ["US", "US"]),
            ("value", Int64, [500, 600])
        )
        .unwrap();
        append_writer
            .append_arrow_batch(us_batch)
            .expect("Failed to append US batch");

        let eu_batch = record_batch!(
            ("id", Int32, [7, 8]),
            ("region", Utf8, ["EU", "EU"]),
            ("value", Int64, [700, 800])
        )
        .unwrap();
        append_writer
            .append_arrow_batch(eu_batch)
            .expect("Failed to append EU batch");

        append_writer
            .flush()
            .await
            .expect("Failed to flush batches");

        // Test list_offsets_for_partition
        // US partition has 4 records: 2 from row append + 2 from batch append
        let us_offsets = admin
            .list_partition_offsets(&table_path, "US", &[0], OffsetSpec::Latest)
            .await
            .expect("Failed to list offsets for US partition");
        assert_eq!(
            us_offsets.get(&0),
            Some(&4),
            "US partition should have 4 records"
        );

        // EU partition has 4 records: 2 from row append + 2 from batch append
        let eu_offsets = admin
            .list_partition_offsets(&table_path, "EU", &[0], OffsetSpec::Latest)
            .await
            .expect("Failed to list offsets for EU partition");
        assert_eq!(
            eu_offsets.get(&0),
            Some(&4),
            "EU partition should have 4 records"
        );

        // test list a not exist partition should return error
        let result = admin
            .list_partition_offsets(&table_path, "NOT Exists", &[0], OffsetSpec::Latest)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "Table partition 'fluss.test_partitioned_log_append(p=NOT Exists)' does not exist."
        ));

        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        let partition_info = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partition infos");
        for partition_info in partition_info {
            log_scanner
                .subscribe_partition(partition_info.get_partition_id(), 0, 0)
                .await
                .expect("Failed to subscribe to partition");
        }

        let expected_records = vec![
            (1, "US", 100i64),
            (2, "US", 200i64),
            (3, "EU", 300i64),
            (4, "EU", 400),
            (5, "US", 500i64),
            (6, "US", 600i64),
            (7, "EU", 700i64),
            (8, "EU", 800i64),
        ];
        let expected_records: Vec<(i32, String, i64)> = expected_records
            .into_iter()
            .map(|(id, region, val)| (id, region.to_string(), val))
            .collect();

        let mut collected_records: Vec<(i32, String, i64)> = Vec::new();
        let start_time = std::time::Instant::now();
        while collected_records.len() < expected_records.len()
            && start_time.elapsed() < Duration::from_secs(10)
        {
            let records = log_scanner
                .poll(Duration::from_millis(500))
                .await
                .expect("Failed to poll log scanner");
            for rec in records {
                let row = rec.row();
                collected_records.push((
                    row.get_int(0).unwrap(),
                    row.get_string(1).unwrap().to_string(),
                    row.get_long(2).unwrap(),
                ));
            }
        }

        assert_eq!(
            collected_records.len(),
            expected_records.len(),
            "Did not receive all records in time, expect receive {} records, but got {} records",
            expected_records.len(),
            collected_records.len()
        );
        collected_records.sort_by_key(|r| r.0);
        assert_eq!(
            collected_records, expected_records,
            "Data mismatch between sent and received"
        );

        // Test unsubscribe_partition: after unsubscribing from one partition,
        // data from that partition should no longer be read.
        let log_scanner_unsub = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner for unsubscribe test");
        let partition_infos = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partition infos");
        let eu_partition_id = partition_infos
            .iter()
            .find(|p| p.get_partition_name() == "EU")
            .map(|p| p.get_partition_id())
            .expect("EU partition should exist");
        for info in &partition_infos {
            log_scanner_unsub
                .subscribe_partition(info.get_partition_id(), 0, 0)
                .await
                .expect("Failed to subscribe to partition");
        }
        log_scanner_unsub
            .unsubscribe_partition(eu_partition_id, 0)
            .await
            .expect("Failed to unsubscribe from EU partition");

        let mut records_after_unsubscribe: Vec<(i32, String, i64)> = Vec::new();
        let unsub_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while records_after_unsubscribe.len() < 4 && std::time::Instant::now() < unsub_deadline {
            let records = log_scanner_unsub
                .poll(Duration::from_millis(300))
                .await
                .expect("Failed to poll after unsubscribe");
            for rec in records {
                let row = rec.row();
                records_after_unsubscribe.push((
                    row.get_int(0).unwrap(),
                    row.get_string(1).unwrap().to_string(),
                    row.get_long(2).unwrap(),
                ));
            }
        }

        assert!(
            records_after_unsubscribe.iter().all(|r| r.1 == "US"),
            "After unsubscribe_partition(EU), only US partition data should be read; got regions: {:?}",
            records_after_unsubscribe
                .iter()
                .map(|r| r.1.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            records_after_unsubscribe.len(),
            4,
            "Should receive exactly 4 US records (ids 1,2,5,6); got {}",
            records_after_unsubscribe.len()
        );

        // Test subscribe_partition_buckets: batch subscribe to all partitions at once
        let log_scanner_batch = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner for batch partition subscribe test");
        let partition_infos = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partition infos");
        let partition_bucket_offsets: HashMap<(i64, i32), i64> = partition_infos
            .iter()
            .map(|p| ((p.get_partition_id(), 0), 0i64))
            .collect();
        log_scanner_batch
            .subscribe_partition_buckets(&partition_bucket_offsets)
            .await
            .expect("Failed to batch subscribe to partitions");

        let mut batch_collected: Vec<(i32, String, i64)> = Vec::new();
        let batch_start = std::time::Instant::now();
        while batch_collected.len() < expected_records.len()
            && batch_start.elapsed() < Duration::from_secs(10)
        {
            let records = log_scanner_batch
                .poll(Duration::from_millis(500))
                .await
                .expect("Failed to poll after batch partition subscribe");
            for rec in records {
                let row = rec.row();
                batch_collected.push((
                    row.get_int(0).unwrap(),
                    row.get_string(1).unwrap().to_string(),
                    row.get_long(2).unwrap(),
                ));
            }
        }
        assert_eq!(
            batch_collected.len(),
            expected_records.len(),
            "Did not receive all records in time, expect receive {} records, but got {} records",
            expected_records.len(),
            batch_collected.len()
        );
        batch_collected.sort_by_key(|r| r.0);
        assert_eq!(
            batch_collected, expected_records,
            "subscribe_partition_buckets should receive the same records as subscribe_partition loop"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Projection over a log table containing every compound type.
    #[tokio::test]
    async fn projection_with_compound_types() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_log_projection_compound");

        let row_type = DataTypes::row(vec![
            DataField::new("seq", DataTypes::int(), None),
            DataField::new("label", DataTypes::string(), None),
        ]);

        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("nested", row_type)
            .column(
                "attrs",
                DataTypes::map(DataTypes::string(), DataTypes::int()),
            )
            .column("tags", DataTypes::array(DataTypes::string()))
            .column("extra", DataTypes::string())
            .build()
            .expect("schema");

        create_table(
            &admin,
            &table_path,
            &TableDescriptor::builder()
                .schema(schema)
                .build()
                .expect("table descriptor"),
        )
        .await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        let mut nested = GenericRow::new(2);
        nested.set_field(0, 42_i32);
        nested.set_field(1, "hello");
        let attrs = {
            let mut w = FlussMapWriter::new(2, &DataTypes::string(), &DataTypes::int());
            w.write_entry("x".into(), 1.into()).unwrap();
            w.write_entry("y".into(), 2.into()).unwrap();
            w.complete().expect("attrs")
        };
        let tags = make_string_array(&[Some("alpha"), Some("beta")]);

        let mut row = GenericRow::new(5);
        row.set_field(0, 7_i32);
        row.set_field(1, Datum::Row(Box::new(nested)));
        row.set_field(2, Datum::Map(attrs));
        row.set_field(3, tags);
        row.set_field(4, "ignore-me");
        writer.append(&row).expect("append");
        writer.flush().await.expect("flush");

        // Project columns in reordered form, dropping `extra`.
        let records = scan_table(&table, |scan| {
            scan.project_by_name(&["nested", "attrs", "tags", "id"])
                .expect("project failed")
        })
        .await;
        assert_eq!(records.len(), 1);
        let r = records[0].row();

        // === Projection: ROW ===
        let projected_nested = r.get_row(0).expect("get_row over projection");
        assert_eq!(projected_nested.get_int(0).unwrap(), 42);
        assert_eq!(projected_nested.get_string(1).unwrap(), "hello");

        // === Projection: MAP ===
        let m = r
            .get_map(1)
            .expect("get_map over projection")
            .expect_binary();
        assert_eq!(m.size(), 2);
        assert_eq!(m.get(&Datum::from("x")).unwrap(), Some(Datum::from(1_i32)));
        assert_eq!(m.get(&Datum::from("y")).unwrap(), Some(Datum::from(2_i32)));

        // === Projection: ARRAY ===
        let a = r.get_array(2).expect("get_array over projection");
        assert_eq!(a.size(), 2);
        assert_eq!(a.get_string(0).unwrap(), "alpha");
        assert_eq!(a.get_string(1).unwrap(), "beta");

        // === Projection: scalar reordered to position 3 ===
        assert_eq!(r.get_int(3).unwrap(), 7);

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    /// Log append + scan against a schema covering every supported data type.
    #[tokio::test]
    async fn all_supported_datatypes() {
        fn assert_f32_special(actual: f32, expected: f32) {
            if expected.is_nan() {
                assert!(actual.is_nan(), "expected NaN");
            } else if expected.is_infinite() {
                assert!(actual.is_infinite());
                assert_eq!(actual.signum(), expected.signum());
            } else {
                assert!((actual - expected).abs() < f32::EPSILON);
            }
        }
        fn assert_f64_special(actual: f64, expected: f64) {
            if expected.is_nan() {
                assert!(actual.is_nan(), "expected NaN");
            } else if expected.is_infinite() {
                assert!(actual.is_infinite());
                assert_eq!(actual.signum(), expected.signum());
            } else {
                assert!((actual - expected).abs() < f64::EPSILON);
            }
        }

        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_log_complex_types");

        let row_seq_label_owned = dt_row_seq_label();
        let row_seq_label = as_row_type(&row_seq_label_owned);
        let inner_array_int = dt_array_int();
        let inner_map_string_int = dt_map_string_int();

        let plan = ColumnPlan::new()
            .add("id", DataTypes::int())
            .start_section("array_basics")
            .extend(array_dt_basics_columns())
            .start_section("row_basics")
            .extend(row_dt_basics_columns())
            .start_section("map_basics")
            .extend(map_dt_basics_columns())
            // ARRAY rich types
            .start_section("array_rich")
            .add("arr_bytes", DataTypes::array(DataTypes::bytes()))
            .add("arr_date", DataTypes::array(DataTypes::date()))
            .add(
                "arr_time",
                DataTypes::array(DataTypes::time_with_precision(3)),
            )
            .add(
                "arr_ts",
                DataTypes::array(DataTypes::timestamp_with_precision(6)),
            )
            .add(
                "arr_ts_ltz",
                DataTypes::array(DataTypes::timestamp_ltz_with_precision(3)),
            )
            .add("arr_decimal", DataTypes::array(DataTypes::decimal(10, 2)))
            .add(
                "arr_decimal_big",
                DataTypes::array(DataTypes::decimal(22, 5)),
            )
            .add("arr_float", DataTypes::array(DataTypes::float()))
            .add("arr_double", DataTypes::array(DataTypes::double()))
            .add("arr_binary", DataTypes::array(DataTypes::binary(4)))
            // MAP rich types
            .start_section("map_rich")
            .add(
                "map_bytes",
                DataTypes::map(DataTypes::string(), DataTypes::bytes()),
            )
            .add(
                "map_decimal",
                DataTypes::map(DataTypes::string(), DataTypes::decimal(10, 2)),
            )
            .add(
                "map_date",
                DataTypes::map(DataTypes::string(), DataTypes::date()),
            )
            .add(
                "map_time",
                DataTypes::map(DataTypes::string(), DataTypes::time_with_precision(3)),
            )
            .add(
                "map_ts",
                DataTypes::map(DataTypes::string(), DataTypes::timestamp_with_precision(6)),
            )
            .add(
                "map_ts_ltz",
                DataTypes::map(
                    DataTypes::string(),
                    DataTypes::timestamp_ltz_with_precision(3),
                ),
            )
            .add(
                "map_float",
                DataTypes::map(DataTypes::string(), DataTypes::float()),
            )
            .add(
                "map_double",
                DataTypes::map(DataTypes::string(), DataTypes::double()),
            )
            .add(
                "map_bool",
                DataTypes::map(DataTypes::string(), DataTypes::boolean()),
            )
            .add(
                "map_binary",
                DataTypes::map(DataTypes::string(), DataTypes::binary(4)),
            )
            .add(
                "map_int_key",
                DataTypes::map(DataTypes::int(), DataTypes::string()),
            )
            .start_section("scalars")
            .extend(scalar_dt_columns());
        let column_count = plan.len();

        create_table(
            &admin,
            &table_path,
            &TableDescriptor::builder()
                .schema(plan.build_schema(None))
                .build()
                .expect("table descriptor"),
        )
        .await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        // Shared scalar values
        let dec = Decimal::from_unscaled_long(12345, 10, 2).unwrap();
        let dec_big = Decimal::from_unscaled_bytes(&[66, 237, 18, 59, 11, 216, 31, 4, 244], 22, 5)
            .expect("big decimal");
        let date_v = Date::new(20476);
        let time_v = Time::new(36_827_123);
        let ts_v = TimestampNtz::from_millis_nanos(1_769_163_227_123, 456_000).unwrap();
        let ts_ltz_v = TimestampLtz::new(1_769_163_227_123);
        let bytes_v = vec![0xDE_u8, 0xAD, 0xBE, 0xEF];
        let fixed_a = vec![0x01_u8, 0x02, 0x03, 0x04];
        let fixed_b = vec![0xAA_u8, 0xBB, 0xCC, 0xDD];

        // Row 0 — every column populated.
        let mut row0 = GenericRow::new(column_count);
        row0.set_field(0, 1_i32);

        // ARRAY basics
        row0.set_field(1, make_int_array(&[Some(10), Some(20), Some(30)]));
        row0.set_field(2, make_string_array(&[Some("hello"), Some("world")]));
        let arr_of_arr_0 = {
            let mut w = FlussArrayWriter::new(2, &inner_array_int);
            w.write_array(0, &make_int_array(&[Some(1), Some(2)]));
            w.write_array(1, &make_int_array(&[Some(3), Some(4)]));
            w.complete().expect("arr_of_arr_0")
        };
        row0.set_field(3, arr_of_arr_0);
        let arr_of_row_0 = {
            let mut w = FlussArrayWriter::new(2, &row_seq_label_owned);
            let mut e0 = GenericRow::new(2);
            e0.set_field(0, 1_i32);
            e0.set_field(1, "open");
            w.write_row(0, &e0).expect("e0");
            let mut e1 = GenericRow::new(2);
            e1.set_field(0, 2_i32);
            e1.set_field(1, "close");
            w.write_row(1, &e1).expect("e1");
            w.complete().expect("arr_of_row_0")
        };
        row0.set_field(4, arr_of_row_0);

        // ROW basics
        let mut row_basic_0 = GenericRow::new(2);
        row_basic_0.set_field(0, 42_i32);
        row_basic_0.set_field(1, "hello");
        row0.set_field(5, Datum::Row(Box::new(row_basic_0)));

        let mut row_deep_inner_0 = GenericRow::new(1);
        row_deep_inner_0.set_field(0, 99_i32);
        let mut row_deep_0 = GenericRow::new(1);
        row_deep_0.set_field(0, Datum::Row(Box::new(row_deep_inner_0)));
        row0.set_field(6, Datum::Row(Box::new(row_deep_0)));

        let mut row_rich_0 = GenericRow::new(14);
        row_rich_0.set_field(0, true);
        row_rich_0.set_field(1, 100_000_i32);
        row_rich_0.set_field(2, 9_876_543_210_i64);
        row_rich_0.set_field(3, f32::INFINITY);
        row_rich_0.set_field(4, f64::NAN);
        row_rich_0.set_field(5, "hello world");
        row_rich_0.set_field(6, b"binary".as_slice());
        row_rich_0.set_field(7, dec.clone());
        row_rich_0.set_field(8, Datum::Date(Date::new(20476)));
        row_rich_0.set_field(9, Datum::Time(Time::new(36_827_123)));
        row_rich_0.set_field(
            10,
            Datum::TimestampNtz(TimestampNtz::new(1_769_163_227_123)),
        );
        row_rich_0.set_field(
            11,
            Datum::TimestampLtz(TimestampLtz::new(1_769_163_227_456)),
        );
        row_rich_0.set_field(12, b"\x01\x02\x03\x04".as_slice());
        row_rich_0.set_field(13, make_int_array(&[Some(7), None, Some(11)]));
        row0.set_field(7, Datum::Row(Box::new(row_rich_0)));

        // MAP basics
        let map_string_int_0 = {
            let mut w = FlussMapWriter::new(3, &DataTypes::string(), &DataTypes::int());
            w.write_entry("a".into(), 1.into()).unwrap();
            w.write_entry("b".into(), Datum::Null).unwrap();
            w.write_entry("c".into(), 3.into()).unwrap();
            w.complete().expect("map_string_int_0")
        };
        row0.set_field(8, Datum::Map(map_string_int_0));

        let map_of_row_0 = {
            let mut e0 = GenericRow::new(2);
            e0.set_field(0, 1_i32);
            e0.set_field(1, "open");
            let mut e1 = GenericRow::new(2);
            e1.set_field(0, 2_i32);
            e1.set_field(1, "close");
            let mut w = FlussMapWriter::new(2, &DataTypes::string(), &row_seq_label_owned);
            w.write_entry("e0".into(), Datum::Row(Box::new(e0)))
                .unwrap();
            w.write_entry("e1".into(), Datum::Row(Box::new(e1)))
                .unwrap();
            w.complete().expect("map_of_row_0")
        };
        row0.set_field(9, Datum::Map(map_of_row_0));

        let map_of_map_0 = {
            let g1 = {
                let mut w = FlussMapWriter::new(2, &DataTypes::string(), &DataTypes::int());
                w.write_entry("a".into(), 1.into()).unwrap();
                w.write_entry("b".into(), 2.into()).unwrap();
                w.complete().expect("g1")
            };
            let g2 = {
                let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::int());
                w.write_entry("c".into(), 3.into()).unwrap();
                w.complete().expect("g2")
            };
            let mut w = FlussMapWriter::new(2, &DataTypes::string(), &inner_map_string_int);
            w.write_entry("g1".into(), Datum::Map(g1)).unwrap();
            w.write_entry("g2".into(), Datum::Map(g2)).unwrap();
            w.complete().expect("map_of_map_0")
        };
        row0.set_field(10, Datum::Map(map_of_map_0));

        let map_of_array_0 = {
            let primes = make_int_array(&[Some(2), Some(3), Some(5)]);
            let squares = make_int_array(&[Some(1), Some(4)]);
            let mut w = FlussMapWriter::new(2, &DataTypes::string(), &inner_array_int);
            w.write_entry("primes".into(), Datum::Array(primes))
                .unwrap();
            w.write_entry("squares".into(), Datum::Array(squares))
                .unwrap();
            w.complete().expect("map_of_array_0")
        };
        row0.set_field(11, Datum::Map(map_of_array_0));

        let array_of_map_0 = {
            let m0 = {
                let mut w = FlussMapWriter::new(2, &DataTypes::string(), &DataTypes::int());
                w.write_entry("x".into(), 1.into()).unwrap();
                w.write_entry("y".into(), 2.into()).unwrap();
                w.complete().expect("m0")
            };
            let m1 = {
                let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::int());
                w.write_entry("z".into(), 9.into()).unwrap();
                w.complete().expect("m1")
            };
            let mut w = FlussArrayWriter::new(2, &inner_map_string_int);
            w.write_map(0, &m0);
            w.write_map(1, &m1);
            w.complete().expect("array_of_map_0")
        };
        row0.set_field(12, array_of_map_0);

        // ARRAY rich types
        let arr_bytes_0 = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::bytes());
            w.write_binary_bytes(0, &bytes_v);
            w.set_null_at(1);
            w.complete().expect("arr_bytes_0")
        };
        row0.set_field(13, arr_bytes_0);
        let arr_date_0 = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::date());
            w.write_date(0, date_v);
            w.set_null_at(1);
            w.complete().expect("arr_date_0")
        };
        row0.set_field(14, arr_date_0);
        let arr_time_0 = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::time_with_precision(3));
            w.write_time(0, time_v);
            w.set_null_at(1);
            w.complete().expect("arr_time_0")
        };
        row0.set_field(15, arr_time_0);
        let arr_ts_0 = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::timestamp_with_precision(6));
            w.write_timestamp_ntz(0, &ts_v, 6);
            w.set_null_at(1);
            w.complete().expect("arr_ts_0")
        };
        row0.set_field(16, arr_ts_0);
        let arr_ts_ltz_0 = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::timestamp_ltz_with_precision(3));
            w.write_timestamp_ltz(0, &ts_ltz_v, 3);
            w.set_null_at(1);
            w.complete().expect("arr_ts_ltz_0")
        };
        row0.set_field(17, arr_ts_ltz_0);
        let arr_decimal_0 = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::decimal(10, 2));
            w.write_decimal(0, &dec, 10);
            w.set_null_at(1);
            w.complete().expect("arr_decimal_0")
        };
        row0.set_field(18, arr_decimal_0);
        let arr_decimal_big_0 = {
            let mut w = FlussArrayWriter::new(1, &DataTypes::decimal(22, 5));
            w.write_decimal(0, &dec_big, 22);
            w.complete().expect("arr_decimal_big_0")
        };
        row0.set_field(19, arr_decimal_big_0);
        let arr_float_0 = {
            let mut w = FlussArrayWriter::new(3, &DataTypes::float());
            w.write_float(0, f32::NAN);
            w.write_float(1, f32::INFINITY);
            w.write_float(2, f32::NEG_INFINITY);
            w.complete().expect("arr_float_0")
        };
        row0.set_field(20, arr_float_0);
        let arr_double_0 = {
            let mut w = FlussArrayWriter::new(3, &DataTypes::double());
            w.write_double(0, f64::NAN);
            w.write_double(1, f64::INFINITY);
            w.write_double(2, f64::NEG_INFINITY);
            w.complete().expect("arr_double_0")
        };
        row0.set_field(21, arr_double_0);
        let arr_binary_0 = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::binary(4));
            w.write_binary_bytes(0, &fixed_a);
            w.write_binary_bytes(1, &fixed_b);
            w.complete().expect("arr_binary_0")
        };
        row0.set_field(22, arr_binary_0);

        // MAP rich types
        let map_bytes_0 = {
            let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::bytes());
            w.write_entry("blob".into(), bytes_v.as_slice().into())
                .unwrap();
            w.complete().expect("map_bytes_0")
        };
        row0.set_field(23, Datum::Map(map_bytes_0));
        let map_decimal_0 = {
            let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::decimal(10, 2));
            w.write_entry("price".into(), Datum::Decimal(dec.clone()))
                .unwrap();
            w.complete().expect("map_decimal_0")
        };
        row0.set_field(24, Datum::Map(map_decimal_0));
        let map_date_0 = {
            let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::date());
            w.write_entry("d".into(), Datum::Date(date_v)).unwrap();
            w.complete().expect("map_date_0")
        };
        row0.set_field(25, Datum::Map(map_date_0));
        let map_time_0 = {
            let mut w =
                FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::time_with_precision(3));
            w.write_entry("t".into(), Datum::Time(time_v)).unwrap();
            w.complete().expect("map_time_0")
        };
        row0.set_field(26, Datum::Map(map_time_0));
        let map_ts_0 = {
            let mut w = FlussMapWriter::new(
                1,
                &DataTypes::string(),
                &DataTypes::timestamp_with_precision(6),
            );
            w.write_entry("ts".into(), Datum::TimestampNtz(ts_v))
                .unwrap();
            w.complete().expect("map_ts_0")
        };
        row0.set_field(27, Datum::Map(map_ts_0));
        let map_ts_ltz_0 = {
            let mut w = FlussMapWriter::new(
                1,
                &DataTypes::string(),
                &DataTypes::timestamp_ltz_with_precision(3),
            );
            w.write_entry("ts".into(), Datum::TimestampLtz(ts_ltz_v))
                .unwrap();
            w.complete().expect("map_ts_ltz_0")
        };
        row0.set_field(28, Datum::Map(map_ts_ltz_0));
        let map_float_0 = {
            let mut w = FlussMapWriter::new(2, &DataTypes::string(), &DataTypes::float());
            w.write_entry("nan".into(), f32::NAN.into()).unwrap();
            w.write_entry("inf".into(), f32::INFINITY.into()).unwrap();
            w.complete().expect("map_float_0")
        };
        row0.set_field(29, Datum::Map(map_float_0));
        let map_double_0 = {
            let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::double());
            w.write_entry("pi".into(), std::f64::consts::PI.into())
                .unwrap();
            w.complete().expect("map_double_0")
        };
        row0.set_field(30, Datum::Map(map_double_0));
        let map_bool_0 = {
            let mut w = FlussMapWriter::new(2, &DataTypes::string(), &DataTypes::boolean());
            w.write_entry("t".into(), true.into()).unwrap();
            w.write_entry("f".into(), false.into()).unwrap();
            w.complete().expect("map_bool_0")
        };
        row0.set_field(31, Datum::Map(map_bool_0));
        let map_binary_0 = {
            let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::binary(4));
            w.write_entry("k".into(), fixed_a.as_slice().into())
                .unwrap();
            w.complete().expect("map_binary_0")
        };
        row0.set_field(32, Datum::Map(map_binary_0));
        let map_int_key_0 = {
            let mut w = FlussMapWriter::new(2, &DataTypes::int(), &DataTypes::string());
            w.write_entry(1.into(), "one".into()).unwrap();
            w.write_entry(2.into(), "two".into()).unwrap();
            w.complete().expect("map_int_key_0")
        };
        row0.set_field(33, Datum::Map(map_int_key_0));

        // Scalar values
        let scalar_tinyint = 127_i8;
        let scalar_smallint = 32_767_i16;
        let scalar_bigint = 9_223_372_036_854_775_807_i64;
        let scalar_float = std::f32::consts::PI;
        let scalar_double = std::f64::consts::E;
        let scalar_char = "hello";
        let scalar_string = "world of fluss rust client";
        let scalar_time_s = Time::new(36_827_000);
        let scalar_time_ms = Time::new(36_827_123);
        let scalar_time_us = Time::new(86_399_999);
        let scalar_time_ns = Time::new(1);
        let scalar_ts_s = TimestampNtz::new(1_769_163_227_000);
        let scalar_ts_ms = TimestampNtz::new(1_769_163_227_123);
        let scalar_ts_us = TimestampNtz::from_millis_nanos(1_769_163_227_123, 456_000).unwrap();
        let scalar_ts_ns = TimestampNtz::from_millis_nanos(1_769_163_227_123, 999_999).unwrap();
        let scalar_ts_ltz_s = TimestampLtz::new(1_769_163_227_000);
        let scalar_ts_ltz_ms = TimestampLtz::new(1_769_163_227_123);
        let scalar_ts_ltz_us = TimestampLtz::from_millis_nanos(1_769_163_227_123, 456_000).unwrap();
        let scalar_ts_ltz_ns = TimestampLtz::from_millis_nanos(1_769_163_227_123, 999_999).unwrap();
        let scalar_bytes_top: Vec<u8> = b"binary data".to_vec();
        let scalar_binary_top: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let scalar_ts_us_neg = TimestampNtz::from_millis_nanos(-301_234_154_877, 456_000).unwrap();
        let scalar_ts_ns_neg = TimestampNtz::from_millis_nanos(-301_234_154_877, 999_999).unwrap();
        let scalar_ts_ltz_us_neg =
            TimestampLtz::from_millis_nanos(-301_234_154_877, 456_000).unwrap();
        let scalar_ts_ltz_ns_neg =
            TimestampLtz::from_millis_nanos(-301_234_154_877, 999_999).unwrap();

        row0.set_field(34, scalar_tinyint);
        row0.set_field(35, scalar_smallint);
        row0.set_field(36, scalar_bigint);
        row0.set_field(37, scalar_float);
        row0.set_field(38, scalar_double);
        row0.set_field(39, true);
        row0.set_field(40, scalar_char);
        row0.set_field(41, scalar_string);
        row0.set_field(42, dec.clone());
        row0.set_field(43, Datum::Date(date_v));
        row0.set_field(44, scalar_time_s);
        row0.set_field(45, scalar_time_ms);
        row0.set_field(46, scalar_time_us);
        row0.set_field(47, scalar_time_ns);
        row0.set_field(48, scalar_ts_s);
        row0.set_field(49, scalar_ts_ms);
        row0.set_field(50, scalar_ts_us);
        row0.set_field(51, scalar_ts_ns);
        row0.set_field(52, scalar_ts_ltz_s);
        row0.set_field(53, scalar_ts_ltz_ms);
        row0.set_field(54, scalar_ts_ltz_us);
        row0.set_field(55, scalar_ts_ltz_ns);
        row0.set_field(56, scalar_bytes_top.as_slice());
        row0.set_field(57, scalar_binary_top.as_slice());
        row0.set_field(58, scalar_ts_us_neg);
        row0.set_field(59, scalar_ts_ns_neg);
        row0.set_field(60, scalar_ts_ltz_us_neg);
        row0.set_field(61, scalar_ts_ltz_ns_neg);

        // Row 1 — ARRAY/MAP basic-shape edge cases (empty, null elements).
        let mut row1 = GenericRow::new(column_count);
        row1.set_field(0, 2_i32);
        row1.set_field(1, make_int_array(&[]));
        row1.set_field(2, make_string_array(&[None]));
        let arr_of_arr_1 = {
            let mut w = FlussArrayWriter::new(3, &inner_array_int);
            w.write_array(0, &make_int_array(&[Some(5)]));
            w.set_null_at(1);
            w.write_array(2, &make_int_array(&[]));
            w.complete().expect("arr_of_arr_1")
        };
        row1.set_field(3, arr_of_arr_1);
        let arr_of_row_1 = {
            let mut w = FlussArrayWriter::new(3, &row_seq_label_owned);
            let mut e0 = GenericRow::new(2);
            e0.set_field(0, 7_i32);
            e0.set_field(1, "x");
            w.write_row(0, &e0).expect("e0");
            w.set_null_at(1);
            let mut e2 = GenericRow::new(2);
            e2.set_field(0, 8_i32);
            e2.set_field(1, "y");
            w.write_row(2, &e2).expect("e2");
            w.complete().expect("arr_of_row_1")
        };
        row1.set_field(4, arr_of_row_1);
        for i in plan.section_range("row_basics") {
            row1.set_field(i, Datum::Null);
        }
        // Empty MAP
        let empty_map = FlussMapWriter::new(0, &DataTypes::string(), &DataTypes::int())
            .complete()
            .expect("empty_map");
        row1.set_field(8, Datum::Map(empty_map));
        for i in (plan.idx("map_string_int") + 1)..plan.len() {
            row1.set_field(i, Datum::Null);
        }

        // Row 2 — every column NULL.
        let mut row2 = GenericRow::new(column_count);
        row2.set_field(0, 3_i32);
        for i in 1..column_count {
            row2.set_field(i, Datum::Null);
        }

        writer.append(&row0).expect("append row0");
        writer.append(&row1).expect("append row1");
        writer.append(&row2).expect("append row2");
        writer.flush().await.expect("flush");

        let records = scan_table(&table, |scan| scan).await;
        assert_eq!(records.len(), 3);
        let r0 = records[0].row();
        let r1 = records[1].row();
        let r2 = records[2].row();

        assert_eq!(r0.get_int(0).unwrap(), 1);
        assert_eq!(r1.get_int(0).unwrap(), 2);
        assert_eq!(r2.get_int(0).unwrap(), 3);

        // === ARRAY: basic shapes ===
        let arr_int = r0.get_array(1).unwrap();
        assert_eq!(arr_int.size(), 3);
        assert_eq!(arr_int.get_int(0).unwrap(), 10);
        assert_eq!(arr_int.get_int(2).unwrap(), 30);
        let arr_string = r0.get_array(2).unwrap();
        assert_eq!(arr_string.size(), 2);
        assert_eq!(arr_string.get_string(0).unwrap(), "hello");
        assert_eq!(arr_string.get_string(1).unwrap(), "world");
        let arr_of_arr = r0.get_array(3).unwrap();
        assert_eq!(arr_of_arr.size(), 2);
        let inner = arr_of_arr.get_array(0).unwrap();
        assert_eq!(inner.size(), 2);
        assert_eq!(inner.get_int(0).unwrap(), 1);
        assert_eq!(inner.get_int(1).unwrap(), 2);
        let inner = arr_of_arr.get_array(1).unwrap();
        assert_eq!(inner.get_int(0).unwrap(), 3);
        assert_eq!(inner.get_int(1).unwrap(), 4);

        // === ARRAY: edge cases on row 1 (empty + null elements + null inner) ===
        assert_eq!(r1.get_array(1).unwrap().size(), 0);
        let arr_string_r1 = r1.get_array(2).unwrap();
        assert_eq!(arr_string_r1.size(), 1);
        assert!(arr_string_r1.is_null_at(0).unwrap());
        let arr_of_arr_r1 = r1.get_array(3).unwrap();
        assert_eq!(arr_of_arr_r1.size(), 3);
        let aa0 = arr_of_arr_r1.get_array(0).unwrap();
        assert_eq!(aa0.size(), 1);
        assert_eq!(aa0.get_int(0).unwrap(), 5);
        assert!(arr_of_arr_r1.is_null_at(1).unwrap());
        assert_eq!(arr_of_arr_r1.get_array(2).unwrap().size(), 0);

        // === ARRAY: null whole column on row 2 ===
        assert!(r2.is_null_at(1).unwrap());
        assert!(r2.is_null_at(2).unwrap());
        assert!(r2.is_null_at(3).unwrap());

        // === ARRAY<ROW>: row 0 + row 1 with null element + row 2 null whole ===
        let aor0 = r0.get_array(4).unwrap().expect_binary();
        assert_eq!(aor0.size(), 2);
        let e0 = aor0.get_row(0, &row_seq_label).unwrap();
        assert_eq!(e0.get_int(0).unwrap(), 1);
        assert_eq!(e0.get_string(1).unwrap(), "open");
        let e1 = aor0.get_row(1, &row_seq_label).unwrap();
        assert_eq!(e1.get_int(0).unwrap(), 2);
        assert_eq!(e1.get_string(1).unwrap(), "close");
        let aor1 = r1.get_array(4).unwrap().expect_binary();
        assert_eq!(aor1.size(), 3);
        let e0 = aor1.get_row(0, &row_seq_label).unwrap();
        assert_eq!(e0.get_int(0).unwrap(), 7);
        assert!(aor1.is_null_at(1));
        let e2 = aor1.get_row(2, &row_seq_label).unwrap();
        assert_eq!(e2.get_int(0).unwrap(), 8);
        assert!(r2.is_null_at(4).unwrap());

        // === ROW: basic + deep + rich types on row 0; row 2 null ===
        let rb = r0.get_row(5).unwrap();
        assert_eq!(rb.get_int(0).unwrap(), 42);
        assert_eq!(rb.get_string(1).unwrap(), "hello");
        let rd = r0.get_row(6).unwrap();
        let rd_inner = rd.get_row(0).unwrap();
        assert_eq!(rd_inner.get_int(0).unwrap(), 99);
        let rr = r0.get_row(7).unwrap();
        assert!(rr.get_boolean(0).unwrap());
        assert_eq!(rr.get_int(1).unwrap(), 100_000);
        assert_eq!(rr.get_long(2).unwrap(), 9_876_543_210);
        assert_f32_special(rr.get_float(3).unwrap(), f32::INFINITY);
        assert!(rr.get_double(4).unwrap().is_nan());
        assert_eq!(rr.get_string(5).unwrap(), "hello world");
        assert_eq!(rr.get_bytes(6).unwrap(), b"binary");
        assert_eq!(rr.get_decimal(7, 10, 2).unwrap(), dec);
        assert_eq!(rr.get_date(8).unwrap().get_inner(), 20476);
        assert_eq!(rr.get_time(9).unwrap().get_inner(), 36_827_123);
        assert_eq!(
            rr.get_timestamp_ntz(10, 6).unwrap().get_millisecond(),
            1_769_163_227_123
        );
        assert_eq!(
            rr.get_timestamp_ltz(11, 6).unwrap().get_epoch_millisecond(),
            1_769_163_227_456
        );
        assert_eq!(rr.get_binary(12, 4).unwrap(), b"\x01\x02\x03\x04");
        let f_arr = rr.get_array(13).unwrap();
        assert_eq!(f_arr.size(), 3);
        assert_eq!(f_arr.get_int(0).unwrap(), 7);
        assert!(f_arr.is_null_at(1).unwrap());
        assert!(r2.is_null_at(5).unwrap());
        assert!(r2.is_null_at(6).unwrap());
        assert!(r2.is_null_at(7).unwrap());

        // === MAP: basic (with null value) + empty (row 1) + null (row 2) ===
        let m = r0.get_map(8).unwrap().expect_binary();
        assert_eq!(m.size(), 3);
        assert_eq!(m.get(&Datum::from("a")).unwrap(), Some(Datum::from(1_i32)));
        assert_eq!(m.get(&Datum::from("b")).unwrap(), Some(Datum::Null));
        assert_eq!(m.get(&Datum::from("c")).unwrap(), Some(Datum::from(3_i32)));
        assert_eq!(r1.get_map(8).unwrap().size(), 0);
        assert!(r2.is_null_at(8).unwrap());

        // === MAP<K, ROW> ===
        let m = r0.get_map(9).unwrap().expect_binary();
        assert_eq!(m.size(), 2);
        let keys = m.key_array();
        let values = m.value_array();
        assert_eq!(keys.get_string(0).unwrap(), "e0");
        let v0 = values.get_row(0, &row_seq_label).unwrap();
        assert_eq!(v0.get_int(0).unwrap(), 1);
        assert_eq!(v0.get_string(1).unwrap(), "open");
        assert_eq!(keys.get_string(1).unwrap(), "e1");
        let v1 = values.get_row(1, &row_seq_label).unwrap();
        assert_eq!(v1.get_int(0).unwrap(), 2);
        assert_eq!(v1.get_string(1).unwrap(), "close");

        // === MAP<K, MAP> ===
        let m = r0.get_map(10).unwrap().expect_binary();
        assert_eq!(m.size(), 2);
        let g1 = m
            .value_array()
            .get_map(0, &DataTypes::string(), &DataTypes::int())
            .unwrap();
        assert_eq!(g1.size(), 2);
        assert_eq!(g1.get(&Datum::from("a")).unwrap(), Some(Datum::from(1_i32)));
        let g2 = m
            .value_array()
            .get_map(1, &DataTypes::string(), &DataTypes::int())
            .unwrap();
        assert_eq!(g2.size(), 1);
        assert_eq!(g2.get(&Datum::from("c")).unwrap(), Some(Datum::from(3_i32)));

        // === MAP<K, ARRAY> + ARRAY<MAP> ===
        let m = r0.get_map(11).unwrap().expect_binary();
        assert_eq!(m.size(), 2);
        let primes = m.value_array().get_array(0).unwrap();
        assert_eq!(primes.size(), 3);
        assert_eq!(primes.get_int(2).unwrap(), 5);
        let am = r0.get_array(12).unwrap().expect_binary();
        assert_eq!(am.size(), 2);
        let am0 = am
            .get_map(0, &DataTypes::string(), &DataTypes::int())
            .unwrap();
        assert_eq!(am0.size(), 2);
        let am1 = am
            .get_map(1, &DataTypes::string(), &DataTypes::int())
            .unwrap();
        assert_eq!(am1.size(), 1);
        assert_eq!(
            am1.get(&Datum::from("z")).unwrap(),
            Some(Datum::from(9_i32))
        );

        // === ARRAY rich types ===
        let ab = r0.get_array(13).unwrap();
        assert_eq!(ab.size(), 2);
        assert_eq!(ab.get_bytes(0).unwrap(), bytes_v.as_slice());
        assert!(ab.is_null_at(1).unwrap());
        let ad = r0.get_array(14).unwrap();
        assert_eq!(ad.get_date(0).unwrap().get_inner(), date_v.get_inner());
        assert!(ad.is_null_at(1).unwrap());
        let at = r0.get_array(15).unwrap();
        assert_eq!(at.get_time(0).unwrap().get_inner(), time_v.get_inner());
        assert!(at.is_null_at(1).unwrap());
        let ats = r0.get_array(16).unwrap();
        let read_ts = ats.get_timestamp_ntz(0, 6).unwrap();
        assert_eq!(read_ts.get_millisecond(), ts_v.get_millisecond());
        assert_eq!(
            read_ts.get_nano_of_millisecond(),
            ts_v.get_nano_of_millisecond()
        );
        assert!(ats.is_null_at(1).unwrap());
        let atl = r0.get_array(17).unwrap();
        assert_eq!(
            atl.get_timestamp_ltz(0, 3).unwrap().get_epoch_millisecond(),
            ts_ltz_v.get_epoch_millisecond()
        );
        assert!(atl.is_null_at(1).unwrap());
        let adc = r0.get_array(18).unwrap();
        assert_eq!(adc.get_decimal(0, 10, 2).unwrap(), dec);
        assert!(adc.is_null_at(1).unwrap());
        let adb = r0.get_array(19).unwrap();
        assert_eq!(adb.get_decimal(0, 22, 5).unwrap(), dec_big);
        let af = r0.get_array(20).unwrap();
        assert_eq!(af.size(), 3);
        assert_f32_special(af.get_float(0).unwrap(), f32::NAN);
        assert_f32_special(af.get_float(1).unwrap(), f32::INFINITY);
        assert_f32_special(af.get_float(2).unwrap(), f32::NEG_INFINITY);
        let adbl = r0.get_array(21).unwrap();
        assert_f64_special(adbl.get_double(0).unwrap(), f64::NAN);
        assert_f64_special(adbl.get_double(1).unwrap(), f64::INFINITY);
        assert_f64_special(adbl.get_double(2).unwrap(), f64::NEG_INFINITY);
        let fb = r0.get_array(22).unwrap().expect_binary();
        assert_eq!(fb.get_binary(0).unwrap(), fixed_a.as_slice());
        assert_eq!(fb.get_binary(1).unwrap(), fixed_b.as_slice());

        // === MAP rich types ===
        let m = r0.get_map(23).unwrap();
        assert_eq!(m.value_array().get_bytes(0).unwrap(), bytes_v.as_slice());
        let m = r0.get_map(24).unwrap();
        assert_eq!(m.value_array().get_decimal(0, 10, 2).unwrap(), dec);
        let m = r0.get_map(25).unwrap();
        assert_eq!(
            m.value_array().get_date(0).unwrap().get_inner(),
            date_v.get_inner()
        );
        let m = r0.get_map(26).unwrap();
        assert_eq!(
            m.value_array().get_time(0).unwrap().get_inner(),
            time_v.get_inner()
        );
        let m = r0.get_map(27).unwrap();
        let read_ts = m.value_array().get_timestamp_ntz(0, 6).unwrap();
        assert_eq!(read_ts.get_millisecond(), ts_v.get_millisecond());
        let m = r0.get_map(28).unwrap();
        let read_ltz = m.value_array().get_timestamp_ltz(0, 3).unwrap();
        assert_eq!(
            read_ltz.get_epoch_millisecond(),
            ts_ltz_v.get_epoch_millisecond()
        );
        let m = r0.get_map(29).unwrap();
        assert!(m.value_array().get_float(0).unwrap().is_nan());
        assert!(m.value_array().get_float(1).unwrap().is_infinite());
        let m = r0.get_map(30).unwrap();
        assert!(
            (m.value_array().get_double(0).unwrap() - std::f64::consts::PI).abs() < f64::EPSILON
        );
        let m = r0.get_map(31).unwrap();
        assert!(m.value_array().get_boolean(0).unwrap());
        assert!(!m.value_array().get_boolean(1).unwrap());
        let m = r0.get_map(32).unwrap().expect_binary();
        assert_eq!(m.value_array().get_binary(0).unwrap(), fixed_a.as_slice());
        let m = r0.get_map(33).unwrap();
        assert_eq!(m.size(), 2);
        assert_eq!(m.key_array().get_int(0).unwrap(), 1);
        assert_eq!(m.value_array().get_string(0).unwrap(), "one");

        // === Convenience API: entries / get / key_type / value_type ===
        // (exercised on row 0's map_string_int at index 8)
        let m = r0.get_map(8).unwrap().expect_binary();
        assert_eq!(m.key_type(), &DataTypes::string().as_non_nullable());
        assert_eq!(m.value_type(), &DataTypes::int());
        let mut got: HashMap<String, Option<i32>> = HashMap::with_capacity(m.size());
        for entry in m.entries() {
            let (k, v) = entry.expect("decode entry");
            let key = match k {
                Datum::String(s) => s.into_owned(),
                other => panic!("unexpected key variant: {other:?}"),
            };
            let value = match v {
                Datum::Int32(i) => Some(i),
                Datum::Null => None,
                other => panic!("unexpected value variant: {other:?}"),
            };
            got.insert(key, value);
        }
        let expected: HashMap<String, Option<i32>> = HashMap::from([
            ("a".to_string(), Some(1)),
            ("b".to_string(), None),
            ("c".to_string(), Some(3)),
        ]);
        assert_eq!(got, expected);
        assert_eq!(m.get(&Datum::from("a")).unwrap(), Some(Datum::from(1_i32)));
        assert!(m.get(&Datum::from("missing")).unwrap().is_none());

        // === Bulk write via FlussMapWriter::extend (covered with a fresh map) ===
        let src: HashMap<&str, i32> = HashMap::from([("a", 1), ("b", 2), ("c", 3)]);
        let extend_built = {
            let mut w = FlussMapWriter::new(src.len(), &DataTypes::string(), &DataTypes::int());
            w.extend(src.clone()).expect("extend");
            w.complete().expect("extend-complete")
        };
        assert_eq!(extend_built.size(), src.len());
        let extend_b = extend_built.get(&Datum::from("b")).unwrap();
        assert_eq!(extend_b, Some(Datum::from(2_i32)));

        // === Scalars: integer family ===
        assert_eq!(r0.get_byte(34).unwrap(), scalar_tinyint);
        assert_eq!(r0.get_short(35).unwrap(), scalar_smallint);
        assert_eq!(r0.get_long(36).unwrap(), scalar_bigint);

        // === Scalars: floating point ===
        assert!((r0.get_float(37).unwrap() - scalar_float).abs() < f32::EPSILON);
        assert!((r0.get_double(38).unwrap() - scalar_double).abs() < f64::EPSILON);

        // === Scalars: boolean / char / string ===
        assert!(r0.get_boolean(39).unwrap());
        assert_eq!(r0.get_char(40, 10).unwrap(), scalar_char);
        assert_eq!(r0.get_string(41).unwrap(), scalar_string);

        // === Scalars: decimal / date ===
        assert_eq!(r0.get_decimal(42, 10, 2).unwrap(), dec);
        assert_eq!(r0.get_date(43).unwrap().get_inner(), date_v.get_inner());

        // === Scalars: time across all four precisions ===
        assert_eq!(
            r0.get_time(44).unwrap().get_inner(),
            scalar_time_s.get_inner()
        );
        assert_eq!(
            r0.get_time(45).unwrap().get_inner(),
            scalar_time_ms.get_inner()
        );
        assert_eq!(
            r0.get_time(46).unwrap().get_inner(),
            scalar_time_us.get_inner()
        );
        assert_eq!(
            r0.get_time(47).unwrap().get_inner(),
            scalar_time_ns.get_inner()
        );

        // === Scalars: timestamp across all four precisions ===
        assert_eq!(
            r0.get_timestamp_ntz(48, 0).unwrap().get_millisecond(),
            scalar_ts_s.get_millisecond()
        );
        assert_eq!(
            r0.get_timestamp_ntz(49, 3).unwrap().get_millisecond(),
            scalar_ts_ms.get_millisecond()
        );
        let read_us = r0.get_timestamp_ntz(50, 6).unwrap();
        assert_eq!(read_us.get_millisecond(), scalar_ts_us.get_millisecond());
        assert_eq!(
            read_us.get_nano_of_millisecond(),
            scalar_ts_us.get_nano_of_millisecond()
        );
        let read_ns = r0.get_timestamp_ntz(51, 9).unwrap();
        assert_eq!(read_ns.get_millisecond(), scalar_ts_ns.get_millisecond());
        assert_eq!(
            read_ns.get_nano_of_millisecond(),
            scalar_ts_ns.get_nano_of_millisecond()
        );

        // === Scalars: timestamp_ltz across all four precisions ===
        assert_eq!(
            r0.get_timestamp_ltz(52, 0).unwrap().get_epoch_millisecond(),
            scalar_ts_ltz_s.get_epoch_millisecond()
        );
        assert_eq!(
            r0.get_timestamp_ltz(53, 3).unwrap().get_epoch_millisecond(),
            scalar_ts_ltz_ms.get_epoch_millisecond()
        );
        let read_ltz_us = r0.get_timestamp_ltz(54, 6).unwrap();
        assert_eq!(
            read_ltz_us.get_epoch_millisecond(),
            scalar_ts_ltz_us.get_epoch_millisecond()
        );
        assert_eq!(
            read_ltz_us.get_nano_of_millisecond(),
            scalar_ts_ltz_us.get_nano_of_millisecond()
        );
        let read_ltz_ns = r0.get_timestamp_ltz(55, 9).unwrap();
        assert_eq!(
            read_ltz_ns.get_epoch_millisecond(),
            scalar_ts_ltz_ns.get_epoch_millisecond()
        );
        assert_eq!(
            read_ltz_ns.get_nano_of_millisecond(),
            scalar_ts_ltz_ns.get_nano_of_millisecond()
        );

        // === Scalars: bytes + fixed binary ===
        assert_eq!(r0.get_bytes(56).unwrap(), scalar_bytes_top.as_slice());
        assert_eq!(r0.get_binary(57, 4).unwrap(), scalar_binary_top.as_slice());

        // === Scalars: negative-epoch timestamps (pre-1970) ===
        let read_neg_us = r0.get_timestamp_ntz(58, 6).unwrap();
        assert_eq!(
            read_neg_us.get_millisecond(),
            scalar_ts_us_neg.get_millisecond()
        );
        assert_eq!(
            read_neg_us.get_nano_of_millisecond(),
            scalar_ts_us_neg.get_nano_of_millisecond()
        );
        let read_neg_ns = r0.get_timestamp_ntz(59, 9).unwrap();
        assert_eq!(
            read_neg_ns.get_millisecond(),
            scalar_ts_ns_neg.get_millisecond()
        );
        assert_eq!(
            read_neg_ns.get_nano_of_millisecond(),
            scalar_ts_ns_neg.get_nano_of_millisecond()
        );
        let read_neg_ltz_us = r0.get_timestamp_ltz(60, 6).unwrap();
        assert_eq!(
            read_neg_ltz_us.get_epoch_millisecond(),
            scalar_ts_ltz_us_neg.get_epoch_millisecond()
        );
        let read_neg_ltz_ns = r0.get_timestamp_ltz(61, 9).unwrap();
        assert_eq!(
            read_neg_ltz_ns.get_epoch_millisecond(),
            scalar_ts_ltz_ns_neg.get_epoch_millisecond()
        );

        // === Scalars: every column NULL on row 2 ===
        for i in plan.section_range("scalars") {
            assert!(
                r2.is_null_at(i).unwrap(),
                "scalar column {i} should be null"
            );
        }

        // === Append-side validation: malformed rows are rejected client-side ===
        // Field count mismatch — far fewer fields than the schema demands.
        let mut undersized = GenericRow::new(2);
        undersized.set_field(0, true);
        let err = writer.append(&undersized).unwrap_err().to_string();
        assert!(
            err.contains(&format!("Expected: {column_count}")) && err.contains("Actual: 2"),
            "expected field-count error, got: {err}"
        );

        // Type mismatch — correct field count but every cell is Bool, which
        // satisfies none of the column types except col_boolean.
        let wrong_types = GenericRow::from_data(
            (0..column_count)
                .map(|_| Datum::Bool(true))
                .collect::<Vec<_>>(),
        );
        assert!(
            writer.append(&wrong_types).is_err(),
            "row with wrong types should be rejected, not panic"
        );

        admin.drop_table(&table_path, false).await.expect("drop");
    }
}
