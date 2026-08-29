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
        ColumnPlan, DEFAULT_POLL_TIMEOUT, array_dt_basics_columns, as_row_type, create_partitions,
        create_table, dt_array_int, dt_map_string_int, dt_row_seq_label, extract_ids_from_batches,
        get_shared_cluster, make_int_array, make_string_array, map_dt_basics_columns,
        poll_until_count, poll_until_nonempty, row_dt_basics_columns, scalar_dt_columns,
        wait_for_partitions_ready, wait_for_table_buckets_ready, wait_for_table_ready,
    };
    use arrow::array::{
        Array, ArrayRef, Int32Array, Int64Array, ListArray, MapArray, RecordBatch, StringArray,
        StructArray, record_batch,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use fluss::client::{EARLIEST_OFFSET, FlussAdmin, FlussTable, TableScan};
    use fluss::error::FlussError;
    use fluss::metadata::{
        AddColumn, AlterTableChanges, ColumnPositionType, DataField, DataTypes, JsonSerde,
        PartitionSpec, Schema, TableDescriptor, TablePath,
    };
    use fluss::predicate::{Literal, Predicate, col};
    use fluss::record::ScanRecord;
    use fluss::row::binary_array::FlussArrayWriter;
    use fluss::row::binary_map::FlussMapWriter;
    use fluss::row::{
        DataGetters, Date, Datum, Decimal, GenericRow, InternalArray, InternalMap, InternalRow,
        Time, TimestampLtz, TimestampNtz,
    };
    use fluss::rpc::message::OffsetSpec;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    fn reject_null(err: impl std::fmt::Display) {
        let text = err.to_string();
        assert!(
            text.contains("declared as non-nullable but contains null values"),
            "unexpected error: {text}"
        );
    }

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
                    .column("c3", DataTypes::bigint().as_non_nullable())
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

        {
            // Distinct c1 values hash across buckets; none may be enqueued.
            let poison_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("c1", ArrowDataType::Int32, true),
                Field::new("c2", ArrowDataType::Utf8, true),
                Field::new("c3", ArrowDataType::Int64, true),
            ]));
            let poison = RecordBatch::try_new(
                poison_schema,
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
                    Arc::new(Int64Array::from(vec![
                        Some(10),
                        Some(20),
                        None,
                        Some(40),
                        Some(50),
                        Some(60),
                    ])),
                ],
            )
            .expect("poison batch");
            let err = append_writer
                .append_arrow_batch(poison)
                .expect_err("null in NOT NULL column must be rejected before the bucket split");
            reject_null(err);

            // Offsets only move on flush.
            append_writer
                .flush()
                .await
                .expect("flush after rejected poison must not send rows");
            wait_for_table_ready(&admin, &table_path).await;
            let buckets: Vec<i32> = (0..table.get_table_info().get_num_buckets()).collect();
            let latest = admin
                .list_offsets(&table_path, &buckets, OffsetSpec::Latest)
                .await
                .expect("list_offsets after poison reject");
            assert!(
                latest.values().all(|&offset| offset == 0),
                "poison batch must not enqueue any bucket, latest={latest:?}"
            );
        }

        let batch1 = record_batch!(
            ("c1", Int32, [1, 2, 3]),
            ("c2", Utf8, ["a1", "a2", "a3"]),
            ("c3", Int64, [10, 20, 30])
        )
        .unwrap();
        append_writer
            .append_arrow_batch(batch1)
            .expect("Failed to append batch with mixed nullability");

        let batch2_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", ArrowDataType::Int32, true),
            Field::new("c2", ArrowDataType::Utf8, true),
            Field::new("c3", ArrowDataType::Int64, true),
        ]));
        let batch2 = RecordBatch::try_new(
            batch2_schema,
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(StringArray::from(vec!["a4", "a5", "a6"])),
                Arc::new(Int64Array::from(vec![40_i64, 50_i64, 60_i64])),
            ],
        )
        .expect("nullable-metadata batch without nulls");
        append_writer
            .append_arrow_batch(batch2)
            .expect("Failed to append nullable-metadata batch without null values");

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
        let mut collected = poll_until_count(
            6,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |d| {
                log_scanner
                    .poll(d)
                    .await
                    .expect("Failed to poll records")
                    .into_iter()
                    .map(|rec| {
                        let row = rec.row();
                        (
                            row.get_int(0).unwrap(),
                            row.get_string(1).unwrap().to_string(),
                            row.get_long(2).unwrap(),
                        )
                    })
                    .collect()
            },
        )
        .await;

        assert_eq!(collected.len(), 6, "Expected 6 records");

        // Sort and verify record contents
        collected.sort();
        let expected: Vec<(i32, String, i64)> = vec![
            (1, "a1".to_string(), 10),
            (2, "a2".to_string(), 20),
            (3, "a3".to_string(), 30),
            (4, "a4".to_string(), 40),
            (5, "a5".to_string(), 50),
            (6, "a6".to_string(), 60),
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
    async fn append_arrow_batch_nested_not_null_columns() {
        fn nested_batch(tags: ArrayRef, attrs: ArrayRef, nested: ArrayRef) -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("tags", tags.data_type().clone(), true),
                    Field::new("attrs", attrs.data_type().clone(), true),
                    Field::new("nested", nested.data_type().clone(), true),
                ])),
                vec![tags, attrs, nested],
            )
            .expect("nested batch")
        }

        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_append_arrow_batch_nested_not_null");
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("tags", DataTypes::array(DataTypes::int()).as_non_nullable())
                    .column(
                        "attrs",
                        DataTypes::map(DataTypes::string(), DataTypes::int()).as_non_nullable(),
                    )
                    .column(
                        "nested",
                        DataTypes::row(vec![
                            DataField::new("seq", DataTypes::int(), None),
                            DataField::new("label", DataTypes::string(), None),
                        ])
                        .as_non_nullable(),
                    )
                    .build()
                    .expect("schema"),
            )
            .build()
            .expect("table descriptor");
        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        let item_field = Arc::new(Field::new("item", ArrowDataType::Int32, true));
        let ok_list: ArrayRef = Arc::new(ListArray::new(
            Arc::clone(&item_field),
            OffsetBuffer::new(vec![0_i32, 2].into()),
            Arc::new(Int32Array::from(vec![10, 20])),
            None,
        ));
        let null_list: ArrayRef = Arc::new(ListArray::new(
            item_field,
            OffsetBuffer::new(vec![0_i32, 0].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Some(NullBuffer::from(vec![false])),
        ));

        let key_field = Arc::new(Field::new("key", ArrowDataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", ArrowDataType::Int32, true));
        let ok_entries = StructArray::from(vec![
            (
                Arc::clone(&key_field),
                Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
            ),
            (
                Arc::clone(&value_field),
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ),
        ]);
        let ok_map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", ok_entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0_i32, 2].into()),
            ok_entries,
            None,
            false,
        ));
        let empty_entries = StructArray::from(vec![
            (
                key_field,
                Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
            ),
            (
                value_field,
                Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            ),
        ]);
        let null_map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new(
                "entries",
                empty_entries.data_type().clone(),
                false,
            )),
            OffsetBuffer::new(vec![0_i32, 0].into()),
            empty_entries,
            Some(NullBuffer::from(vec![false])),
            false,
        ));

        let row_fields = arrow::datatypes::Fields::from(vec![
            Field::new("seq", ArrowDataType::Int32, true),
            Field::new("label", ArrowDataType::Utf8, true),
        ]);
        let ok_struct: ArrayRef = Arc::new(StructArray::new(
            row_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![42])) as ArrayRef,
                Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
            ],
            None,
        ));
        let null_struct: ArrayRef = Arc::new(StructArray::new(
            row_fields,
            vec![
                Arc::new(Int32Array::from(vec![None])) as ArrayRef,
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ],
            Some(NullBuffer::from(vec![false])),
        ));

        reject_null(
            writer
                .append_arrow_batch(nested_batch(
                    Arc::clone(&null_list),
                    Arc::clone(&ok_map),
                    Arc::clone(&ok_struct),
                ))
                .expect_err("null ARRAY value in NOT NULL column"),
        );
        reject_null(
            writer
                .append_arrow_batch(nested_batch(
                    Arc::clone(&ok_list),
                    Arc::clone(&null_map),
                    Arc::clone(&ok_struct),
                ))
                .expect_err("null MAP value in NOT NULL column"),
        );
        reject_null(
            writer
                .append_arrow_batch(nested_batch(
                    Arc::clone(&ok_list),
                    Arc::clone(&ok_map),
                    Arc::clone(&null_struct),
                ))
                .expect_err("null ROW value in NOT NULL column"),
        );

        writer
            .append_arrow_batch(nested_batch(ok_list, ok_map, ok_struct))
            .expect("populated nested values must be accepted");
        writer.flush().await.expect("flush");

        let records = scan_table(&table, |scan| scan).await;
        assert_eq!(records.len(), 1);
        let row = records[0].row();

        let tags = row.get_array(0).expect("tags");
        assert_eq!(tags.size(), 2);
        assert_eq!(tags.get_int(0).unwrap(), 10);
        assert_eq!(tags.get_int(1).unwrap(), 20);

        let attrs = row.get_map(1).expect("attrs");
        assert_eq!(attrs.size(), 2);
        assert_eq!(attrs.key_array().get_string(0).unwrap(), "x");
        assert_eq!(attrs.value_array().get_int(0).unwrap(), 1);
        assert_eq!(attrs.key_array().get_string(1).unwrap(), "y");
        assert_eq!(attrs.value_array().get_int(1).unwrap(), 2);

        let nested = row.get_row(2).expect("nested");
        assert_eq!(nested.get_int(0).unwrap(), 42);
        assert_eq!(nested.get_string(1).unwrap(), "hello");
    }

    #[tokio::test]
    async fn append_arrow_batch_rejects_nulls_inside_not_null_nested_fields() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");
        let table_path = TablePath::new("fluss", "test_append_arrow_batch_not_null_inside_nested");
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column(
                        "tags",
                        DataTypes::array(DataTypes::int().as_non_nullable()).as_non_nullable(),
                    )
                    .column(
                        "nested",
                        DataTypes::row(vec![
                            DataField::new("seq", DataTypes::int().as_non_nullable(), None),
                            DataField::new("label", DataTypes::string(), None),
                        ])
                        .as_non_nullable(),
                    )
                    .build()
                    .expect("schema"),
            )
            .build()
            .expect("table descriptor");
        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        let item_field = Arc::new(Field::new("item", ArrowDataType::Int32, true));
        let ok_list: ArrayRef = Arc::new(ListArray::new(
            Arc::clone(&item_field),
            OffsetBuffer::new(vec![0_i32, 3].into()),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            None,
        ));
        let poison_list: ArrayRef = Arc::new(ListArray::new(
            item_field,
            OffsetBuffer::new(vec![0_i32, 3].into()),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            None,
        ));

        let row_fields = arrow::datatypes::Fields::from(vec![
            Field::new("seq", ArrowDataType::Int32, true),
            Field::new("label", ArrowDataType::Utf8, true),
        ]);
        let ok_struct: ArrayRef = Arc::new(StructArray::new(
            row_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![42])) as ArrayRef,
                Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
            ],
            None,
        ));
        let poison_struct: ArrayRef = Arc::new(StructArray::new(
            row_fields,
            vec![
                Arc::new(Int32Array::from(vec![None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("hello")])) as ArrayRef,
            ],
            None,
        ));

        fn batch(tags: ArrayRef, nested: ArrayRef) -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("tags", tags.data_type().clone(), true),
                    Field::new("nested", nested.data_type().clone(), true),
                ])),
                vec![tags, nested],
            )
            .expect("batch")
        }

        reject_null(
            writer
                .append_arrow_batch(batch(Arc::clone(&poison_list), Arc::clone(&ok_struct)))
                .expect_err("null ARRAY element in ARRAY<INT NOT NULL>"),
        );
        reject_null(
            writer
                .append_arrow_batch(batch(Arc::clone(&ok_list), Arc::clone(&poison_struct)))
                .expect_err("null ROW field in ROW<seq INT NOT NULL>"),
        );

        writer
            .append_arrow_batch(batch(ok_list, ok_struct))
            .expect("null-free nested values must be accepted");
        writer.flush().await.expect("flush");
    }

    #[tokio::test]
    async fn append_and_scan_with_iceberg_format() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");
        let table_path = TablePath::new("fluss", "test_append_with_iceberg_format");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .distributed_by(Some(3), vec!["id".to_string()])
            .property("table.datalake.format", "iceberg")
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
            .expect("Failed to create Iceberg writer");

        for (id, name) in [(34, "Frost"), (35, "Ember"), (36, "Gale")] {
            let mut row = GenericRow::new(2);
            row.set_field(0, id);
            row.set_field(1, name);
            append_writer
                .append(&row)
                .expect("Failed to append Iceberg row");
        }
        append_writer.flush().await.expect("Failed to flush");

        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        for bucket_id in 0..table.get_table_info().get_num_buckets() {
            log_scanner
                .subscribe(bucket_id, EARLIEST_OFFSET)
                .await
                .expect("Failed to subscribe with EARLIEST_OFFSET");
        }

        let mut collected = poll_until_count(
            3,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |d| {
                log_scanner
                    .poll(d)
                    .await
                    .expect("Failed to poll records")
                    .into_iter()
                    .map(|record| {
                        let row = record.row();
                        (
                            row.get_int(0).unwrap(),
                            row.get_string(1).unwrap().to_string(),
                        )
                    })
                    .collect()
            },
        )
        .await;

        collected.sort();
        assert_eq!(
            collected,
            vec![
                (34, "Frost".to_string()),
                (35, "Ember".to_string()),
                (36, "Gale".to_string()),
            ]
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
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

        let record_timestamps = poll_until_count(
            3,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |d| {
                log_scanner
                    .poll(d)
                    .await
                    .expect("Failed to poll records")
                    .into_iter()
                    .map(|rec| rec.timestamp())
                    .collect()
            },
        )
        .await;
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

    /// Creates a single-bucket log table with statistics enabled and tiered
    /// segments kept local, since remotely read segments bypass server-side
    /// filtering.
    async fn create_stats_log_table(admin: &FlussAdmin, table_path: &TablePath) {
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .property("table.statistics.columns", "*")
            .property("table.log.tiered.local-segments", "100")
            .build()
            .expect("Failed to build table");
        create_table(admin, table_path, &table_descriptor).await;
        wait_for_table_ready(admin, table_path).await;
    }

    async fn poll_ids_and_names(
        log_scanner: &fluss::client::LogScanner,
        expected_count: usize,
    ) -> Vec<(i32, String)> {
        let mut collected = poll_until_count(
            expected_count,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |d| {
                log_scanner
                    .poll(d)
                    .await
                    .expect("Failed to poll records")
                    .into_iter()
                    .map(|rec| {
                        let row = rec.row();
                        (
                            row.get_int(0).unwrap(),
                            row.get_string(1).unwrap().to_string(),
                        )
                    })
                    .collect()
            },
        )
        .await;
        collected.sort();
        collected
    }

    /// Batches whose statistics cannot match the predicate are pruned from the
    /// fetch, batches that can match are returned whole.
    #[tokio::test]
    async fn filter_pushdown_prunes_non_matching_batches() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_filter_pushdown_prune");
        create_stats_log_table(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        // Three wire batches with disjoint id ranges, so min/max pruning is
        // deterministic: a batch either fully matches the filter or cannot match.
        writer
            .append_arrow_batch(
                record_batch!(
                    ("id", Int32, [1, 2, 3, 4, 5]),
                    ("name", Utf8, ["v1", "v2", "v3", "v4", "v5"])
                )
                .unwrap(),
            )
            .expect("Failed to append batch 1");
        writer
            .append_arrow_batch(
                record_batch!(
                    ("id", Int32, [100, 101, 102, 103, 104]),
                    ("name", Utf8, ["v100", "v101", "v102", "v103", "v104"])
                )
                .unwrap(),
            )
            .expect("Failed to append batch 2");
        writer
            .append_arrow_batch(
                record_batch!(
                    ("id", Int32, [200, 201, 202, 203, 204]),
                    ("name", Utf8, ["v200", "v201", "v202", "v203", "v204"])
                )
                .unwrap(),
            )
            .expect("Failed to append batch 3");
        writer.flush().await.expect("Failed to flush");

        let log_scanner = table
            .new_scan()
            .filter(col("id").ge(200))
            .expect("Failed to set filter")
            .create_log_scanner()
            .expect("Failed to create log scanner");
        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe");

        let collected = poll_ids_and_names(&log_scanner, 5).await;
        let expected: Vec<(i32, String)> = (200..=204).map(|id| (id, format!("v{id}"))).collect();
        assert_eq!(
            collected, expected,
            "Only the batch overlapping the predicate should be fetched; the two \
             disjoint batches must be pruned server-side"
        );

        // The filter resolves against the full row type, so filtering on a column
        // excluded from the projection must still prune correctly.
        let projected_scanner = table
            .new_scan()
            .project_by_name(&["name"])
            .expect("Failed to project")
            .filter(col("id").ge(200))
            .expect("Failed to set filter")
            .create_log_scanner()
            .expect("Failed to create projected log scanner");
        projected_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe");

        let mut names = poll_until_count(
            5,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |d| {
                projected_scanner
                    .poll(d)
                    .await
                    .expect("Failed to poll records")
                    .into_iter()
                    .map(|rec| rec.row().get_string(0).unwrap().to_string())
                    .collect()
            },
        )
        .await;
        names.sort();
        assert_eq!(
            names,
            vec!["v200", "v201", "v202", "v203", "v204"],
            "Filtering on a non-projected column should still prune correctly"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Pruning is batch-granular, so a batch whose statistics overlap the
    /// predicate is returned whole, non-matching rows included.
    #[tokio::test]
    async fn filter_with_overlapping_statistics_returns_whole_batch() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_filter_overlapping_statistics");
        create_stats_log_table(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        writer
            .append_arrow_batch(
                record_batch!(
                    ("id", Int32, [1, 2, 3, 4, 5]),
                    ("name", Utf8, ["low1", "low2", "low3", "low4", "low5"])
                )
                .unwrap(),
            )
            .expect("Failed to append non-matching batch");
        writer
            .append_arrow_batch(
                record_batch!(
                    ("id", Int32, [1, 6, 3, 8, 2]),
                    ("name", Utf8, ["m1", "m6", "m3", "m8", "m2"])
                )
                .unwrap(),
            )
            .expect("Failed to append mixed batch");
        writer.flush().await.expect("Failed to flush");

        let log_scanner = table
            .new_scan()
            .filter(col("id").gt(5))
            .expect("Failed to set filter")
            .create_log_scanner()
            .expect("Failed to create log scanner");
        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe");

        let collected = poll_ids_and_names(&log_scanner, 5).await;
        let expected: Vec<(i32, String)> = vec![
            (1, "m1".to_string()),
            (2, "m2".to_string()),
            (3, "m3".to_string()),
            (6, "m6".to_string()),
            (8, "m8".to_string()),
        ];
        assert_eq!(
            collected, expected,
            "The mixed batch (min=1, max=8) should be returned whole as a \
             superset, while the all-low batch is pruned"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Row appends are buffered through the row-to-Arrow builder rather than
    /// passed through as a ready-made batch, and the batches it builds must
    /// still carry prunable statistics.
    #[tokio::test]
    async fn filter_pushdown_prunes_row_appended_batches() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_filter_pushdown_row_append");
        create_stats_log_table(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        // Flush between the groups so each becomes its own wire batch.
        for id in 1..=5 {
            let mut row = GenericRow::new(2);
            row.set_field(0, id);
            row.set_field(1, format!("low{id}"));
            writer.append(&row).expect("Failed to append row");
        }
        writer.flush().await.expect("Failed to flush");
        for id in 200..=204 {
            let mut row = GenericRow::new(2);
            row.set_field(0, id);
            row.set_field(1, format!("v{id}"));
            writer.append(&row).expect("Failed to append row");
        }
        writer.flush().await.expect("Failed to flush");

        let log_scanner = table
            .new_scan()
            .filter(col("id").ge(200))
            .expect("Failed to set filter")
            .create_log_scanner()
            .expect("Failed to create log scanner");
        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe");

        let collected = poll_ids_and_names(&log_scanner, 5).await;
        let expected: Vec<(i32, String)> = (200..=204).map(|id| (id, format!("v{id}"))).collect();
        assert_eq!(
            collected, expected,
            "Row-appended batches must carry statistics, so the low batch is pruned"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Pruning across the statistics encodings: spilled string bounds, compact
    /// and non-compact decimals and timestamps, time with seconds-to-millis
    /// scaling, and null counts.
    #[tokio::test]
    async fn filter_pushdown_prunes_across_column_types() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_filter_pushdown_column_types");
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .column("price", DataTypes::decimal(10, 2))
                    .column("big", DataTypes::decimal(22, 5))
                    .column("ts3", DataTypes::timestamp_with_precision(3))
                    .column("ts6", DataTypes::timestamp_with_precision(6))
                    .column("t", DataTypes::time_with_precision(0))
                    .column("opt", DataTypes::int())
                    .build()
                    .expect("Failed to build schema"),
            )
            .property("table.statistics.columns", "*")
            .property("table.log.tiered.local-segments", "100")
            .build()
            .expect("Failed to build table");
        create_table(&admin, &table_path, &table_descriptor).await;
        wait_for_table_ready(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        // Names exceed 7 bytes so the string bounds spill to the stats row tail.
        for id in 1..=3 {
            let mut row = GenericRow::new(8);
            row.set_field(0, id);
            row.set_field(1, format!("aaaaaaaaaaaa-{id}"));
            row.set_field(
                2,
                Decimal::from_unscaled_long(1_000 + id as i64, 10, 2).unwrap(),
            );
            row.set_field(
                3,
                Decimal::from_unscaled_bytes(&(100_000_i64 + id as i64).to_be_bytes(), 22, 5)
                    .unwrap(),
            );
            row.set_field(
                4,
                TimestampNtz::from_millis_nanos(1_600_000_000_000 + id as i64, 0).unwrap(),
            );
            row.set_field(
                5,
                TimestampNtz::from_millis_nanos(1_600_000_000_000 + id as i64, 123_000).unwrap(),
            );
            row.set_field(6, Time::new(32_400_000 + id * 1_000));
            row.set_field(7, Datum::Null);
            writer.append(&row).expect("Failed to append low row");
        }
        writer.flush().await.expect("Failed to flush");
        for id in 101..=103 {
            let mut row = GenericRow::new(8);
            row.set_field(0, id);
            row.set_field(1, format!("zzzzzzzzzzzz-{id}"));
            row.set_field(
                2,
                Decimal::from_unscaled_long(99_000 + id as i64, 10, 2).unwrap(),
            );
            row.set_field(
                3,
                Decimal::from_unscaled_bytes(
                    &(900_000_000_000_i64 + id as i64).to_be_bytes(),
                    22,
                    5,
                )
                .unwrap(),
            );
            row.set_field(
                4,
                TimestampNtz::from_millis_nanos(1_900_000_000_000 + id as i64, 0).unwrap(),
            );
            row.set_field(
                5,
                TimestampNtz::from_millis_nanos(1_900_000_000_000 + id as i64, 456_000).unwrap(),
            );
            row.set_field(6, Time::new(72_000_000 + id * 1_000));
            row.set_field(7, id);
            writer.append(&row).expect("Failed to append high row");
        }
        writer.flush().await.expect("Failed to flush");

        let low_ids = vec![1, 2, 3];
        let high_ids = vec![101, 102, 103];
        let cases: Vec<(&str, Predicate, &Vec<i32>)> = vec![
            ("string", col("name").gt("mmmmmmmmmmmm"), &high_ids),
            (
                "compact decimal",
                col("price").gt(Decimal::from_unscaled_long(50_000, 10, 2).unwrap()),
                &high_ids,
            ),
            (
                "non-compact decimal",
                col("big").gt(Decimal::from_unscaled_bytes(
                    &500_000_000_000_i64.to_be_bytes(),
                    22,
                    5,
                )
                .unwrap()),
                &high_ids,
            ),
            (
                "compact timestamp",
                col("ts3").gt(TimestampNtz::from_millis_nanos(1_800_000_000_000, 0).unwrap()),
                &high_ids,
            ),
            (
                "non-compact timestamp",
                col("ts6").gt(TimestampNtz::from_millis_nanos(1_800_000_000_000, 500_000).unwrap()),
                &high_ids,
            ),
            ("time", col("t").gt(Literal::Time(43_200_000)), &high_ids),
            ("is_not_null", col("opt").is_not_null(), &high_ids),
            ("is_null", col("opt").is_null(), &low_ids),
        ];

        for (label, predicate, expected) in cases {
            let log_scanner = table
                .new_scan()
                .filter(predicate)
                .expect("Failed to set filter")
                .create_log_scanner()
                .expect("Failed to create log scanner");
            log_scanner
                .subscribe(0, EARLIEST_OFFSET)
                .await
                .expect("Failed to subscribe");
            let ids: Vec<i32> = poll_ids_and_names(&log_scanner, expected.len())
                .await
                .into_iter()
                .map(|(id, _)| id)
                .collect();
            assert_eq!(
                &ids, expected,
                "Predicate on {label} should prune the non-matching batch"
            );
        }

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// When every batch is pruned the server returns only a filtered end
    /// offset, and the scanner must advance across the fully pruned segments
    /// instead of refetching them forever.
    #[tokio::test]
    async fn filter_pushdown_advances_past_fully_pruned_range() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_filter_pushdown_fully_pruned");
        create_stats_log_table(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        for (ids, names) in [
            ([1, 2, 3, 4, 5], ["a1", "a2", "a3", "a4", "a5"]),
            ([6, 7, 8, 9, 10], ["a6", "a7", "a8", "a9", "a10"]),
            ([11, 12, 13, 14, 15], ["a11", "a12", "a13", "a14", "a15"]),
        ] {
            writer
                .append_arrow_batch(
                    record_batch!(("id", Int32, ids.to_vec()), ("name", Utf8, names.to_vec()))
                        .unwrap(),
                )
                .expect("Failed to append batch");
        }
        writer.flush().await.expect("Failed to flush");

        // Prove the data is fetchable before asserting the filtered scan sees none.
        let unfiltered = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create unfiltered log scanner");
        unfiltered
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe");
        let all = poll_ids_and_names(&unfiltered, 15).await;
        assert_eq!(
            all.len(),
            15,
            "All rows should be visible to an unfiltered scan"
        );

        let log_scanner = table
            .new_scan()
            .filter(col("id").gt(1000))
            .expect("Failed to set filter")
            .create_log_scanner()
            .expect("Failed to create log scanner");
        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe");

        // Every existing batch is pruned, so the poll must come back empty
        // rather than erroring or returning non-matching rows.
        let records = log_scanner
            .poll(Duration::from_secs(3))
            .await
            .expect("Failed to poll fully pruned range");
        let leaked_ids: Vec<i32> = records
            .into_iter()
            .map(|rec| rec.row().get_int(0).unwrap())
            .collect();
        assert!(
            leaked_ids.is_empty(),
            "A fully pruned fetch should return no records, got ids: {leaked_ids:?}"
        );

        // New matching data appended after the pruned range must be reachable.
        writer
            .append_arrow_batch(
                record_batch!(
                    ("id", Int32, [2001, 2002, 2003, 2004, 2005]),
                    ("name", Utf8, ["b1", "b2", "b3", "b4", "b5"])
                )
                .unwrap(),
            )
            .expect("Failed to append matching batch");
        writer.flush().await.expect("Failed to flush");

        let collected = poll_ids_and_names(&log_scanner, 5).await;
        let collected_ids: Vec<i32> = collected.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            collected_ids,
            vec![2001, 2002, 2003, 2004, 2005],
            "The scanner should advance past the fully pruned range and reach \
             the new matching batch"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Without `table.statistics.columns` batches carry no statistics, so the
    /// server cannot prune and a filtered scan returns every row.
    #[tokio::test]
    async fn filter_without_statistics_returns_all_rows() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_filter_without_statistics");
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .property("table.log.tiered.local-segments", "100")
            .build()
            .expect("Failed to build table");
        create_table(&admin, &table_path, &table_descriptor).await;
        wait_for_table_ready(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");
        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [1, 2, 3]), ("name", Utf8, ["x1", "x2", "x3"]))
                    .unwrap(),
            )
            .expect("Failed to append batch 1");
        writer
            .append_arrow_batch(
                record_batch!(
                    ("id", Int32, [100, 101, 102]),
                    ("name", Utf8, ["y1", "y2", "y3"])
                )
                .unwrap(),
            )
            .expect("Failed to append batch 2");
        writer.flush().await.expect("Failed to flush");

        let log_scanner = table
            .new_scan()
            .filter(col("id").ge(100))
            .expect("Failed to set filter")
            .create_log_scanner()
            .expect("Failed to create log scanner");
        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe");

        let collected = poll_ids_and_names(&log_scanner, 6).await;
        let collected_ids: Vec<i32> = collected.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            collected_ids,
            vec![1, 2, 3, 100, 101, 102],
            "Without statistics the server cannot prune, so the scan returns a \
             superset containing every row"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
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
        let all_ids =
            poll_until_count(6, DEFAULT_POLL_TIMEOUT, Duration::from_secs(5), async |d| {
                let batches = scanner.poll(d).await.unwrap();
                extract_ids_from_batches(&batches)
            })
            .await;

        // Test 2: Order should be preserved across multiple batches
        assert_eq!(all_ids, vec![1, 2, 3, 4, 5, 6]);

        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [7, 8]), ("name", Utf8, ["g", "h"])).unwrap(),
            )
            .unwrap();
        writer.flush().await.unwrap();

        let new_ids =
            poll_until_count(2, DEFAULT_POLL_TIMEOUT, Duration::from_secs(5), async |d| {
                let more = scanner.poll(d).await.unwrap();
                extract_ids_from_batches(&more)
            })
            .await;

        // Test 3: Subsequent polls should not return duplicate data (offset continuation)
        assert_eq!(new_ids, vec![7, 8]);

        // Test 4: Subscribing from mid-offset should truncate batch (Arrow batch slicing)
        // Server returns all records from start of batch, but client truncates to subscription offset
        let trunc_scanner = table.new_scan().create_record_batch_log_scanner().unwrap();
        trunc_scanner.subscribe(0, 3).await.unwrap();
        let trunc_ids =
            poll_until_count(5, DEFAULT_POLL_TIMEOUT, Duration::from_secs(5), async |d| {
                let trunc_batches = trunc_scanner.poll(d).await.unwrap();
                extract_ids_from_batches(&trunc_batches)
            })
            .await;

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
        let proj_batches =
            poll_until_nonempty(DEFAULT_POLL_TIMEOUT, Duration::from_secs(5), async |d| {
                proj.poll(d).await.unwrap()
            })
            .await;
        assert!(
            !proj_batches.is_empty(),
            "Expected at least one batch from projection scanner"
        );

        // Projected batch should have 1 column (id), not 2 (id, name)
        assert_eq!(proj_batches[0].batch().num_columns(), 1);
    }

    async fn create_region_partitioned_log_table(admin: &FlussAdmin, table_path: &TablePath) {
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

        create_table(admin, table_path, &table_descriptor).await;
        create_partitions(admin, table_path, "region", &["US", "EU"]).await;
        wait_for_partitions_ready(admin, table_path, &["US", "EU"]).await;
    }

    /// Integration test covering produce and scan operations for all supported datatypes
    /// in log tables.
    #[tokio::test]
    async fn partitioned_table_append_scan() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_partitioned_log_append");

        create_region_partitioned_log_table(&admin, &table_path).await;

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
        let partition_infos = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partition infos");
        let nonexistent_partition_id = i64::MAX;
        assert_eq!(
            log_scanner
                .subscribe_partition(nonexistent_partition_id, 0, 0)
                .await
                .expect_err("Subscribing to a nonexistent partition should fail")
                .api_error(),
            Some(FlussError::PartitionNotExists)
        );
        for partition_info in &partition_infos {
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

        let mut collected_records = poll_until_count(
            expected_records.len(),
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |d| {
                log_scanner
                    .poll(d)
                    .await
                    .expect("Failed to poll log scanner")
                    .into_iter()
                    .map(|rec| {
                        let row = rec.row();
                        (
                            row.get_int(0).unwrap(),
                            row.get_string(1).unwrap().to_string(),
                            row.get_long(2).unwrap(),
                        )
                    })
                    .collect()
            },
        )
        .await;

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

        let records_after_unsubscribe = poll_until_count(
            4,
            Duration::from_secs(5),
            Duration::from_millis(300),
            async |d| {
                log_scanner_unsub
                    .poll(d)
                    .await
                    .expect("Failed to poll after unsubscribe")
                    .into_iter()
                    .map(|rec| {
                        let row = rec.row();
                        (
                            row.get_int(0).unwrap(),
                            row.get_string(1).unwrap().to_string(),
                            row.get_long(2).unwrap(),
                        )
                    })
                    .collect()
            },
        )
        .await;

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
        let invalid_partition_bucket_offsets = HashMap::from([
            ((partition_infos[0].get_partition_id(), 0), 0),
            ((nonexistent_partition_id, 0), 0),
        ]);
        assert_eq!(
            log_scanner_batch
                .subscribe_partition_buckets(&invalid_partition_bucket_offsets)
                .await
                .expect_err("Batch subscription containing a nonexistent partition should fail")
                .api_error(),
            Some(FlussError::PartitionNotExists)
        );
        let partition_bucket_offsets: HashMap<(i64, i32), i64> = partition_infos
            .iter()
            .map(|p| ((p.get_partition_id(), 0), 0i64))
            .collect();
        log_scanner_batch
            .subscribe_partition_buckets(&partition_bucket_offsets)
            .await
            .expect("Failed to batch subscribe to partitions");

        let mut batch_collected = poll_until_count(
            expected_records.len(),
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |d| {
                log_scanner_batch
                    .poll(d)
                    .await
                    .expect("Failed to poll after batch partition subscribe")
                    .into_iter()
                    .map(|rec| {
                        let row = rec.row();
                        (
                            row.get_int(0).unwrap(),
                            row.get_string(1).unwrap().to_string(),
                            row.get_long(2).unwrap(),
                        )
                    })
                    .collect()
            },
        )
        .await;
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

    #[tokio::test]
    async fn partitioned_scanner_continues_after_partition_drop() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");
        let table_path = TablePath::new("fluss", "test_scanner_after_partition_drop");
        create_region_partitioned_log_table(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let append_writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");
        let mut row = GenericRow::new(3);
        row.set_field(0, 1);
        row.set_field(1, "US");
        row.set_field(2, 100_i64);
        append_writer.append(&row).expect("Failed to append row");
        append_writer.flush().await.expect("Failed to flush");

        let partition_infos = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partition infos");
        let partition_bucket_offsets: HashMap<(i64, i32), i64> = partition_infos
            .iter()
            .map(|partition| ((partition.get_partition_id(), 0), 0))
            .collect();
        let scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        scanner
            .subscribe_partition_buckets(&partition_bucket_offsets)
            .await
            .expect("Failed to subscribe to partitions");

        admin
            .drop_partition(
                &table_path,
                &PartitionSpec::new(HashMap::from([("region", "EU")])),
                false,
            )
            .await
            .expect("Failed to drop EU partition");

        let ids = poll_until_count(
            1,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |timeout| {
                scanner
                    .poll(timeout)
                    .await
                    .expect("Failed to poll surviving partition")
                    .into_iter()
                    .filter_map(|record| {
                        let row = record.row();
                        if row.get_string(1).unwrap() == "US" {
                            Some(row.get_int(0).unwrap())
                        } else {
                            None
                        }
                    })
                    .collect()
            },
        )
        .await;
        assert_eq!(
            ids,
            vec![1],
            "The surviving partition should remain readable"
        );

        let mut row = GenericRow::new(3);
        row.set_field(0, 2);
        row.set_field(1, "US");
        row.set_field(2, 200_i64);
        append_writer.append(&row).expect("Failed to append row");
        append_writer.flush().await.expect("Failed to flush");

        let ids = poll_until_count(
            1,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |timeout| {
                scanner
                    .poll(timeout)
                    .await
                    .expect("Failed to poll surviving partition")
                    .into_iter()
                    .filter_map(|record| {
                        let row = record.row();
                        if row.get_string(1).unwrap() == "US" {
                            Some(row.get_int(0).unwrap())
                        } else {
                            None
                        }
                    })
                    .collect()
            },
        )
        .await;
        assert_eq!(
            ids,
            vec![2],
            "The surviving partition should keep making progress after another partition is dropped"
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

    #[tokio::test]
    async fn schema_evolution_add_column_log_scanner_dynamic_schema() {
        run_schema_evolution_add_column_log_scanner(false).await;
    }

    #[tokio::test]
    async fn schema_evolution_add_column_log_scanner_fixed_schema() {
        run_schema_evolution_add_column_log_scanner(true).await;
    }

    /// Test that a single log scanner can read records across a schema change
    /// (add column) in both dynamic-schema and fixed-schema modes.
    async fn run_schema_evolution_add_column_log_scanner(fixed_schema: bool) {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new(
            "fluss",
            format!("test_schema_evolution_log_scanner_fixed_{fixed_schema}"),
        );

        // 1. Create table with initial schema: (id INT, name STRING)
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

        // 2. Get table handle and create scanner + subscribe from EARLIEST
        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let log_scanner = table
            .new_scan()
            .with_fixed_schema(fixed_schema)
            .create_log_scanner()
            .expect("Failed to create log scanner");
        let num_buckets = table.get_table_info().get_num_buckets();
        for bucket_id in 0..num_buckets {
            log_scanner
                .subscribe(bucket_id, EARLIEST_OFFSET)
                .await
                .expect("Failed to subscribe");
        }

        // 3. Write records with old schema
        let writer_v0 = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");
        writer_v0
            .append_arrow_batch(
                record_batch!(
                    ("id", Int32, [1, 2, 3]),
                    ("name", Utf8, ["alice", "bob", "charlie"])
                )
                .unwrap(),
            )
            .expect("Failed to append old-schema batch");
        writer_v0.flush().await.expect("flush");

        // 4. Poll old-schema records
        let mut old_records = poll_until_count(
            3,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |d| {
                log_scanner
                    .poll(d)
                    .await
                    .expect("poll")
                    .into_iter()
                    .map(|rec| {
                        let row = rec.row();
                        (
                            row.get_int(0).unwrap(),
                            row.get_string(1).unwrap().to_string(),
                        )
                    })
                    .collect()
            },
        )
        .await;
        assert_eq!(old_records.len(), 3, "Expected 3 old-schema records");
        old_records.sort_by_key(|r| r.0);
        assert_eq!(
            old_records,
            vec![
                (1, "alice".to_string()),
                (2, "bob".to_string()),
                (3, "charlie".to_string()),
            ]
        );

        // 5. Alter table: add column "age INT"
        let age_type_json = serde_json::to_vec(
            &DataTypes::int()
                .serialize_json()
                .expect("serialize INT type"),
        )
        .expect("to_vec");
        admin
            .alter_table(
                &table_path,
                false,
                AlterTableChanges {
                    add_columns: vec![AddColumn {
                        column_name: "age".to_string(),
                        data_type_json: age_type_json,
                        comment: None,
                        position: ColumnPositionType::Last,
                    }],
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to alter table");

        // 6. Get a new table handle with the updated schema and write new records
        let table_v1 = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get updated table");
        let writer_v1 = table_v1
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");
        writer_v1
            .append_arrow_batch(
                record_batch!(
                    ("id", Int32, [4, 5, 6]),
                    ("name", Utf8, ["dave", "eve", "frank"]),
                    ("age", Int32, [30, 25, 40])
                )
                .unwrap(),
            )
            .expect("Failed to append new-schema batch");
        writer_v1.flush().await.expect("flush");

        // 7. Continue reading from the SAME scanner — it should handle schema
        //    evolution and return the new-schema records with the extra column.
        let mut new_records = poll_until_count(
            3,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |d| {
                log_scanner
                    .poll(d)
                    .await
                    .expect("poll")
                    .into_iter()
                    .map(|rec| {
                        let row = rec.row();
                        let id = row.get_int(0).unwrap();
                        let name = row.get_string(1).unwrap().to_string();
                        let age = if row.get_field_count() > 2 && !row.is_null_at(2).unwrap_or(true)
                        {
                            Some(row.get_int(2).unwrap())
                        } else {
                            None
                        };
                        (id, name, age)
                    })
                    .collect()
            },
        )
        .await;
        assert_eq!(new_records.len(), 3, "Expected 3 new-schema records");
        new_records.sort_by_key(|r| r.0);
        assert_eq!(
            new_records,
            vec![
                (4, "dave".to_string(), (!fixed_schema).then_some(30),),
                (5, "eve".to_string(), (!fixed_schema).then_some(25)),
                (6, "frank".to_string(), (!fixed_schema).then_some(40),),
            ]
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn append_hash_distributes_by_declared_bucket_key() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_log_append_bucket_key_spread");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .column("c3", DataTypes::bigint().as_non_nullable())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(3), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;
        wait_for_table_buckets_ready(&admin, &table_path, &[0, 1, 2]).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        let row_count: i32 = 30;
        for id in 1..=row_count {
            let mut row = GenericRow::new(3);
            row.set_field(0, id);
            row.set_field(1, "x");
            row.set_field(2, id as i64 * 10);
            writer
                .append(&row)
                .expect("append row with mixed nullability");
        }
        writer.flush().await.expect("flush");

        let offsets = admin
            .list_offsets(&table_path, &[0, 1, 2], OffsetSpec::Latest)
            .await
            .expect("list offsets");

        let total: i64 = offsets.values().sum();
        assert_eq!(
            total, row_count as i64,
            "every appended row must be persisted, got per-bucket offsets {offsets:?}"
        );

        let non_empty = offsets.values().filter(|&&o| o > 0).count();
        assert!(
            non_empty >= 2,
            "rows must hash-distribute across buckets, got per-bucket offsets {offsets:?}"
        );

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    #[tokio::test]
    async fn append_same_bucket_key_lands_in_one_bucket() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_log_append_bucket_key_colocate");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(3), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;
        wait_for_table_buckets_ready(&admin, &table_path, &[0, 1, 2]).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        for _ in 0..5 {
            let mut row = GenericRow::new(2);
            row.set_field(0, 42);
            row.set_field(1, "x");
            writer.append(&row).expect("append row");
        }
        writer.flush().await.expect("flush");

        let offsets = admin
            .list_offsets(&table_path, &[0, 1, 2], OffsetSpec::Latest)
            .await
            .expect("list offsets");

        let non_empty: Vec<_> = offsets.iter().filter(|&(_, &o)| o > 0).collect();
        assert_eq!(
            non_empty.len(),
            1,
            "all rows with the same bucket key must land in one bucket, got {offsets:?}"
        );
        assert_eq!(
            *non_empty[0].1, 5,
            "that bucket must hold all five rows, got {offsets:?}"
        );

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    #[tokio::test]
    async fn append_arrow_batch_splits_mixed_keys_across_buckets() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_log_append_arrow_split");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(3), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;
        wait_for_table_buckets_ready(&admin, &table_path, &[0, 1, 2]).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        let batch = record_batch!(
            ("c1", Int32, [1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c2", Utf8, ["a", "b", "c", "d", "e", "f", "g", "h", "i"])
        )
        .unwrap();
        writer.append_arrow_batch(batch).expect("append batch");
        writer.flush().await.expect("flush");

        let offsets = admin
            .list_offsets(&table_path, &[0, 1, 2], OffsetSpec::Latest)
            .await
            .expect("list offsets");

        let total: i64 = offsets.values().sum();
        assert_eq!(total, 9, "all rows must be persisted, got {offsets:?}");
        let non_empty = offsets.values().filter(|&&o| o > 0).count();
        assert!(
            non_empty >= 2,
            "a mixed-key batch must be split across buckets, got {offsets:?}"
        );

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    /// An empty first batch must not lock in a sticky assigner and break later keyed appends.
    #[tokio::test]
    async fn append_empty_batch_first_keeps_key_distribution() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_log_append_empty_first");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(3), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;
        wait_for_table_buckets_ready(&admin, &table_path, &[0, 1, 2]).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        let empty = record_batch!(("c1", Int32, [1]), ("c2", Utf8, ["x"]))
            .unwrap()
            .slice(0, 0);
        writer
            .append_arrow_batch(empty)
            .expect("append empty batch");

        let batch = record_batch!(
            ("c1", Int32, [1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c2", Utf8, ["a", "b", "c", "d", "e", "f", "g", "h", "i"])
        )
        .unwrap();
        writer.append_arrow_batch(batch).expect("append batch");
        writer.flush().await.expect("flush");

        let offsets = admin
            .list_offsets(&table_path, &[0, 1, 2], OffsetSpec::Latest)
            .await
            .expect("list offsets");

        let total: i64 = offsets.values().sum();
        assert_eq!(total, 9, "all rows must be persisted, got {offsets:?}");
        let non_empty = offsets.values().filter(|&&o| o > 0).count();
        assert!(
            non_empty >= 2,
            "keyed rows must still spread across buckets after an empty first batch, got {offsets:?}"
        );

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    /// A pending write to a dropped table must complete with TableNotExist
    /// instead of retrying UnknownTableOrBucketException forever, and the
    /// stale table metadata must be evicted so later writes fail fast.
    #[tokio::test]
    async fn write_after_drop_completes_with_table_not_exist() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_log_write_after_drop");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(1), vec![])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;
        wait_for_table_ready(&admin, &table_path).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        // Warm the metadata cache with a successful write.
        let mut row = GenericRow::new(1);
        row.set_field(0, 1);
        writer
            .append(&row)
            .expect("append")
            .await
            .expect("first write");

        admin.drop_table(&table_path, false).await.expect("drop");

        // Append until a write observes the drop, tolerating writes that still
        // succeed while the drop propagates to the tablet servers. Every write
        // shares one budget, so a write left retrying trips the timeout instead
        // of running until the caller's own deadline.
        let deadline = std::time::Instant::now() + DEFAULT_POLL_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "writes kept succeeding after the table was dropped"
            );
            let result = match writer.append(&row) {
                Ok(write_future) => tokio::time::timeout(remaining, write_future)
                    .await
                    .expect("write must settle promptly instead of retrying until the deadline"),
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => tokio::time::sleep(Duration::from_millis(500)).await,
                Err(error) => {
                    assert_eq!(
                        error.api_error(),
                        Some(FlussError::TableNotExist),
                        "Expected TableNotExist error, got {:?}",
                        error
                    );
                    break;
                }
            }
        }

        // The stale metadata is evicted, so the next append fails fast rather
        // than hanging or panicking in the bucket assigner, and it reports the
        // same error as the write that observed the drop.
        let error = writer
            .append(&row)
            .expect_err("append after eviction must fail");
        assert_eq!(
            error.api_error(),
            Some(FlussError::TableNotExist),
            "Expected TableNotExist error, got {:?}",
            error
        );
    }

    /// Drives real rows through a live cluster across a drop and a recreate of
    /// the same path, covering both table instance behaviours end to end.
    #[tokio::test]
    async fn real_data_survives_drop_and_recreate_of_the_same_path() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_real_data_drop_recreate");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(3), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;
        wait_for_table_buckets_ready(&admin, &table_path, &[0, 1, 2]).await;

        // Real rows across all three buckets must persist before the drop.
        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");
        let batch = record_batch!(
            ("c1", Int32, [1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c2", Utf8, ["a", "b", "c", "d", "e", "f", "g", "h", "i"])
        )
        .unwrap();
        writer.append_arrow_batch(batch).expect("append batch");
        writer.flush().await.expect("flush");
        let offsets = admin
            .list_offsets(&table_path, &[0, 1, 2], OffsetSpec::Latest)
            .await
            .expect("list offsets");
        let total: i64 = offsets.values().sum();
        assert_eq!(total, 9, "all rows must persist, got {offsets:?}");

        admin.drop_table(&table_path, false).await.expect("drop");

        // Writes settle with TableNotExist instead of retrying to the deadline.
        let mut row = GenericRow::new(2);
        row.set_field(0, 42);
        row.set_field(1, "after-drop");
        let deadline = std::time::Instant::now() + DEFAULT_POLL_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "writes kept succeeding after the drop"
            );
            let result = match writer.append(&row) {
                Ok(write_future) => tokio::time::timeout(remaining, write_future)
                    .await
                    .expect("write must settle, not retry to the deadline"),
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => tokio::time::sleep(Duration::from_millis(500)).await,
                Err(error) => {
                    assert_eq!(
                        error.api_error(),
                        Some(FlussError::TableNotExist),
                        "Expected TableNotExist, got {:?}",
                        error
                    );
                    break;
                }
            }
        }

        // The writer sees the drop before the coordinator finishes clearing it,
        // so wait for the server side to settle before recreating the path.
        let deadline = std::time::Instant::now() + DEFAULT_POLL_TIMEOUT;
        while admin.table_exists(&table_path).await.unwrap_or(true) {
            assert!(
                std::time::Instant::now() < deadline,
                "the dropped table never disappeared server side"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // The recreated path must hold only rows written to the new table instance.
        create_table(&admin, &table_path, &descriptor).await;
        wait_for_table_buckets_ready(&admin, &table_path, &[0, 1, 2]).await;

        let fresh_table = connection.get_table(&table_path).await.expect("table");
        let fresh_writer = fresh_table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");
        let batch2 = record_batch!(
            ("c1", Int32, [10, 11, 12, 13]),
            ("c2", Utf8, ["j", "k", "l", "m"])
        )
        .unwrap();
        fresh_writer
            .append_arrow_batch(batch2)
            .expect("append batch");
        fresh_writer.flush().await.expect("flush");

        let offsets = admin
            .list_offsets(&table_path, &[0, 1, 2], OffsetSpec::Latest)
            .await
            .expect("list offsets");
        let total: i64 = offsets.values().sum();
        assert_eq!(
            total, 4,
            "the recreated table must hold only its own rows, got {offsets:?}"
        );

        admin.drop_table(&table_path, false).await.expect("drop");
    }
}
