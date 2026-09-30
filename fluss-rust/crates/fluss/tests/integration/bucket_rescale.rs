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

/// Mirrors Java's `PartitionBucketCountRescaleITCase`: after `bucket.num` changes, the
/// "old" partition keeps its bucket count and the "new" one uses the changed count.
#[cfg(test)]
mod bucket_rescale_test {
    use crate::integration::utils::{
        DEFAULT_POLL_TIMEOUT, create_table, get_shared_cluster, poll_until_count,
        wait_for_partitions_ready,
    };
    use fluss::PartitionId;
    use fluss::client::{BoundedLogReadRange, FlussAdmin, FlussTable, RecordBatchLogReader};
    use fluss::error::{Error, FlussError};
    use fluss::metadata::{
        AlterConfig, AlterConfigOpType, AlterTableChanges, BucketStatsRequest, DataTypes,
        PartitionInfo, PartitionSpec, Schema, TableBucket, TableDescriptor, TablePath,
    };
    use fluss::row::{DataGetters, GenericRow};
    use fluss::rpc::message::OffsetSpec;
    use std::collections::HashMap;
    use std::time::Duration;

    const OLD_BUCKET_NUM: i32 = 2;
    const NEW_BUCKET_NUM: i32 = 4;
    const RECORDS_PER_PARTITION: i32 = 12;
    const PARTITIONS: [&str; 2] = ["old", "new"];

    fn log_schema() -> Schema {
        Schema::builder()
            .column("a", DataTypes::int())
            .column("b", DataTypes::string())
            .column("c", DataTypes::string())
            .build()
            .expect("schema")
    }

    fn pk_schema(primary_keys: &[&str]) -> Schema {
        Schema::builder()
            .column("a", DataTypes::int())
            .column("b", DataTypes::string())
            .column("c", DataTypes::string())
            .primary_key(primary_keys.to_vec())
            .expect("primary key")
            .build()
            .expect("schema")
    }

    fn row(a: i32, b: &str, c: &str) -> GenericRow<'static> {
        let mut row = GenericRow::new(3);
        row.set_field(0, a);
        row.set_field(1, b.to_string());
        row.set_field(2, c.to_string());
        row
    }

    fn key(a: i32, c: &str) -> GenericRow<'static> {
        let mut key = GenericRow::new(2);
        key.set_field(0, a);
        key.set_field(1, c.to_string());
        key
    }

    async fn create_partitioned_table(
        admin: &FlussAdmin,
        table_path: &TablePath,
        schema: Schema,
        bucket_keys: &[&str],
    ) {
        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(
                Some(OLD_BUCKET_NUM),
                bucket_keys.iter().map(|k| k.to_string()).collect(),
            )
            .partitioned_by(vec!["c"])
            .build()
            .expect("table descriptor");
        create_table(admin, table_path, &descriptor).await;
    }

    async fn create_partition(admin: &FlussAdmin, table_path: &TablePath, value: &str) {
        admin
            .create_partition(
                table_path,
                &PartitionSpec::new(HashMap::from([("c", value)])),
                false,
            )
            .await
            .expect("create partition");
    }

    async fn alter_bucket_num(admin: &FlussAdmin, table_path: &TablePath, bucket_num: i32) {
        admin
            .alter_table(
                table_path,
                false,
                AlterTableChanges {
                    new_bucket_count: Some(bucket_num),
                    ..Default::default()
                },
            )
            .await
            .expect("alter bucket.num");
    }

    async fn setup_old_new_partitions(
        admin: &FlussAdmin,
        table_path: &TablePath,
    ) -> Vec<PartitionInfo> {
        create_partition(admin, table_path, "old").await;
        alter_bucket_num(admin, table_path, NEW_BUCKET_NUM).await;
        create_partition(admin, table_path, "new").await;
        wait_for_partitions_ready(admin, table_path, &PARTITIONS).await;

        let partition_infos = admin
            .list_partition_infos(table_path)
            .await
            .expect("list partitions");
        assert_eq!(
            bucket_count_by_name(&partition_infos),
            HashMap::from([
                ("old".to_string(), OLD_BUCKET_NUM),
                ("new".to_string(), NEW_BUCKET_NUM),
            ])
        );
        partition_infos
    }

    fn bucket_count_by_name(partition_infos: &[PartitionInfo]) -> HashMap<String, i32> {
        partition_infos
            .iter()
            .map(|p| (p.get_partition_name(), p.get_bucket_count()))
            .collect()
    }

    fn partition_id(partition_infos: &[PartitionInfo], name: &str) -> PartitionId {
        partition_infos
            .iter()
            .find(|p| p.get_partition_name() == name)
            .expect("partition")
            .get_partition_id()
    }

    async fn scan_all_buckets(
        table: &FlussTable<'_>,
        partition_infos: &[PartitionInfo],
        expected_total: usize,
    ) -> Vec<(PartitionId, i32, i32, String)> {
        let scanner = table.new_scan().create_log_scanner().expect("log scanner");
        for info in partition_infos {
            for bucket in 0..info.get_bucket_count() {
                scanner
                    .subscribe_partition(info.get_partition_id(), bucket, 0)
                    .await
                    .expect("subscribe");
            }
        }
        let records = poll_until_count(
            expected_total,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(200),
            async |timeout| {
                let mut records = Vec::new();
                for (bucket, bucket_records) in scanner
                    .poll(timeout)
                    .await
                    .expect("poll")
                    .into_records_by_buckets()
                {
                    for record in bucket_records {
                        let row = record.row();
                        records.push((
                            bucket.partition_id().expect("partition id"),
                            bucket.bucket_id(),
                            row.get_int(0).unwrap(),
                            row.get_string(1).unwrap().to_string(),
                        ));
                    }
                }
                records
            },
        )
        .await;
        assert_eq!(records.len(), expected_total);
        records
    }

    #[tokio::test]
    async fn log_table_read_write_across_rescale() {
        let cluster = get_shared_cluster();
        let admin_connection = cluster.get_fluss_connection().await;
        let admin = admin_connection.get_admin().expect("admin");
        let table_path = TablePath::new("fluss", "test_rescale_log_table");
        create_partitioned_table(&admin, &table_path, log_schema(), &[]).await;
        let partition_infos = setup_old_new_partitions(&admin, &table_path).await;

        // A connection that has cached no partition yet.
        let connection = cluster.get_fluss_connection().await;
        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");
        let mut acks = Vec::new();
        for partition in PARTITIONS {
            for a in 0..RECORDS_PER_PARTITION {
                acks.push(
                    writer
                        .append(&row(a, &format!("v{a}"), partition))
                        .expect("append"),
                );
            }
        }
        writer.flush().await.expect("flush");
        for ack in acks {
            ack.await.expect("append ack");
        }

        let records = scan_all_buckets(
            &table,
            &partition_infos,
            PARTITIONS.len() * RECORDS_PER_PARTITION as usize,
        )
        .await;
        for partition in PARTITIONS {
            let id = partition_id(&partition_infos, partition);
            let mut rows: Vec<(i32, String)> = records
                .iter()
                .filter(|(p, ..)| *p == id)
                .map(|(_, _, a, b)| (*a, b.clone()))
                .collect();
            rows.sort();
            let expected: Vec<(i32, String)> = (0..RECORDS_PER_PARTITION)
                .map(|a| (a, format!("v{a}")))
                .collect();
            assert_eq!(rows, expected, "rows of partition {partition}");
        }

        let bucket_count = bucket_count_by_name(&partition_infos);
        for partition in PARTITIONS {
            let buckets: Vec<i32> = (0..bucket_count[partition]).collect();
            let offsets = admin
                .list_partition_offsets(&table_path, partition, &buckets, OffsetSpec::Latest)
                .await
                .expect("list offsets");
            assert_eq!(
                offsets.values().sum::<i64>(),
                RECORDS_PER_PARTITION as i64,
                "offsets of partition {partition}"
            );
        }

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    #[tokio::test]
    async fn pk_table_read_paths_across_rescale() {
        let cluster = get_shared_cluster();
        let admin_connection = cluster.get_fluss_connection().await;
        let admin = admin_connection.get_admin().expect("admin");
        let table_path = TablePath::new("fluss", "test_rescale_pk_read_paths");
        create_partitioned_table(&admin, &table_path, pk_schema(&["a", "c"]), &[]).await;
        let partition_infos = setup_old_new_partitions(&admin, &table_path).await;
        let bucket_count = bucket_count_by_name(&partition_infos);

        // A connection that has cached no partition yet.
        let connection = cluster.get_fluss_connection().await;
        let table = connection.get_table(&table_path).await.expect("table");
        let table_id = table.get_table_info().table_id;
        let writer = table
            .new_upsert()
            .expect("upsert")
            .create_writer()
            .expect("writer");
        let mut acks = Vec::new();
        for partition in PARTITIONS {
            for a in 0..RECORDS_PER_PARTITION {
                acks.push(
                    writer
                        .upsert(&row(a, &format!("v{a}"), partition))
                        .expect("upsert"),
                );
            }
        }
        writer.flush().await.expect("flush");
        for ack in acks {
            ack.await.expect("upsert ack");
        }

        let mut lookuper = table
            .new_lookup()
            .expect("lookup")
            .create_lookuper()
            .expect("lookuper");
        for partition in PARTITIONS {
            for a in 0..RECORDS_PER_PARTITION {
                let result = lookuper.lookup(&key(a, partition)).await.expect("lookup");
                let row = result
                    .get_single_row()
                    .expect("row")
                    .unwrap_or_else(|| panic!("missing key {a} in partition {partition}"));
                assert_eq!(row.get_string(1).unwrap(), format!("v{a}"));
            }
        }

        let records = scan_all_buckets(
            &table,
            &partition_infos,
            PARTITIONS.len() * RECORDS_PER_PARTITION as usize,
        )
        .await;
        for partition in PARTITIONS {
            let id = partition_id(&partition_infos, partition);
            let buckets_used: Vec<i32> = records
                .iter()
                .filter(|(p, ..)| *p == id)
                .map(|(_, bucket, ..)| *bucket)
                .collect();
            assert_eq!(buckets_used.len(), RECORDS_PER_PARTITION as usize);
            assert!(
                buckets_used.iter().all(|b| *b < bucket_count[partition]),
                "partition {partition} used buckets {buckets_used:?}"
            );
        }

        for partition in PARTITIONS {
            let id = partition_id(&partition_infos, partition);
            let mut rows = 0;
            for bucket in 0..bucket_count[partition] {
                let batches = table
                    .new_scan()
                    .limit(RECORDS_PER_PARTITION)
                    .expect("limit")
                    .create_bucket_batch_scanner(TableBucket::new_with_partition(
                        table_id,
                        Some(id),
                        bucket,
                    ))
                    .expect("batch scanner")
                    .collect_all_batches()
                    .await
                    .expect("limit scan");
                rows += batches.iter().map(|b| b.batch().num_rows()).sum::<usize>();
            }
            assert_eq!(
                rows, RECORDS_PER_PARTITION as usize,
                "limit scan of partition {partition}"
            );
        }

        let buckets = partition_infos
            .iter()
            .flat_map(|info| {
                (0..info.get_bucket_count())
                    .map(|bucket| BucketStatsRequest::new(Some(info.get_partition_id()), bucket))
            })
            .collect();
        let stats = admin
            .get_table_stats(table_id, buckets, vec![])
            .await
            .expect("table stats");
        for bucket in &stats.buckets {
            assert_eq!(bucket.error, None, "stats of {bucket:?}");
        }
        assert_eq!(
            stats
                .buckets
                .iter()
                .map(|bucket| bucket.row_count.unwrap_or(0))
                .sum::<i64>(),
            PARTITIONS.len() as i64 * RECORDS_PER_PARTITION as i64
        );

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    /// A writer opened before the change fails its first batch to a later partition, and a retry
    /// with the same writer succeeds.
    #[tokio::test]
    async fn stale_writer_retries_writes_to_a_partition_created_after_rescale() {
        let cluster = get_shared_cluster();
        let admin_connection = cluster.get_fluss_connection().await;
        let admin = admin_connection.get_admin().expect("admin");
        // Admin calls refresh the metadata of their connection, so the writer gets its own.
        let connection = cluster.get_fluss_connection().await;

        for new_bucket_num in [1, NEW_BUCKET_NUM] {
            let table_path = TablePath::new(
                "fluss",
                format!("test_rescale_stale_writer_{new_bucket_num}"),
            );
            create_partitioned_table(&admin, &table_path, pk_schema(&["a", "c"]), &[]).await;

            let table = connection.get_table(&table_path).await.expect("table");
            let writer = table
                .new_upsert()
                .expect("upsert")
                .create_writer()
                .expect("writer");
            alter_bucket_num(&admin, &table_path, new_bucket_num).await;
            create_partition(&admin, &table_path, "later").await;
            wait_for_partitions_ready(&admin, &table_path, &["later"]).await;

            let mut rescale_failures = 0;
            for version in 1..=2 {
                let value = format!("v{version}");
                let acks: Vec<_> = (0..RECORDS_PER_PARTITION)
                    .map(|a| writer.upsert(&row(a, &value, "later")).expect("upsert"))
                    .collect();
                writer.flush().await.expect("flush");
                for (a, ack) in acks.into_iter().enumerate() {
                    if let Err(e) = ack.await {
                        assert_eq!(e.api_error(), Some(FlussError::InvalidBucketRouting));
                        rescale_failures += 1;
                        writer
                            .upsert(&row(a as i32, &value, "later"))
                            .expect("upsert")
                            .await
                            .expect("retried upsert");
                    }
                }
            }
            assert!(rescale_failures > 0);

            let partition_infos = admin
                .list_partition_infos(&table_path)
                .await
                .expect("list partitions");
            assert_eq!(
                bucket_count_by_name(&partition_infos)["later"],
                new_bucket_num
            );

            let mut lookuper = table
                .new_lookup()
                .expect("lookup")
                .create_lookuper()
                .expect("lookuper");
            for a in 0..RECORDS_PER_PARTITION {
                let result = lookuper.lookup(&key(a, "later")).await.expect("lookup");
                let row = result.get_single_row().expect("row").expect("present");
                assert_eq!(row.get_string(1).unwrap(), "v2");
            }

            admin.drop_table(&table_path, false).await.expect("drop");
        }
    }

    #[tokio::test]
    async fn prefix_lookup_across_partitions_with_different_bucket_counts() {
        let cluster = get_shared_cluster();
        let admin_connection = cluster.get_fluss_connection().await;
        let admin = admin_connection.get_admin().expect("admin");
        let table_path = TablePath::new("fluss", "test_rescale_prefix_lookup");
        create_partitioned_table(&admin, &table_path, pk_schema(&["a", "b", "c"]), &["a"]).await;
        setup_old_new_partitions(&admin, &table_path).await;

        let a_cardinality = 8;
        let b_per_a = 3;
        let connection = cluster.get_fluss_connection().await;
        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_upsert()
            .expect("upsert")
            .create_writer()
            .expect("writer");
        let mut acks = Vec::new();
        for partition in PARTITIONS {
            for a in 0..a_cardinality {
                for b in 0..b_per_a {
                    acks.push(
                        writer
                            .upsert(&row(a, &format!("b{b}"), partition))
                            .expect("upsert"),
                    );
                }
            }
        }
        writer.flush().await.expect("flush");
        for ack in acks {
            ack.await.expect("upsert ack");
        }

        let mut lookuper = table
            .new_lookup()
            .expect("lookup")
            .lookup_by(vec!["a".to_string(), "c".to_string()])
            .create_lookuper()
            .expect("prefix lookuper");
        for partition in PARTITIONS {
            for a in 0..a_cardinality {
                let mut prefix = GenericRow::new(2);
                prefix.set_field(0, a);
                prefix.set_field(1, partition);
                let result = lookuper.lookup(&prefix).await.expect("prefix lookup");
                let rows = result.get_rows().expect("rows");
                assert_eq!(rows.len(), b_per_a, "prefix ({a}, {partition})");
            }
        }

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    #[tokio::test]
    async fn same_value_bucket_num_alter_is_no_op() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");
        let table_path = TablePath::new("fluss", "test_rescale_same_value_alter");
        create_partitioned_table(&admin, &table_path, log_schema(), &[]).await;

        let before = admin.get_table_info(&table_path).await.expect("table info");
        assert_eq!(before.num_buckets, OLD_BUCKET_NUM);

        alter_bucket_num(&admin, &table_path, OLD_BUCKET_NUM).await;
        let same_valued = admin.get_table_info(&table_path).await.expect("table info");
        assert_eq!(same_valued.num_buckets, OLD_BUCKET_NUM);
        assert_eq!(
            same_valued.get_bucket_count_epoch(),
            before.get_bucket_count_epoch()
        );

        alter_bucket_num(&admin, &table_path, NEW_BUCKET_NUM).await;
        let rescaled = admin.get_table_info(&table_path).await.expect("table info");
        assert_eq!(rescaled.num_buckets, NEW_BUCKET_NUM);
        assert!(rescaled.get_bucket_count_epoch() > before.get_bucket_count_epoch());

        alter_bucket_num(&admin, &table_path, NEW_BUCKET_NUM).await;
        let after = admin.get_table_info(&table_path).await.expect("table info");
        assert_eq!(after.num_buckets, NEW_BUCKET_NUM);
        assert_eq!(
            after.get_bucket_count_epoch(),
            rescaled.get_bucket_count_epoch()
        );

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    #[tokio::test]
    async fn bucket_count_change_cannot_be_mixed_with_property_change() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");
        let table_path = TablePath::new("fluss", "test_rescale_mixed_alter");
        create_partitioned_table(&admin, &table_path, log_schema(), &[]).await;

        let error = admin
            .alter_table(
                &table_path,
                false,
                AlterTableChanges {
                    new_bucket_count: Some(NEW_BUCKET_NUM),
                    config_changes: vec![AlterConfig::new(
                        "custom-key",
                        Some("value".to_string()),
                        AlterConfigOpType::Set,
                    )],
                    ..Default::default()
                },
            )
            .await
            .expect_err("mixed alter");
        assert_eq!(
            error.api_error(),
            Some(FlussError::InvalidAlterTableException)
        );
        assert!(
            error
                .to_string()
                .contains("table properties, table schema, or table distribution"),
            "{error}"
        );
        assert_eq!(
            admin
                .get_table_info(&table_path)
                .await
                .expect("table info")
                .num_buckets,
            OLD_BUCKET_NUM
        );

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    /// A handle opened before the change reads every bucket of a partition created after it.
    #[tokio::test]
    async fn limit_scan_through_a_stale_handle_checks_the_partition_bucket_count() {
        let cluster = get_shared_cluster();
        let admin_connection = cluster.get_fluss_connection().await;
        let admin = admin_connection.get_admin().expect("admin");
        let table_path = TablePath::new("fluss", "test_rescale_stale_limit_scan");
        create_partitioned_table(&admin, &table_path, pk_schema(&["a", "c"]), &[]).await;

        let stale_connection = cluster.get_fluss_connection().await;
        let stale_table = stale_connection
            .get_table(&table_path)
            .await
            .expect("table");
        alter_bucket_num(&admin, &table_path, NEW_BUCKET_NUM).await;
        create_partition(&admin, &table_path, "later").await;
        wait_for_partitions_ready(&admin, &table_path, &["later"]).await;

        let table = admin_connection
            .get_table(&table_path)
            .await
            .expect("table");
        let writer = table
            .new_upsert()
            .expect("upsert")
            .create_writer()
            .expect("writer");
        for a in 0..RECORDS_PER_PARTITION {
            writer.upsert(&row(a, "v", "later")).expect("upsert");
        }
        writer.flush().await.expect("flush");

        let table_id = stale_table.get_table_info().table_id;
        let partition_id = partition_id(
            &admin
                .list_partition_infos(&table_path)
                .await
                .expect("partitions"),
            "later",
        );
        let limit_scan = |bucket| {
            stale_table
                .new_scan()
                .limit(RECORDS_PER_PARTITION)
                .expect("limit")
                .create_bucket_batch_scanner(TableBucket::new_with_partition(
                    table_id,
                    Some(partition_id),
                    bucket,
                ))
                .expect("batch scanner")
        };

        // The stale connection has not cached the partition yet, so the scan checks the range
        // once it fetches the partition's metadata.
        let error = limit_scan(NEW_BUCKET_NUM)
            .collect_all_batches()
            .await
            .expect_err("bucket outside the partition");
        assert!(
            matches!(&error, Error::IllegalArgument { message } if message.contains("out of range")),
            "{error}"
        );

        let mut rows = 0;
        for bucket in 0..NEW_BUCKET_NUM {
            let batches = limit_scan(bucket)
                .collect_all_batches()
                .await
                .expect("limit scan");
            rows += batches.iter().map(|b| b.batch().num_rows()).sum::<usize>();
        }
        assert_eq!(rows, RECORDS_PER_PARTITION as usize);

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    #[tokio::test]
    async fn bounded_reader_checks_the_partition_bucket_count() {
        let cluster = get_shared_cluster();
        let admin_connection = cluster.get_fluss_connection().await;
        let admin = admin_connection.get_admin().expect("admin");
        let table_path = TablePath::new("fluss", "test_rescale_bounded_reader");
        create_partitioned_table(&admin, &table_path, log_schema(), &[]).await;

        let stale_connection = cluster.get_fluss_connection().await;
        let stale_table = stale_connection
            .get_table(&table_path)
            .await
            .expect("table");
        alter_bucket_num(&admin, &table_path, NEW_BUCKET_NUM).await;
        create_partition(&admin, &table_path, "later").await;
        wait_for_partitions_ready(&admin, &table_path, &["later"]).await;

        let table_id = stale_table.get_table_info().table_id;
        let partition_id = partition_id(
            &admin
                .list_partition_infos(&table_path)
                .await
                .expect("partitions"),
            "later",
        );
        let range = |bucket| BoundedLogReadRange {
            bucket: TableBucket::new_with_partition(table_id, Some(partition_id), bucket),
            starting_offset: 0,
            stopping_offset: 0,
        };

        let stale_scanner = stale_table
            .new_scan()
            .create_record_batch_log_scanner()
            .expect("scanner");
        RecordBatchLogReader::new_from_ranges(stale_scanner, vec![range(NEW_BUCKET_NUM - 1)])
            .await
            .expect("a valid bucket of the later partition");

        let fresh_connection = cluster.get_fluss_connection().await;
        let fresh_scanner = fresh_connection
            .get_table(&table_path)
            .await
            .expect("table")
            .new_scan()
            .create_record_batch_log_scanner()
            .expect("scanner");
        let error =
            RecordBatchLogReader::new_from_ranges(fresh_scanner, vec![range(NEW_BUCKET_NUM)])
                .await
                .err()
                .expect("bucket outside the partition");
        assert!(matches!(error, Error::IllegalArgument { .. }), "{error}");

        admin.drop_table(&table_path, false).await.expect("drop");
    }
}
