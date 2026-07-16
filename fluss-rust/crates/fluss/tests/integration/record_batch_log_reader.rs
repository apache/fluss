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
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#[cfg(test)]
mod reader_test {
    use crate::integration::utils::{
        create_partitions, create_table, extract_ids_from_batches, get_shared_cluster,
        wait_for_partitions_ready,
    };
    use arrow::array::record_batch;
    use fluss::client::{EARLIEST_OFFSET, FlussConnection, RecordBatchLogReader};
    use fluss::config::{Config, NoKeyAssigner};
    use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TablePath};
    use fluss::rpc::message::OffsetSpec;
    use futures::TryStreamExt;
    use std::collections::HashMap;
    use std::time::Duration;

    #[tokio::test]
    async fn until_offsets_stops_at_explicit_offset() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_reader_until_offsets");
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
                    ("id", Int32, [1, 2, 3, 4, 5, 6]),
                    ("name", Utf8, ["a", "b", "c", "d", "e", "f"])
                )
                .unwrap(),
            )
            .expect("Failed to append batch");
        writer.flush().await.expect("Failed to flush");

        let scanner = table
            .new_scan()
            .create_record_batch_log_scanner()
            .expect("Failed to create record batch scanner");
        scanner
            .subscribe(0, 1)
            .await
            .expect("Failed to subscribe from offset 1");

        let table_id = table.get_table_info().table_id;
        let mut reader = RecordBatchLogReader::new_until_offsets(
            scanner,
            HashMap::from([(TableBucket::new(table_id, 0), 4)]),
        )
        .expect("Failed to create record batch reader");

        let batches = tokio::time::timeout(Duration::from_secs(10), reader.collect_all_batches())
            .await
            .expect("Timed out collecting bounded reader batches")
            .expect("Failed to collect bounded reader batches");

        assert_eq!(
            extract_ids_from_batches(&batches),
            vec![2, 3, 4],
            "reader should include offsets [1, 4) and stop before offset 4"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn until_offsets_with_empty_range() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_reader_until_offsets_empty_range");
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
                record_batch!(("id", Int32, [1, 2, 3]), ("name", Utf8, ["a", "b", "c"])).unwrap(),
            )
            .expect("Failed to append batch");
        writer.flush().await.expect("Failed to flush");

        let scanner = table
            .new_scan()
            .create_record_batch_log_scanner()
            .expect("Failed to create record batch scanner");
        scanner
            .subscribe(0, 1)
            .await
            .expect("Failed to subscribe from offset 1");

        let table_id = table.get_table_info().table_id;
        let mut reader = RecordBatchLogReader::new_until_offsets(
            scanner,
            HashMap::from([(TableBucket::new(table_id, 0), 1)]),
        )
        .expect("Failed to create record batch reader");

        let batches = tokio::time::timeout(Duration::from_secs(10), reader.collect_all_batches())
            .await
            .expect("Timed out collecting empty-range reader batches")
            .expect("Failed to collect empty-range reader batches");

        assert!(
            batches.is_empty(),
            "reader should return no batches when start and stop offsets are equal"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn until_offsets_past_end_of_log() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_reader_until_offsets_past_end");
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
                record_batch!(("id", Int32, [1, 2, 3]), ("name", Utf8, ["a", "b", "c"])).unwrap(),
            )
            .expect("Failed to append initial batch");
        writer.flush().await.expect("Failed to flush initial batch");

        let scanner = table
            .new_scan()
            .create_record_batch_log_scanner()
            .expect("Failed to create record batch scanner");
        scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe bucket");

        let table_id = table.get_table_info().table_id;
        let mut reader = RecordBatchLogReader::new_until_offsets(
            scanner,
            HashMap::from([(TableBucket::new(table_id, 0), 6)]),
        )
        .expect("Failed to create record batch reader");

        let collect_task = tokio::spawn(async move { reader.collect_all_batches().await });
        tokio::time::sleep(Duration::from_millis(750)).await;
        assert!(
            !collect_task.is_finished(),
            "reader should wait when the stopping offset is beyond the current log end"
        );

        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [4, 5, 6]), ("name", Utf8, ["d", "e", "f"])).unwrap(),
            )
            .expect("Failed to append follow-up batch");
        writer
            .flush()
            .await
            .expect("Failed to flush follow-up batch");

        let batches = tokio::time::timeout(Duration::from_secs(10), collect_task)
            .await
            .expect("Timed out collecting reader batches after appending past stop offset")
            .expect("Reader task panicked")
            .expect("Failed to collect reader batches");

        assert_eq!(
            extract_ids_from_batches(&batches),
            vec![1, 2, 3, 4, 5, 6],
            "reader should resume after future records arrive and stop at the requested offset"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn until_offsets_multi_bucket() {
        let cluster = get_shared_cluster();
        let connection = FlussConnection::new(Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: cluster.plaintext_bootstrap_servers().to_string(),
            writer_bucket_no_key_assigner: NoKeyAssigner::RoundRobin,
            ..Default::default()
        })
        .await
        .expect("Failed to connect with round-robin bucket assignment");
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_reader_until_offsets_multi_bucket");
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .distributed_by(Some(2), vec!["id".to_string()])
            .build()
            .expect("Failed to build table");
        create_table(&admin, &table_path, &table_descriptor).await;

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
                    ("id", Int32, [1, 2, 3, 4]),
                    ("name", Utf8, ["a", "b", "c", "d"])
                )
                .unwrap(),
            )
            .expect("Failed to append first batch");
        writer
            .append_arrow_batch(
                record_batch!(
                    ("id", Int32, [5, 6, 7, 8]),
                    ("name", Utf8, ["e", "f", "g", "h"])
                )
                .unwrap(),
            )
            .expect("Failed to append second batch");
        writer.flush().await.expect("Failed to flush");

        let latest_offsets = admin
            .list_offsets(&table_path, &[0, 1], OffsetSpec::Latest)
            .await
            .expect("Failed to list latest offsets");
        assert!(
            latest_offsets.values().all(|offset| *offset > 0),
            "test records should be distributed across both buckets: {latest_offsets:?}"
        );

        let scanner = table
            .new_scan()
            .create_record_batch_log_scanner()
            .expect("Failed to create record batch scanner");
        scanner
            .subscribe_buckets(&HashMap::from([(0, 0), (1, 0)]))
            .await
            .expect("Failed to subscribe to multiple buckets");

        let table_id = table.get_table_info().table_id;
        let stopping_offsets: HashMap<TableBucket, i64> = latest_offsets
            .into_iter()
            .map(|(bucket, offset)| (TableBucket::new(table_id, bucket), offset))
            .collect();
        assert_eq!(
            stopping_offsets.len(),
            2,
            "reader should track two stopping offsets"
        );

        let mut reader = RecordBatchLogReader::new_until_offsets(scanner, stopping_offsets)
            .expect("Failed to create record batch reader");
        let batches = tokio::time::timeout(Duration::from_secs(10), reader.collect_all_batches())
            .await
            .expect("Timed out collecting multi-bucket reader batches")
            .expect("Failed to collect multi-bucket reader batches");

        let mut ids = extract_ids_from_batches(&batches);
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 7, 8]);

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn until_latest_reads_non_partitioned_table() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_reader_non_partitioned_latest");
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
                    ("id", Int32, [1, 2, 3, 4]),
                    ("name", Utf8, ["a", "b", "c", "d"])
                )
                .unwrap(),
            )
            .expect("Failed to append batch");
        writer.flush().await.expect("Failed to flush");

        let scanner = table
            .new_scan()
            .create_record_batch_log_scanner()
            .expect("Failed to create record batch scanner");
        scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe bucket");

        let mut reader = RecordBatchLogReader::new_until_latest(scanner, &admin)
            .await
            .expect("Failed to create latest-offset reader");
        let batches = tokio::time::timeout(Duration::from_secs(10), reader.collect_all_batches())
            .await
            .expect("Timed out collecting non-partitioned reader batches")
            .expect("Failed to collect non-partitioned reader batches");

        assert_eq!(
            extract_ids_from_batches(&batches),
            vec![1, 2, 3, 4],
            "latest-offset reader should read all records present in the non-partitioned table"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn until_latest_reads_partitioned_table() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_reader_partitioned_latest");
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
        create_partitions(&admin, &table_path, "region", &["US", "EU"]).await;
        wait_for_partitions_ready(&admin, &table_path, &["US", "EU"]).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        let us_batch = record_batch!(
            ("id", Int32, [1, 2]),
            ("region", Utf8, ["US", "US"]),
            ("value", Int64, [100, 200])
        )
        .unwrap();
        writer
            .append_arrow_batch(us_batch)
            .expect("Failed to append US batch");

        let eu_batch = record_batch!(
            ("id", Int32, [3, 4]),
            ("region", Utf8, ["EU", "EU"]),
            ("value", Int64, [300, 400])
        )
        .unwrap();
        writer
            .append_arrow_batch(eu_batch)
            .expect("Failed to append EU batch");
        writer.flush().await.expect("Failed to flush");

        let scanner = table
            .new_scan()
            .create_record_batch_log_scanner()
            .expect("Failed to create record batch scanner");
        for partition in admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partition infos")
        {
            // The table uses the default single-bucket layout, so bucket 0 is
            // the only bucket in each partition. If this test switches to a
            // multi-bucket table, subscribe all buckets for each partition.
            scanner
                .subscribe_partition(partition.get_partition_id(), 0, EARLIEST_OFFSET)
                .await
                .expect("Failed to subscribe partition bucket");
        }

        let mut reader = RecordBatchLogReader::new_until_latest(scanner, &admin)
            .await
            .expect("Failed to create latest-offset reader");
        let batches = tokio::time::timeout(Duration::from_secs(10), reader.collect_all_batches())
            .await
            .expect("Timed out collecting partitioned reader batches")
            .expect("Failed to collect partitioned reader batches");

        let mut ids = extract_ids_from_batches(&batches);
        ids.sort();
        assert_eq!(
            ids,
            vec![1, 2, 3, 4],
            "latest-offset reader should read all records present in subscribed partitions"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn into_stream_yields_same_batches_as_next_batch() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_reader_into_stream");
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
                    ("name", Utf8, ["a", "b", "c", "d", "e"])
                )
                .unwrap(),
            )
            .expect("Failed to append batch");
        writer.flush().await.expect("Failed to flush");

        let scanner = table
            .new_scan()
            .create_record_batch_log_scanner()
            .expect("Failed to create record batch scanner");
        scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe bucket");

        let reader = RecordBatchLogReader::new_until_latest(scanner, &admin)
            .await
            .expect("Failed to create latest-offset reader");
        let batches = tokio::time::timeout(
            Duration::from_secs(10),
            reader.into_stream().try_collect::<Vec<_>>(),
        )
        .await
        .expect("Timed out draining reader stream")
        .expect("Failed to drain reader stream");

        assert_eq!(
            extract_ids_from_batches(&batches),
            vec![1, 2, 3, 4, 5],
            "into_stream should yield the same records next_batch would return"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }
}
