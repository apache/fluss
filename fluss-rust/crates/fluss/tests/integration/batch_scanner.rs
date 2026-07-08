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
mod batch_scanner_test {
    use crate::integration::utils::{create_table, get_shared_cluster};
    use arrow::array::{Int32Array, Int64Array, StringArray, record_batch};
    use fluss::metadata::{DataTypes, LogFormat, Schema, TableBucket, TableDescriptor, TablePath};
    use fluss::row::GenericRow;
    use std::collections::HashMap;

    /// End-to-end check that the scanner yields the appended rows once and then
    /// `None`, honoring the configured limit.
    #[tokio::test]
    async fn batch_scanner_returns_appended_rows_then_none() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_batch_scanner_log");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .build()
                    .expect("schema"),
            )
            // Single bucket so a single BatchScanner sees every row.
            .distributed_by(Some(1), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        let batch = record_batch!(
            ("c1", Int32, [1, 2, 3, 4, 5]),
            ("c2", Utf8, ["a", "b", "c", "d", "e"])
        )
        .unwrap();
        writer.append_arrow_batch(batch).expect("append batch");
        writer.flush().await.expect("flush");

        let table_info = table.get_table_info();
        let bucket = TableBucket::new(table_info.table_id, 0);

        let mut scanner = table
            .new_scan()
            .limit(3)
            .expect("limit")
            .create_bucket_batch_scanner(bucket.clone())
            .expect("create batch scanner");

        let first = scanner
            .next_batch()
            .await
            .expect("poll")
            .expect("first batch should be Some");

        assert_eq!(first.bucket(), &bucket);
        // The server may return fewer rows than the limit on the first call,
        // but must never exceed it.
        assert!(
            first.num_records() > 0 && first.num_records() <= 3,
            "expected 1..=3 records, got {}",
            first.num_records()
        );

        assert!(
            scanner.next_batch().await.expect("poll").is_none(),
            "scanner must end after one batch"
        );
    }

    /// End-to-end projection skipping the middle `c2` string column.
    #[tokio::test]
    async fn batch_scanner_projects_non_contiguous_columns() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_batch_scanner_projection");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .column("c3", DataTypes::bigint())
                    .build()
                    .expect("schema"),
            )
            // Single bucket so a single BatchScanner sees every row.
            .distributed_by(Some(1), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        let batch = record_batch!(
            ("c1", Int32, [1, 2, 3]),
            ("c2", Utf8, ["a", "b", "c"]),
            ("c3", Int64, [100, 200, 300])
        )
        .unwrap();
        writer.append_arrow_batch(batch).expect("append batch");
        writer.flush().await.expect("flush");

        let table_info = table.get_table_info();
        let bucket = TableBucket::new(table_info.table_id, 0);

        let mut scanner = table
            .new_scan()
            .project(&[0, 2])
            .expect("project")
            .limit(10)
            .expect("limit")
            .create_bucket_batch_scanner(bucket.clone())
            .expect("create batch scanner");

        let first = scanner
            .next_batch()
            .await
            .expect("poll")
            .expect("first batch should be Some");

        let rows = first.batch();
        assert_eq!(rows.num_columns(), 2, "projected to c1 + c3");
        assert_eq!(rows.schema().field(0).name(), "c1");
        assert_eq!(rows.schema().field(1).name(), "c3");

        let c1 = rows
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("c1 Int32");
        let c3 = rows
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c3 Int64");
        // Every (c1, c3) pair must match what we appended (c2 is dropped).
        let expected: HashMap<i32, i64> = [(1, 100), (2, 200), (3, 300)].into();
        for i in 0..rows.num_rows() {
            assert_eq!(
                expected.get(&c1.value(i)),
                Some(&c3.value(i)),
                "projected row ({}, {}) does not match appended data",
                c1.value(i),
                c3.value(i)
            );
        }
    }

    /// Limit scan on a primary-key table: decodes the value-record batch and
    /// honors the limit. Exercises the KV wire path (distinct from the log one).
    #[tokio::test]
    async fn batch_scanner_reads_primary_key_table() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_batch_scanner_pk");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .primary_key(vec!["id"])
                    .build()
                    .expect("schema"),
            )
            // Single bucket so one BatchScanner sees every row.
            .distributed_by(Some(1), vec!["id".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_upsert()
            .expect("upsert")
            .create_writer()
            .expect("writer");

        let expected: HashMap<i32, &str> =
            [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")].into();
        for (id, name) in &expected {
            let mut row = GenericRow::new(2);
            row.set_field(0, *id);
            row.set_field(1, *name);
            writer.upsert(&row).expect("upsert row");
        }
        writer.flush().await.expect("flush");

        let table_info = table.get_table_info();
        let bucket = TableBucket::new(table_info.table_id, 0);

        let mut scanner = table
            .new_scan()
            .limit(3)
            .expect("limit")
            .create_bucket_batch_scanner(bucket.clone())
            .expect("create batch scanner");

        let first = scanner
            .next_batch()
            .await
            .expect("poll")
            .expect("first batch should be Some");

        assert_eq!(first.bucket(), &bucket);
        let rows = first.batch();
        assert_eq!(rows.num_columns(), 2, "id + name");
        assert!(
            rows.num_rows() > 0 && rows.num_rows() <= 3,
            "expected 1..=3 records, got {}",
            rows.num_rows()
        );

        // Every returned (id, name) must match what we upserted.
        let ids = rows
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column Int32");
        let names = rows
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column Utf8");
        for i in 0..rows.num_rows() {
            let id = ids.value(i);
            let name = names.value(i);
            assert_eq!(
                expected.get(&id),
                Some(&name),
                "decoded row ({id}, {name}) does not match upserted data"
            );
        }

        assert!(
            scanner.next_batch().await.expect("poll").is_none(),
            "scanner must end after one batch"
        );
    }

    /// A bucket with the wrong table_id or an out-of-range bucket_id must be
    /// rejected before any RPC is made.
    #[tokio::test]
    async fn batch_scanner_rejects_invalid_bucket() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_batch_scanner_table_id");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(1), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let table_id = table.get_table_info().table_id;

        // Wrong table_id.
        assert!(
            table
                .new_scan()
                .limit(1)
                .expect("limit")
                .create_bucket_batch_scanner(TableBucket::new(table_id + 9999, 0))
                .is_err(),
            "must reject mismatched table_id"
        );

        // Bucket id past the single bucket of this table.
        assert!(
            table
                .new_scan()
                .limit(1)
                .expect("limit")
                .create_bucket_batch_scanner(TableBucket::new(table_id, 99))
                .is_err(),
            "must reject out-of-range bucket_id"
        );
    }

    /// A limit scan over a non-ARROW log table must be rejected (the log path
    /// decodes Arrow IPC).
    #[tokio::test]
    async fn batch_scanner_rejects_non_arrow_log_format() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_batch_scanner_indexed");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .build()
                    .expect("schema"),
            )
            .log_format(LogFormat::INDEXED)
            .distributed_by(Some(1), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let bucket = TableBucket::new(table.get_table_info().table_id, 0);

        assert!(
            table
                .new_scan()
                .limit(1)
                .expect("limit")
                .create_bucket_batch_scanner(bucket)
                .is_err(),
            "must reject INDEXED log format"
        );
    }

    /// `.limit(n)` must reject non-positive values before any scanner is built.
    #[tokio::test]
    async fn batch_scanner_rejects_non_positive_limit() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_batch_scanner_bad_limit");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(1), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        assert!(table.new_scan().limit(0).is_err());
        assert!(table.new_scan().limit(-5).is_err());
    }

    /// A configured limit must be rejected by the log scanners rather than
    /// silently ignored.
    #[tokio::test]
    async fn limit_is_rejected_by_log_scanners() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_batch_scanner_limit_logscan");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(1), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        assert!(
            table
                .new_scan()
                .limit(5)
                .expect("limit")
                .create_log_scanner()
                .is_err(),
            "create_log_scanner must reject a configured limit"
        );
        assert!(
            table
                .new_scan()
                .limit(5)
                .expect("limit")
                .create_record_batch_log_scanner()
                .is_err(),
            "create_record_batch_log_scanner must reject a configured limit"
        );
    }
}
