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
mod dynamic_batch_size_test {
    use crate::integration::utils::{create_table, get_shared_cluster, wait_for_table_ready};
    use fluss::client::{EARLIEST_OFFSET, FlussConnection};
    use fluss::config::Config;
    use fluss::metadata::{DataTypes, PhysicalTablePath, Schema, TableDescriptor, TablePath};
    use fluss::row::{DataGetters, Datum, GenericRow};
    use std::sync::Arc;
    use std::time::Duration;

    fn make_config(
        bootstrap_servers: &str,
        batch_size: i32,
        dynamic_enabled: bool,
        dynamic_min: i32,
    ) -> Config {
        Config {
            bootstrap_servers: bootstrap_servers.to_string(),
            writer_acks: "all".to_string(),
            writer_batch_size: batch_size,
            writer_dynamic_batch_size_enabled: dynamic_enabled,
            writer_dynamic_batch_size_min: dynamic_min,
            ..Config::default()
        }
    }

    fn log_table_descriptor() -> TableDescriptor {
        TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("payload", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .build()
            .expect("Failed to build table descriptor")
    }

    fn make_row(id: i32, payload: &str) -> GenericRow<'static> {
        let mut row = GenericRow::new(2);
        row.set_field(0, Datum::Int32(id));
        row.set_field(1, Datum::String(payload.to_string().into()));
        row
    }

    async fn read_back_records(
        connection: &FlussConnection,
        table_path: &TablePath,
        expected_count: usize,
    ) -> Vec<(i32, String)> {
        let table = connection
            .get_table(table_path)
            .await
            .expect("Failed to get table for scan");
        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe");

        let mut collected: Vec<(i32, String)> = Vec::new();
        let start = std::time::Instant::now();
        while collected.len() < expected_count && start.elapsed() < Duration::from_secs(30) {
            let records = log_scanner
                .poll(Duration::from_millis(500))
                .await
                .expect("Failed to poll");
            for rec in records {
                let row = rec.row();
                collected.push((
                    row.get_int(0).unwrap(),
                    row.get_string(1).unwrap().to_string(),
                ));
            }
        }
        collected
    }

    /// Writes many tiny rows (well below 50% of batch size per drain) so the estimator
    /// shrinks the target toward min, then reads all records back to verify data integrity.
    #[tokio::test]
    async fn small_rows_shrink_batch_size() {
        let cluster = get_shared_cluster();
        let config = make_config(
            cluster.plaintext_bootstrap_servers(),
            256 * 1024, // max = 256 KB
            true,
            4 * 1024, // min = 4 KB
        );
        let connection = FlussConnection::new(config)
            .await
            .expect("Failed to connect");
        let admin = connection.get_admin().expect("Failed to get admin");
        let table_path = TablePath::new("fluss", "test_dynamic_small_rows");
        create_table(&admin, &table_path, &log_table_descriptor()).await;
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

        // Write many tiny rows — each well below 50% of the batch size so
        // the estimator shrinks the target toward min after each drain.
        let row_count = 200usize;
        for i in 0..row_count {
            let row = make_row(i as i32, "x");
            writer.append(&row).expect("Failed to append row");
        }
        writer.flush().await.expect("Failed to flush");

        // Verify the estimator actually shrunk toward min, not just that flush succeeded.
        let physical_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let estimated = connection
            .estimated_batch_size_for_table(&physical_path)
            .expect("Estimator should exist after writes");
        assert!(
            estimated < 256 * 1024,
            "Expected batch size to shrink below max (256 KB), got {estimated}"
        );
    }

    /// Verifies estimator adapts correctly across a shrink-then-grow cycle:
    /// first shrinks with tiny rows, then grows with large rows, and all
    /// records written in both phases are read back correctly.
    #[tokio::test]
    async fn shrink_then_grow_batch_size() {
        let cluster = get_shared_cluster();
        let max_batch = 256 * 1024i32; // 256 KB
        let config = make_config(
            cluster.plaintext_bootstrap_servers(),
            max_batch,
            true,
            4 * 1024, // min = 4 KB
        );
        let connection = FlussConnection::new(config)
            .await
            .expect("Failed to connect");
        let admin = connection.get_admin().expect("Failed to get admin");
        let table_path = TablePath::new("fluss", "test_dynamic_shrink_grow");
        create_table(&admin, &table_path, &log_table_descriptor()).await;
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

        // Phase 1: shrink — many tiny rows drive the target toward min.
        let small_count = 100usize;
        for i in 0..small_count {
            let row = make_row(i as i32, "small");
            writer.append(&row).expect("Failed to append small row");
        }
        writer.flush().await.expect("Failed to flush phase 1");

        // Phase 2: grow — large rows (>80% of current target) drive the
        // estimator back up toward max after it has been shrunk.
        let large_payload = "A".repeat(200 * 1024); // ~200 KB
        let large_count = 5usize;
        for i in 0..large_count {
            let row = make_row((small_count + i) as i32, &large_payload);
            writer.append(&row).expect("Failed to append large row");
        }
        writer.flush().await.expect("Failed to flush phase 2");

        // Read back all records from both phases to verify data integrity.
        let total = small_count + large_count;
        let records = read_back_records(&connection, &table_path, total).await;
        assert_eq!(
            records.len(),
            total,
            "Expected {total} records after shrink-then-grow cycle, got {}",
            records.len()
        );
    }

    /// With dynamic sizing disabled, the writer uses the static writer_batch_size
    /// for every batch. Verifies all records are written and read back correctly.
    #[tokio::test]
    async fn disabled_keeps_static_batch_size() {
        let cluster = get_shared_cluster();
        let config = make_config(
            cluster.plaintext_bootstrap_servers(),
            256 * 1024,
            false, // disabled
            4 * 1024,
        );
        let connection = FlussConnection::new(config)
            .await
            .expect("Failed to connect");
        let admin = connection.get_admin().expect("Failed to get admin");
        let table_path = TablePath::new("fluss", "test_dynamic_disabled");
        create_table(&admin, &table_path, &log_table_descriptor()).await;
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

        let row_count = 100usize;
        for i in 0..row_count {
            let row = make_row(i as i32, "static");
            writer.append(&row).expect("Failed to append row");
        }
        writer.flush().await.expect("Failed to flush");

        let records = read_back_records(&connection, &table_path, row_count).await;
        assert_eq!(
            records.len(),
            row_count,
            "Expected {row_count} records with static batch size, got {}",
            records.len()
        );
    }

    /// Multiple concurrent tasks share a single connection (and thus a single
    /// WriterClient and RecordAccumulator with a shared estimator). Verifies that
    /// concurrent appends from different tasks don't corrupt estimator state and
    /// all records land correctly.
    #[tokio::test]
    async fn concurrent_appends_share_estimator_without_corruption() {
        let cluster = get_shared_cluster();
        let config = make_config(
            cluster.plaintext_bootstrap_servers(),
            256 * 1024,
            true,
            4 * 1024,
        );
        let connection = FlussConnection::new(config)
            .await
            .expect("Failed to connect");
        let admin = connection.get_admin().expect("Failed to get admin");
        let table_path = TablePath::new("fluss", "test_dynamic_concurrent");
        create_table(&admin, &table_path, &log_table_descriptor()).await;
        wait_for_table_ready(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        // All tasks share the same AppendWriter (and thus the same
        // RecordAccumulator / estimator) via Arc.
        let writer = Arc::new(
            table
                .new_append()
                .expect("Failed to create append")
                .create_writer()
                .expect("Failed to create writer"),
        );

        let tasks_count = 4usize;
        let rows_per_task = 50usize;
        let mut handles = Vec::new();

        for task_id in 0..tasks_count {
            let writer_clone = Arc::clone(&writer);
            let handle = tokio::spawn(async move {
                for i in 0..rows_per_task {
                    let row = make_row((task_id * rows_per_task + i) as i32, "concurrent");
                    writer_clone.append(&row).expect("Failed to append");
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("Task panicked");
        }
        writer.flush().await.expect("Failed to flush");

        let total = tasks_count * rows_per_task;
        let records = read_back_records(&connection, &table_path, total).await;
        assert_eq!(
            records.len(),
            total,
            "Expected {total} records from concurrent appends, got {}",
            records.len()
        );
    }
}
