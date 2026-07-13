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
mod kv_changelog_test {
    use crate::integration::utils::{create_table, get_shared_cluster};
    use fluss::client::EARLIEST_OFFSET;
    use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::record::{ChangeType, ScanRecord};
    use fluss::row::{DataGetters, GenericRow};
    use std::time::Duration;

    /// Subscribing to a primary-key (KV) table yields its CDC changelog.
    ///
    /// With the default `FULL` changelog image: upserting a new key emits `+I`,
    /// overwriting an existing key emits `-U` (old image) then `+U` (new image),
    /// and a delete emits `-D` (old image). A single bucket keeps the changelog
    /// offsets contiguous so the sequence is deterministic. This exercises the
    /// per-record change-type vector decode on a real cluster end to end.
    #[tokio::test]
    async fn subscribe_kv_table_changelog() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_kv_changelog_subscribe");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .primary_key(vec!["id"])
                    .build()
                    .expect("Failed to build schema"),
            )
            .distributed_by(Some(1), vec!["id".to_string()])
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let upsert_writer = table
            .new_upsert()
            .expect("Failed to create upsert")
            .create_writer()
            .expect("Failed to create writer");

        let make_row = |id: i32, name: &'static str| {
            let mut row = GenericRow::new(2);
            row.set_field(0, id);
            row.set_field(1, name);
            row
        };

        // Await each write so the changelog offsets are produced in a fixed order.
        // +I (1, "alice")
        upsert_writer
            .upsert(&make_row(1, "alice"))
            .expect("upsert (1, alice)")
            .await
            .expect("ack (1, alice)");
        // +I (2, "bob")
        upsert_writer
            .upsert(&make_row(2, "bob"))
            .expect("upsert (2, bob)")
            .await
            .expect("ack (2, bob)");
        // Overwrite id=1 -> -U (1, "alice") then +U (1, "alice2")
        upsert_writer
            .upsert(&make_row(1, "alice2"))
            .expect("update (1, alice2)")
            .await
            .expect("ack (1, alice2)");
        // -D (2, "bob")
        let mut delete_key = GenericRow::new(2);
        delete_key.set_field(0, 2);
        upsert_writer
            .delete(&delete_key)
            .expect("delete (2)")
            .await
            .expect("ack delete (2)");

        // Subscribe to the single bucket from the earliest changelog offset.
        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe to bucket 0");

        // Poll until the whole changelog is collected (or time out).
        let mut records: Vec<ScanRecord> = Vec::new();
        let start = std::time::Instant::now();
        while records.len() < 5 && start.elapsed() < Duration::from_secs(15) {
            let polled = log_scanner
                .poll(Duration::from_millis(500))
                .await
                .expect("Failed to poll changelog");
            records.extend(polled);
        }

        assert_eq!(records.len(), 5, "expected 5 changelog records");

        let change_types: Vec<ChangeType> = records.iter().map(|r| *r.change_type()).collect();
        assert_eq!(
            change_types,
            vec![
                ChangeType::Insert,
                ChangeType::Insert,
                ChangeType::UpdateBefore,
                ChangeType::UpdateAfter,
                ChangeType::Delete,
            ],
            "unexpected change-type sequence"
        );

        // Single bucket -> contiguous, in-order offsets.
        let offsets: Vec<i64> = records.iter().map(|r| r.offset()).collect();
        assert_eq!(offsets, vec![0, 1, 2, 3, 4]);

        // Each record carries the correct before/after row image.
        let rows: Vec<(i32, String)> = records
            .iter()
            .map(|r| {
                let row = r.row();
                (
                    row.get_int(0).unwrap(),
                    row.get_string(1).unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                (1, "alice".to_string()),  // +I
                (2, "bob".to_string()),    // +I
                (1, "alice".to_string()),  // -U (old image)
                (1, "alice2".to_string()), // +U (new image)
                (2, "bob".to_string()),    // -D (old image)
            ]
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }
}
