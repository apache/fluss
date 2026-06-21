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
mod table_remote_scan_test {
    use crate::integration::utils::{create_table, get_shared_cluster};
    use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::row::{GenericRow, InternalRow};
    use std::time::Duration;

    #[tokio::test]
    async fn test_scan_remote_log() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_scan_remote_log");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
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

        let append_writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        // append 20 rows, there must be some tiered to remote
        let record_count = 20;
        for i in 0..record_count {
            let mut row = GenericRow::new(2);
            row.set_field(0, i as i32);
            let v = format!("v{}", i);
            row.set_field(1, v.as_str());
            append_writer.append(&row).expect("Failed to append row");
        }

        append_writer.flush().await.expect("Failed to flush");

        // Create a log scanner and subscribe to all buckets to read appended records
        let num_buckets = table.get_table_info().get_num_buckets();
        let log_scanner = table
            .new_scan()
            .project(&[1, 0])
            .unwrap()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        for bucket_id in 0..num_buckets {
            log_scanner
                .subscribe(bucket_id, 0)
                .await
                .expect("Failed to subscribe");
        }

        let mut records = Vec::with_capacity(record_count);
        let start = std::time::Instant::now();
        const MAX_WAIT_DURATION: Duration = Duration::from_secs(60);
        while records.len() < record_count {
            if start.elapsed() > MAX_WAIT_DURATION {
                panic!(
                    "Timed out waiting for {} records; only got {} after {:?}",
                    record_count,
                    records.len(),
                    start.elapsed()
                );
            }
            let scan_records = log_scanner
                .poll(Duration::from_secs(1))
                .await
                .expect("Failed to poll log scanner");
            records.extend(scan_records);
        }

        // then, check the data
        for (i, record) in records.iter().enumerate() {
            let row = record.row();
            let expected_c1 = i as i32;
            let expected_c2 = format!("v{}", i);
            assert_eq!(
                row.get_int(1).unwrap(),
                expected_c1,
                "c1 mismatch at index {}",
                i
            );
            assert_eq!(
                row.get_string(0).unwrap(),
                expected_c2,
                "c2 mismatch at index {}",
                i
            );
        }
    }
}
