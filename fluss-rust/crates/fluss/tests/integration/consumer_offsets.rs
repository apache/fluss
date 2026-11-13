// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use fluss::client::{
    ConsumerGroupOptions, FlussConnection, OffsetAndMetadata, OffsetCommitOutcome,
    OffsetsInitializer,
};
use fluss::config::Config;
use fluss::metadata::{DataTypes, PartitionSpec, Schema, TableBucket, TableDescriptor, TablePath};
use fluss::row::{DataGetters, GenericRow};
use std::collections::HashMap;
use std::time::Duration;

fn success(result: fluss::client::OffsetCommitResult) {
    assert!(!result.is_empty());
    assert!(
        result
            .values()
            .all(|outcome| matches!(outcome, OffsetCommitOutcome::Committed)),
        "{result:?}"
    );
}

async fn connection() -> FlussConnection {
    FlussConnection::new(Config {
        bootstrap_servers: std::env::var("FLUSS_BOOTSTRAP_SERVERS").unwrap_or_else(|_| {
            super::utils::get_shared_cluster()
                .plaintext_bootstrap_servers()
                .to_owned()
        }),
        scanner_log_max_poll_records: 2,
        ..Config::default()
    })
    .await
    .unwrap()
}

fn table_path() -> TablePath {
    TablePath::new(
        "fluss",
        format!("consumer_offsets_{}", uuid::Uuid::new_v4().simple()),
    )
}

async fn log_table(connection: &FlussConnection) -> (TablePath, TableBucket) {
    let path = table_path();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .build()
                .unwrap(),
        )
        .distributed_by(Some(1), vec![])
        .build()
        .unwrap();
    connection
        .get_admin()
        .unwrap()
        .create_table(&path, &descriptor, false)
        .await
        .unwrap();
    let table = connection.get_table(&path).await.unwrap();
    let writer = table.new_append().unwrap().create_writer().unwrap();
    for id in 0..8 {
        let mut row = GenericRow::new(1);
        row.set_field(0, id);
        writer.append(&row).unwrap();
    }
    writer.flush().await.unwrap();
    (path, TableBucket::new(table.get_table_info().table_id, 0))
}

#[tokio::test]
async fn explicit_and_polled_offsets_restore_without_committing_prefetch() {
    let connection = connection().await;
    let admin = connection.get_admin().unwrap();
    let (path, bucket) = log_table(&connection).await;
    let table = connection.get_table(&path).await.unwrap();
    let group = format!("group-{}", bucket.table_id());
    let buckets = std::slice::from_ref(&bucket);
    let scanner = table
        .new_scan()
        .with_consumer_group(ConsumerGroupOptions::new(&group))
        .unwrap()
        .create_log_scanner()
        .unwrap();
    scanner
        .subscribe_with_initializer(buckets, &OffsetsInitializer::committed(&group))
        .await
        .unwrap();
    let records = scanner.poll(Duration::from_secs(10)).await.unwrap();
    assert_eq!(
        records
            .records(&bucket)
            .iter()
            .map(|r| (r.offset(), r.row().get_int(0).unwrap()))
            .collect::<Vec<_>>(),
        vec![(0, 0), (1, 1)]
    );
    assert_eq!(
        admin.get_consumer_offsets(&group, buckets).await.unwrap()[&bucket],
        None
    );
    // Only the first returned record has been processed.
    success(
        scanner
            .commit_offsets_sync(HashMap::from([(bucket.clone(), OffsetAndMetadata::new(1))]))
            .await
            .unwrap(),
    );
    drop(scanner);
    let restored = table
        .new_scan()
        .with_consumer_group(ConsumerGroupOptions::new(&group))
        .unwrap()
        .create_log_scanner()
        .unwrap();
    restored
        .subscribe_with_initializer(
            buckets,
            &OffsetsInitializer::committed_with_fallback(&group, OffsetsInitializer::fail()),
        )
        .await
        .unwrap();
    let records = restored.poll(Duration::from_secs(10)).await.unwrap();
    assert_eq!(
        records
            .records(&bucket)
            .iter()
            .map(|r| (r.offset(), r.row().get_int(0).unwrap()))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 2)]
    );
    success(restored.commit_sync().await.unwrap());
    // Eight records exist, but prefetch must not move the saved position past this poll.
    assert_eq!(
        admin.get_consumer_offsets(&group, buckets).await.unwrap()[&bucket],
        Some(OffsetAndMetadata::new(3))
    );
    let (other_path, other_bucket) = log_table(&connection).await;
    let saved = admin
        .get_consumer_offsets(&group, &[bucket.clone(), other_bucket.clone()])
        .await
        .unwrap();
    assert_eq!(saved[&bucket], Some(OffsetAndMetadata::new(3)));
    assert_eq!(saved[&other_bucket], None);
    admin.drop_table(&other_path, false).await.unwrap();
    admin.drop_table(&path, false).await.unwrap();
    connection.close(Duration::from_secs(10)).await.unwrap();
}

#[tokio::test]
async fn partition_commits_keep_partial_failures_and_missing_positions_distinct() {
    let connection = connection().await;
    let admin = connection.get_admin().unwrap();
    let path = table_path();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("region", DataTypes::string())
                .build()
                .unwrap(),
        )
        .distributed_by(Some(1), vec![])
        .partitioned_by(vec!["region"])
        .build()
        .unwrap();
    admin.create_table(&path, &descriptor, false).await.unwrap();
    for region in ["a", "b"] {
        admin
            .create_partition(
                &path,
                &PartitionSpec::new(HashMap::from([("region", region)])),
                false,
            )
            .await
            .unwrap();
    }
    let table = connection.get_table(&path).await.unwrap();
    let buckets: Vec<_> = admin
        .list_partition_infos(&path)
        .await
        .unwrap()
        .iter()
        .map(|p| {
            TableBucket::new_with_partition(
                table.get_table_info().table_id,
                Some(p.get_partition_id()),
                0,
            )
        })
        .collect();
    let group = format!("group-{}", table.get_table_info().table_id);
    let scanner = table
        .new_scan()
        .with_consumer_group(ConsumerGroupOptions::new(&group))
        .unwrap()
        .create_record_batch_log_scanner()
        .unwrap();
    let first = OffsetAndMetadata {
        offset: 10,
        metadata: Some("中文".into()),
    };
    let result = scanner
        .commit_offsets_sync(HashMap::from([
            (buckets[0].clone(), first.clone()),
            (
                buckets[1].clone(),
                OffsetAndMetadata {
                    offset: 20,
                    metadata: Some("超".repeat(2000)),
                },
            ),
        ]))
        .await
        .unwrap();
    assert!(matches!(
        result[&buckets[0]],
        OffsetCommitOutcome::Committed
    ));
    assert!(
        matches!(result[&buckets[1]], OffsetCommitOutcome::Rejected(ref error) if error.code == 80)
    );
    let saved = admin.get_consumer_offsets(&group, &buckets).await.unwrap();
    assert_eq!(saved[&buckets[0]], Some(first.clone()));
    assert_eq!(saved[&buckets[1]], None);
    scanner
        .subscribe_with_initializer(&buckets, &OffsetsInitializer::offset(3))
        .await
        .unwrap();
    assert!(
        scanner
            .subscribe_with_initializer(
                &buckets,
                &OffsetsInitializer::committed_with_fallback(&group, OffsetsInitializer::fail())
            )
            .await
            .is_err()
    );
    assert!(
        scanner
            .get_subscribed_buckets()
            .iter()
            .all(|(_, offset)| *offset == 3)
    );
    scanner
        .subscribe_with_initializer(
            &buckets,
            &OffsetsInitializer::committed_with_fallback(&group, OffsetsInitializer::latest()),
        )
        .await
        .unwrap();
    let starts: HashMap<_, _> = scanner.get_subscribed_buckets().into_iter().collect();
    assert_eq!(starts[&buckets[0]], 10);
    assert_eq!(starts[&buckets[1]], 0);
    let second = OffsetAndMetadata {
        offset: 2,
        metadata: Some(String::new()),
    };
    success(
        scanner
            .commit_offsets_sync(HashMap::from([(buckets[1].clone(), second.clone())]))
            .await
            .unwrap(),
    );
    let saved = admin.get_consumer_offsets(&group, &buckets).await.unwrap();
    assert_eq!(saved[&buckets[0]], Some(first));
    assert_eq!(saved[&buckets[1]], Some(second));
    assert!(
        admin
            .get_consumer_offsets(&format!("other-{group}"), &buckets)
            .await
            .unwrap()
            .values()
            .all(Option::is_none)
    );
    admin.drop_table(&path, false).await.unwrap();
    connection.close(Duration::from_secs(10)).await.unwrap();
}

#[tokio::test]
async fn arrow_commits_track_delivered_batches_and_forget_progress_after_seek() {
    let connection = connection().await;
    let admin = connection.get_admin().unwrap();
    let (path, bucket) = log_table(&connection).await;
    let table = connection.get_table(&path).await.unwrap();
    let group = format!("group-{}", bucket.table_id());
    let buckets = std::slice::from_ref(&bucket);
    let scanner = table
        .new_scan()
        .with_consumer_group(ConsumerGroupOptions::new(&group))
        .unwrap()
        .create_record_batch_log_scanner()
        .unwrap();
    scanner
        .subscribe_with_initializer(buckets, &OffsetsInitializer::committed(&group))
        .await
        .unwrap();
    let batches = scanner.poll(Duration::from_secs(10)).await.unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch().num_rows())
            .sum::<usize>(),
        8
    );
    success(scanner.commit_sync().await.unwrap());
    assert_eq!(
        admin.get_consumer_offsets(&group, buckets).await.unwrap()[&bucket],
        Some(OffsetAndMetadata::new(8))
    );
    scanner.subscribe(0, 0).await.unwrap();
    assert!(scanner.commit_sync().await.unwrap().is_empty());
    assert_eq!(
        admin.get_consumer_offsets(&group, buckets).await.unwrap()[&bucket],
        Some(OffsetAndMetadata::new(8))
    );
    admin.drop_table(&path, false).await.unwrap();
    connection.close(Duration::from_secs(10)).await.unwrap();
}
