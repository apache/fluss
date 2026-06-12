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

use clap::Parser;
use fluss::client::{FlussAdmin, FlussConnection};
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, PartitionSpec, Schema, TableDescriptor, TablePath};
use fluss::row::{GenericRow, InternalRow};
use std::collections::HashMap;

#[tokio::main]
#[allow(dead_code)]
pub async fn main() -> Result<()> {
    let mut config = Config::parse();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();

    let conn = FlussConnection::new(config).await?;

    // Partitioned schema: pk is (region, user_id, session_id, event_seq),
    // `region` is the partition key, and the bucket key (user_id, session_id)
    // is a prefix of the *non-partition* portion of the primary key — which is
    // the condition for prefix lookup on a partitioned table. The lookup
    // key must include the partition column(s) in addition to the bucket
    // prefix, so we look up by (region, user_id, session_id).
    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("region", DataTypes::string())
                .column("user_id", DataTypes::int())
                .column("session_id", DataTypes::string())
                .column("event_seq", DataTypes::bigint())
                .column("event_data", DataTypes::string())
                .primary_key(vec!["region", "user_id", "session_id", "event_seq"])
                .build()?,
        )
        .partitioned_by(vec!["region"])
        .distributed_by(
            Some(3),
            vec!["user_id".to_string(), "session_id".to_string()],
        )
        .build()?;

    let table_path = TablePath::new("fluss", "rust_partitioned_prefix_lookup_example");

    let admin = conn.get_admin()?;
    admin
        .create_table(&table_path, &table_descriptor, true)
        .await?;
    println!(
        "Created partitioned KV Table:\n {}\n",
        admin.get_table_info(&table_path).await?
    );

    create_partition(&table_path, &admin, "US").await;
    create_partition(&table_path, &admin, "EU").await;

    let table = conn.get_table(&table_path).await?;
    let table_upsert = table.new_upsert()?;
    let upsert_writer = table_upsert.create_writer()?;

    println!("\n=== Upserting session events ===");
    for (region, user_id, session_id, event_seq, event_data) in [
        ("US", 1, "sess-a", 1i64, "open"),
        ("US", 1, "sess-a", 2, "click"),
        ("US", 1, "sess-a", 3, "close"),
        ("US", 2, "sess-b", 1, "open"),
        ("EU", 1, "sess-a", 1, "open"),
    ] {
        let mut row = GenericRow::new(5);
        row.set_field(0, region);
        row.set_field(1, user_id);
        row.set_field(2, session_id);
        row.set_field(3, event_seq);
        row.set_field(4, event_data);
        upsert_writer.upsert(&row)?;
        println!("Upserted: {row:?}");
    }
    upsert_writer.flush().await?;

    println!("\n=== Prefix lookup by (region, user_id, session_id) ===");
    let mut prefix_lookuper = table
        .new_lookup()?
        .lookup_by(vec![
            "region".to_string(),
            "user_id".to_string(),
            "session_id".to_string(),
        ])
        .create_lookuper()?;

    for (region, user_id, session_id) in [
        ("US", 1, "sess-a"),
        ("US", 2, "sess-b"),
        ("EU", 1, "sess-a"),
        ("EU", 1, "sess-missing"),
    ] {
        let result = prefix_lookuper
            .lookup(&make_prefix(region, user_id, session_id))
            .await?;
        let rows = result.get_rows()?;
        println!(
            "region={region}, user_id={user_id}, session_id={session_id}: {} event(s)",
            rows.len()
        );
        for row in &rows {
            println!("  seq={}, data={}", row.get_long(3)?, row.get_string(4)?);
        }
    }

    Ok(())
}

async fn create_partition(table_path: &TablePath, admin: &FlussAdmin, region: &str) {
    let mut partition_values = HashMap::new();
    partition_values.insert("region".to_string(), region.to_string());
    let partition_spec = PartitionSpec::new(partition_values);

    admin
        .create_partition(table_path, &partition_spec, true)
        .await
        .unwrap();
}

fn make_prefix(region: &str, user_id: i32, session_id: &str) -> GenericRow<'static> {
    let mut row = GenericRow::new(3);
    row.set_field(0, region.to_string());
    row.set_field(1, user_id);
    row.set_field(2, session_id.to_string());
    row
}
