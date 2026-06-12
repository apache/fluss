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

    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("region", DataTypes::string())
                .column("zone", DataTypes::bigint())
                .column("score", DataTypes::bigint())
                .primary_key(vec!["id", "region", "zone"])
                .build()?,
        )
        .partitioned_by(vec!["region", "zone"])
        .build()?;

    let table_path = TablePath::new("fluss", "partitioned_kv_example");

    let admin = conn.get_admin()?;
    admin
        .create_table(&table_path, &table_descriptor, true)
        .await?;
    println!(
        "Created KV Table:\n {}\n",
        admin.get_table_info(&table_path).await?
    );

    create_partition(&table_path, &admin, "APAC", 1).await;
    create_partition(&table_path, &admin, "EMEA", 2).await;
    create_partition(&table_path, &admin, "US", 3).await;

    let table = conn.get_table(&table_path).await?;
    let table_upsert = table.new_upsert()?;
    let upsert_writer = table_upsert.create_writer()?;

    println!("\n=== Upserting ===");
    for (id, region, zone, score) in [
        (1001, "APAC", 1i64, 1234i64),
        (1002, "EMEA", 2, 2234),
        (1003, "US", 3, 3234),
    ] {
        let mut row = GenericRow::new(4);
        row.set_field(0, id);
        row.set_field(1, region);
        row.set_field(2, zone);
        row.set_field(3, score);
        upsert_writer.upsert(&row)?;
        println!("Upserted: {row:?}");
    }
    upsert_writer.flush().await?;

    println!("\n=== Looking up ===");
    let mut lookuper = table.new_lookup()?.create_lookuper()?;

    for (id, region, zone) in [(1001, "APAC", 1i64), (1002, "EMEA", 2), (1003, "US", 3)] {
        let result = lookuper
            .lookup(&make_key(id, region, zone))
            .await
            .expect("lookup");
        let row = result.get_single_row()?.unwrap();
        println!(
            "Found id={id}: region={}, zone={}, score={}",
            row.get_string(1)?,
            row.get_long(2)?,
            row.get_long(3)?
        );
    }

    println!("\n=== Updating ===");
    let mut row = GenericRow::new(4);
    row.set_field(0, 1001);
    row.set_field(1, "APAC");
    row.set_field(2, 1i64);
    row.set_field(3, 4321i64);
    upsert_writer.upsert(&row)?.await?;
    println!("Updated: {row:?}");

    let result = lookuper.lookup(&make_key(1001, "APAC", 1)).await?;
    let row = result.get_single_row()?.unwrap();
    println!(
        "Verified update: region={}, zone={}",
        row.get_string(1)?,
        row.get_long(2)?
    );

    println!("\n=== Deleting ===");
    let mut row = GenericRow::new(4);
    row.set_field(0, 1002);
    row.set_field(1, "EMEA");
    row.set_field(2, 2i64);
    upsert_writer.delete(&row)?.await?;
    println!("Deleted: {row:?}");

    let result = lookuper.lookup(&make_key(1002, "EMEA", 2)).await?;
    if result.get_single_row()?.is_none() {
        println!("Verified deletion");
    }

    Ok(())
}

async fn create_partition(table_path: &TablePath, admin: &FlussAdmin, region: &str, zone: i64) {
    let mut partition_values = HashMap::new();
    partition_values.insert("region".to_string(), region.to_string());
    partition_values.insert("zone".to_string(), zone.to_string());
    let partition_spec = PartitionSpec::new(partition_values);

    admin
        .create_partition(table_path, &partition_spec, true)
        .await
        .unwrap();
}

fn make_key(id: i32, region: &str, zone: i64) -> GenericRow<'static> {
    let mut row = GenericRow::new(4);
    row.set_field(0, id);
    row.set_field(1, region.to_string());
    row.set_field(2, zone);
    row
}
