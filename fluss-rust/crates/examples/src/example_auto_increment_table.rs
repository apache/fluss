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

//! The server assigns the auto-increment column, so writes must be partial updates
//! that omit it. A full-row write is rejected.

use clap::Parser;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{DataGetters, GenericRow};

#[tokio::main]
#[allow(dead_code)]
pub async fn main() -> Result<()> {
    let mut config = Config::parse();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();

    let conn = FlussConnection::new(config).await?;

    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("uid", DataTypes::string())
                .column("region", DataTypes::string())
                .column("uid_int", DataTypes::bigint())
                .primary_key(vec!["uid"])?
                .enable_auto_increment("uid_int")?
                .build()?,
        )
        .build()?;

    let table_path = TablePath::new("fluss", "rust_auto_increment_example");

    let admin = conn.get_admin()?;
    admin.drop_table(&table_path, true).await?;
    admin
        .create_table(&table_path, &table_descriptor, true)
        .await?;

    let table_info = admin.get_table_info(&table_path).await?;
    println!(
        "Created table with auto-increment columns: {:?}",
        table_info.get_schema().auto_increment_col_names()
    );

    let table = conn.get_table(&table_path).await?;

    println!("\n=== Full-row write ===");
    match table.new_upsert()?.create_writer() {
        Ok(_) => panic!("expected a full-row writer to be rejected"),
        Err(error) => println!("Rejected: {error}"),
    }

    println!("\n=== Partial update, omitting the auto-increment column ===");
    let upsert_writer = table
        .new_upsert()?
        .partial_update_with_column_names(&["uid", "region"])?
        .create_writer()?;

    for (uid, region) in [("alice", "eu"), ("bob", "us"), ("carol", "apac")] {
        let mut row = GenericRow::new(3);
        row.set_field(0, uid);
        row.set_field(1, region);
        upsert_writer.upsert(&row)?;
        println!("Upserted uid={uid}");
    }
    upsert_writer.flush().await?;

    let mut lookuper = table.new_lookup()?.create_lookuper()?;
    let mut assigned = Vec::new();
    for uid in ["alice", "bob", "carol"] {
        let mut key = GenericRow::new(1);
        key.set_field(0, uid);
        let result = lookuper.lookup(&key).await?;
        let row = result.get_single_row()?.expect("row should exist");
        let uid_int = row.get_long(2)?;
        assigned.push(uid_int);
        println!("uid={uid} region={} uid_int={uid_int}", row.get_string(1)?);
    }

    assigned.sort_unstable();
    assigned.dedup();
    assert_eq!(assigned.len(), 3, "server assigned duplicate values");

    admin.drop_table(&table_path, true).await?;
    println!("\nDropped table: {table_path}");
    Ok(())
}
