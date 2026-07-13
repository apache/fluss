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
use fluss::client::{EARLIEST_OFFSET, FlussConnection};
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{DataGetters, GenericRow};
use std::time::Duration;

#[tokio::main]
#[allow(dead_code)]
pub async fn main() -> Result<()> {
    let mut config = Config::parse();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();

    let conn = FlussConnection::new(config).await?;
    let admin = conn.get_admin()?;

    // A single-bucket primary key table keeps the changelog on one bucket and in
    // order, which makes the CDC output easy to follow.
    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .primary_key(vec!["id"])
                .build()?,
        )
        .distributed_by(Some(1), vec!["id".to_string()])
        .build()?;

    let table_path = TablePath::new("fluss", "rust_kv_changelog_example");
    admin
        .create_table(&table_path, &table_descriptor, true)
        .await?;

    let table = conn.get_table(&table_path).await?;
    let upsert_writer = table.new_upsert()?.create_writer()?;

    // Insert three keys (+I), update one (-U / +U) and delete one (-D).
    for (id, name) in [(1, "alice"), (2, "bob"), (3, "carol")] {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, name);
        upsert_writer.upsert(&row)?;
    }
    let mut updated = GenericRow::new(2);
    updated.set_field(0, 2);
    updated.set_field(1, "bob-v2");
    upsert_writer.upsert(&updated)?;

    let mut deleted = GenericRow::new(2);
    deleted.set_field(0, 3);
    upsert_writer.delete(&deleted)?;
    upsert_writer.flush().await?;

    // Subscribe to the changelog from the start and print each CDC event until
    // we reach the end of the log.
    let log_scanner = table.new_scan().create_log_scanner()?;
    log_scanner.subscribe(0, EARLIEST_OFFSET).await?;

    println!("Changelog (change_type id name):");
    loop {
        let records = log_scanner.poll(Duration::from_secs(3)).await?;
        if records.is_empty() {
            break;
        }
        for record in records {
            let row = record.row();
            println!(
                "  {} {} {}",
                record.change_type().short_string(),
                row.get_int(0)?,
                row.get_string(1)?,
            );
        }
    }

    Ok(())
}
