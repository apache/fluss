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
use fluss::predicate::col;
use fluss::row::{DataGetters, GenericRow};
use std::time::Duration;

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut config = Config::parse();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();

    let conn = FlussConnection::new(config).await?;
    let admin = conn.get_admin()?;

    let table_path = TablePath::new("fluss", "rust_filter_pushdown");
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .build()?,
        )
        // Collect per-batch column statistics; without them nothing is pruned.
        .property("table.statistics.columns", "*")
        .build()?;
    admin.create_table(&table_path, &descriptor, true).await?;

    let table = conn.get_table(&table_path).await?;
    let writer = table.new_append()?.create_writer()?;

    // Two flushes produce two batches with disjoint id ranges.
    for id in 1..=3 {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, format!("low-{id}"));
        writer.append(&row)?;
    }
    writer.flush().await?;
    for id in 101..=103 {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, format!("high-{id}"));
        writer.append(&row)?;
    }
    writer.flush().await?;

    // The server skips the first batch: its statistics say max(id) is 3.
    let scanner = table
        .new_scan()
        .filter(col("id").gt(100))?
        .create_log_scanner()?;
    scanner.subscribe(0, EARLIEST_OFFSET).await?;

    let mut received = 0;
    while received < 3 {
        let records = scanner.poll(Duration::from_secs(10)).await?;
        for record in records {
            let row = record.row();
            println!(
                "id={}, name={} @ offset={}",
                row.get_int(0)?,
                row.get_string(1)?,
                record.offset()
            );
            received += 1;
        }
    }

    admin.drop_table(&table_path, false).await?;
    Ok(())
}
