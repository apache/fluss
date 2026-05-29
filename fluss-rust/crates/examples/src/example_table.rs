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

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod example_kv_table;
mod example_partitioned_kv_table;

use clap::Parser;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{GenericRow, InternalRow};
use std::time::Duration;

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut config = Config::parse();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();

    let conn = FlussConnection::new(config).await?;

    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("c1", DataTypes::int())
                .column("c2", DataTypes::string())
                .column("c3", DataTypes::bigint())
                .build()?,
        )
        .build()?;

    let table_path = TablePath::new("fluss", "rust_test_long");

    let admin = conn.get_admin()?;

    admin
        .create_table(&table_path, &table_descriptor, true)
        .await?;

    // 2: get the table
    let table_info = admin.get_table_info(&table_path).await?;
    print!("Get created table:\n {table_info}\n");

    // write row
    let mut row = GenericRow::new(3);
    row.set_field(0, 22222);
    row.set_field(1, "t2t");
    row.set_field(2, 123_456_789_123i64);

    let table = conn.get_table(&table_path).await?;
    let append_writer = table.new_append()?.create_writer()?;
    // Fire-and-forget: queue writes then flush
    append_writer.append(&row)?;
    let mut row = GenericRow::new(3);
    row.set_field(0, 233333);
    row.set_field(1, "tt44");
    row.set_field(2, 987_654_321_987i64);
    append_writer.append(&row)?;
    append_writer.flush().await?;

    // scan rows
    let log_scanner = table.new_scan().create_log_scanner()?;
    log_scanner.subscribe(0, 0).await?;

    loop {
        let scan_records = log_scanner.poll(Duration::from_secs(10)).await?;
        println!("Start to poll records......");
        for record in scan_records {
            let row = record.row();
            println!(
                "{{{}, {}, {}}}@{}",
                row.get_int(0)?,
                row.get_string(1)?,
                row.get_long(2)?,
                record.offset()
            );
        }
    }
}
