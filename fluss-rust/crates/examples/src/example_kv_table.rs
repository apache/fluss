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
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{GenericRow, InternalRow};

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
                .column("name", DataTypes::string())
                .column("age", DataTypes::bigint())
                .primary_key(vec!["id"])
                .build()?,
        )
        .build()?;

    let table_path = TablePath::new("fluss", "rust_upsert_lookup_example");

    let admin = conn.get_admin()?;
    admin
        .create_table(&table_path, &table_descriptor, true)
        .await?;
    println!(
        "Created KV Table:\n {}\n",
        admin.get_table_info(&table_path).await?
    );

    let table = conn.get_table(&table_path).await?;
    let table_upsert = table.new_upsert()?;
    let upsert_writer = table_upsert.create_writer()?;

    println!("\n=== Upserting ===");
    for (id, name, age) in [(1, "Verso", 32i64), (2, "Noco", 25), (3, "Esquie", 35)] {
        let mut row = GenericRow::new(3);
        row.set_field(0, id);
        row.set_field(1, name);
        row.set_field(2, age);
        upsert_writer.upsert(&row)?;
        println!("Upserted: {row:?}");
    }
    upsert_writer.flush().await?;

    println!("\n=== Looking up ===");
    let mut lookuper = table.new_lookup()?.create_lookuper()?;

    for id in 1..=3 {
        let result = lookuper.lookup(&make_key(id)).await?;
        let row = result.get_single_row()?.unwrap();
        println!(
            "Found id={id}: name={}, age={}",
            row.get_string(1)?,
            row.get_long(2)?
        );
    }

    println!("\n=== Updating ===");
    let mut row = GenericRow::new(3);
    row.set_field(0, 1);
    row.set_field(1, "Verso");
    row.set_field(2, 33i64);
    upsert_writer.upsert(&row)?.await?;
    println!("Updated: {row:?}");

    let result = lookuper.lookup(&make_key(1)).await?;
    let row = result.get_single_row()?.unwrap();
    println!(
        "Verified update: name={}, age={}",
        row.get_string(1)?,
        row.get_long(2)?
    );

    println!("\n=== Deleting ===");
    // For delete, only primary key field needs to be set; other fields can remain null
    let mut row = GenericRow::new(3);
    row.set_field(0, 2);
    upsert_writer.delete(&row)?.await?;
    println!("Deleted row with id=2");

    let result = lookuper.lookup(&make_key(2)).await?;
    if result.get_single_row()?.is_none() {
        println!("Verified deletion");
    }

    Ok(())
}

fn make_key(id: i32) -> GenericRow<'static> {
    let mut row = GenericRow::new(1);
    row.set_field(0, id);
    row
}
