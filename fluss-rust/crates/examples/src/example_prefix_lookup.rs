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

    // Schema: primary key is (user_id, session_id, event_seq); the bucket key
    // (user_id, session_id) is a strict prefix of the primary key, which is
    // what enables prefix lookup.
    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("user_id", DataTypes::int())
                .column("session_id", DataTypes::string())
                .column("event_seq", DataTypes::bigint())
                .column("event_data", DataTypes::string())
                .primary_key(vec!["user_id", "session_id", "event_seq"])
                .build()?,
        )
        .distributed_by(
            Some(3),
            vec!["user_id".to_string(), "session_id".to_string()],
        )
        .build()?;

    let table_path = TablePath::new("fluss", "rust_prefix_lookup_example");

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

    println!("\n=== Upserting session events ===");
    for (user_id, session_id, event_seq, event_data) in [
        (1, "sess-a", 1i64, "open"),
        (1, "sess-a", 2, "click"),
        (1, "sess-a", 3, "close"),
        (1, "sess-b", 1, "open"),
        (2, "sess-c", 1, "open"),
    ] {
        let mut row = GenericRow::new(4);
        row.set_field(0, user_id);
        row.set_field(1, session_id);
        row.set_field(2, event_seq);
        row.set_field(3, event_data);
        upsert_writer.upsert(&row)?;
        println!("Upserted: {row:?}");
    }
    upsert_writer.flush().await?;

    println!("\n=== Prefix lookup by (user_id, session_id) ===");
    // `lookup_by` names the prefix columns. The resulting lookuper returns all
    // rows whose primary key starts with the given prefix.
    let mut prefix_lookuper = table
        .new_lookup()?
        .lookup_by(vec!["user_id".to_string(), "session_id".to_string()])
        .create_lookuper()?;

    for (user_id, session_id) in [
        (1, "sess-a"),
        (1, "sess-b"),
        (2, "sess-c"),
        (2, "sess-missing"),
    ] {
        let result = prefix_lookuper
            .lookup(&make_prefix(user_id, session_id))
            .await?;
        let rows = result.get_rows()?;
        println!(
            "user_id={user_id}, session_id={session_id}: {} event(s)",
            rows.len()
        );
        for row in &rows {
            println!("  seq={}, data={}", row.get_long(2)?, row.get_string(3)?);
        }
    }

    Ok(())
}

fn make_prefix(user_id: i32, session_id: &str) -> GenericRow<'static> {
    let mut row = GenericRow::new(2);
    row.set_field(0, user_id);
    row.set_field(1, session_id.to_string());
    row
}
