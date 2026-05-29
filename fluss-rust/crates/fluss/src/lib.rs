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

//! Apache Fluss (Incubating) Official Rust Client
//!
//! Official Rust client library for [Apache Fluss (Incubating)](https://fluss.apache.org/).
//! It supports **primary key (KV) tables** (upsert + lookup) and **log tables** (append + scan).
//!
//! # Examples
//!
//! ## Primary key table and log table
//!
//! Connect to a cluster, create a KV table (upsert and lookup), then a log table (append and scan):
//!
//! ```rust,no_run
//! use fluss::client::EARLIEST_OFFSET;
//! use fluss::client::FlussConnection;
//! use fluss::config::Config;
//! use fluss::error::Result;
//! use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
//! use fluss::row::{GenericRow, InternalRow};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut config = Config::default();
//!     config.bootstrap_servers = "127.0.0.1:9123".to_string();
//!     let connection = FlussConnection::new(config).await?;
//!     let admin = connection.get_admin()?;
//!
//!     // ---- Primary key (KV) table: upsert and lookup ----
//!     let kv_path = TablePath::new("fluss", "users");
//!     let mut kv_schema = Schema::builder()
//!         .column("id", DataTypes::int())
//!         .column("name", DataTypes::string())
//!         .column("age", DataTypes::bigint())
//!         .primary_key(vec!["id"]);
//!     let kv_descriptor = TableDescriptor::builder()
//!         .schema(kv_schema.build()?)
//!         .build()?;
//!     admin.create_table(&kv_path, &kv_descriptor, false).await?;
//!
//!     let kv_table = connection.get_table(&kv_path).await?;
//!     let upsert_writer = kv_table.new_upsert()?.create_writer()?;
//!     let mut row = GenericRow::new(3);
//!     row.set_field(0, 1i32);
//!     row.set_field(1, "Alice");
//!     row.set_field(2, 30i64);
//!     upsert_writer.upsert(&row)?;
//!     upsert_writer.flush().await?;
//!
//!     let mut lookuper = kv_table.new_lookup()?.create_lookuper()?;
//!     let mut key = GenericRow::new(1);
//!     key.set_field(0, 1i32);
//!     let result = lookuper.lookup(&key).await?;
//!     if let Some(r) = result.get_single_row()? {
//!         println!("KV lookup: id={}, name={}, age={}",
//!                  r.get_int(0)?, r.get_string(1)?, r.get_long(2)?);
//!     }
//!
//!     // ---- Log table: append and scan ----
//!     let log_path = TablePath::new("fluss", "events");
//!     let mut log_schema_builder = Schema::builder()
//!         .column("ts", DataTypes::bigint())
//!         .column("message", DataTypes::string());
//!     let log_descriptor = TableDescriptor::builder()
//!         .schema(log_schema_builder.build()?)
//!         .build()?;
//!     admin.create_table(&log_path, &log_descriptor, false).await?;
//!
//!     let log_table = connection.get_table(&log_path).await?;
//!     let append_writer = log_table.new_append()?.create_writer()?;
//!     let mut event = GenericRow::new(2);
//!     event.set_field(0, 1700000000i64);
//!     event.set_field(1, "hello");
//!     append_writer.append(&event)?;
//!     append_writer.flush().await?;
//!
//!     let scanner = log_table.new_scan().create_log_scanner()?;
//!     scanner.subscribe(0, EARLIEST_OFFSET).await?;
//!     let scan_records = scanner.poll(Duration::from_secs(1)).await?;
//!     for record in scan_records {
//!         let r = record.row();
//!         println!("Log scan: ts={}, message={}", r.get_long(0)?, r.get_string(1)?);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Performance
//!
//! For production deployments on Linux, we recommend using
//! [jemalloc](https://crates.io/crates/tikv-jemallocator) as the global allocator.
//! The default glibc allocator (ptmalloc2) can cause RSS bloat and fragmentation under
//! sustained write loads due to repeated same-size alloc/free cycles in Arrow batch building.
//! jemalloc's thread-local size-class bins handle this pattern efficiently.
//!
//! ```toml
//! [target.'cfg(not(target_env = "msvc"))'.dependencies]
//! tikv-jemallocator = "0.6"
//! ```
//!
//! ```rust,ignore
//! #[cfg(not(target_env = "msvc"))]
//! #[global_allocator]
//! static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
//! ```

pub mod client;
pub mod metadata;
pub mod record;
pub mod row;
pub mod rpc;

mod cluster;
pub use cluster::{ServerNode, ServerType};

pub mod config;
pub mod error;
pub mod metrics;

mod bucketing;
mod compression;
pub mod io;
mod util;

#[cfg(test)]
mod test_utils;

pub type TableId = i64;
pub type PartitionId = i64;
pub type BucketId = i32;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}
