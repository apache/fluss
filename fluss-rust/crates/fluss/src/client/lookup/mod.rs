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

//! Lookup client implementation with batching and queuing support.
//!
//! This module provides a high-throughput lookup client that batches multiple
//! lookup operations together to reduce network round trips, achieving parity
//! with the Java client implementation.
//!
//! # Example
//!
//! ```ignore
//! let lookup_client = LookupClient::new(config, metadata);
//! let future = lookup_client.lookup(table_path, table_bucket, key_bytes);
//! let result = future.await?;
//! ```

mod lookup_client;
mod lookup_query;
mod lookup_queue;
mod lookup_sender;

pub use lookup_client::LookupClient;
pub(crate) use lookup_query::{PrefixLookupQuery, PrimaryLookupQuery, QueuedLookup};
pub(crate) use lookup_queue::LookupQueue;
