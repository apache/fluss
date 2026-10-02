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

// Rustler 0.37 wraps every NIF body in `std::panic::catch_unwind`, which requires
// all captured values (including `ResourceArc<T>`) to be `RefUnwindSafe`.
// `ResourceArc` contains `*mut T`, so it is only `RefUnwindSafe` when `T` is.
// Our resource types contain `parking_lot` locks (`UnsafeCell`) which opt out of
// the auto-trait. We manually impl `RefUnwindSafe` on each resource type because
// panic safety is already guaranteed by the NIF boundary — a panic is caught and
// converted to an Erlang exception, never observed by Rust code.

mod admin;
mod append_writer;
mod async_nif;
mod atoms;
mod config;
mod connection;
mod log_scanner;
mod row_convert;
mod schema;
mod table;
mod write_handle;

use std::sync::LazyLock;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime")
});

rustler::init!("Elixir.Fluss.Native");
