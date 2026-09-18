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

//! Internal implementation of the stateless REST gateway for Apache Fluss.
//!
//! The library target keeps the executable entry point thin and lets integration tests exercise the production
//! router and lifecycle in process. The package is not published as a reusable crate and makes no public API
//! compatibility commitment.
//!
//! # Statelessness contract
//!
//! The gateway keeps **no** request-spanning state. There is no session store, no cursor store, and no replay
//! cache — deliberately, there is not even a `store` module for one to be added to. Every response is derivable
//! from the request plus current cluster state, so any instance can serve any request and instances can be added
//! or removed freely behind a plain load balancer.

pub mod backend;
pub mod config;
pub mod error;
pub mod lifecycle;
pub mod observability;
pub mod protocol;
