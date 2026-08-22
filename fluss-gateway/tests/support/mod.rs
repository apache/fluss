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

//! HTTP and process helpers shared by the gateway test suites.
//!
//! The suites differ only in what serves the requests — an in-process gateway, the compiled binary, or the
//! binary alongside a dockerized Fluss cluster — so the client side and the process handling live here once.

// Each test binary uses a different subset of these helpers.
#![allow(dead_code)]

use fluss_gateway::config::GatewayConfig;
use fluss_gateway::lifecycle::RunningGateway;
use serde_json::Value;
use std::io::Write;
use std::process::{Child, Command, ExitStatus};
use std::time::{Duration, Instant};

/// A thin REST client bound to one gateway base URL.
///
/// The gateway has no authentication yet, so every request is sent bare.
pub struct Api {
    client: reqwest::Client,
    base: String,
}

impl Api {
    /// Creates a test client bound to `base_url`.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base: base_url.into(),
        }
    }

    /// Resolves one absolute request URL against the configured base URL.
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Sends a GET request and returns the raw response.
    pub async fn get(&self, path: &str) -> reqwest::Response {
        self.client
            .get(self.url(path))
            .send()
            .await
            .expect("GET request")
    }

    /// GET expecting 200, returning the parsed body.
    pub async fn get_ok(&self, path: &str) -> Value {
        let response = self.get(path).await;
        assert_eq!(response.status(), 200, "GET {path}");
        response.json().await.expect("JSON body")
    }
}

/// Starts an in-process gateway over `lifecycle::start` with an ephemeral port and no metrics listener.
pub async fn start_gateway() -> RunningGateway {
    start(false).await
}

/// Starts an in-process gateway with the Prometheus listener bound to an ephemeral port.
pub async fn start_gateway_with_metrics() -> RunningGateway {
    start(true).await
}

async fn start(metrics: bool) -> RunningGateway {
    let mut config = GatewayConfig::default();
    config.server.rest.bind_address = "127.0.0.1:0".parse().expect("valid");
    config.server.metrics.enabled = metrics;
    config.server.metrics.bind_address = "127.0.0.1:0".parse().expect("valid");
    fluss_gateway::lifecycle::start(config)
        .await
        .expect("gateway starts")
}

/// A command that runs the compiled gateway executable, so the suites exercise CLI parsing, configuration
/// loading, logging setup, and the production lifecycle exactly as an operator would.
pub fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_fluss-gateway"))
}

/// Polls `url` until it answers 200 or the deadline passes.
///
/// Async on purpose, and the only variant: `reqwest::blocking` panics with "Cannot drop a runtime in a
/// context where blocking is not allowed" when it is called from inside a tokio context, so a synchronous
/// helper is a trap for any suite that later becomes a `#[tokio::test]`.
pub async fn await_http_ok(url: &str, deadline: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if reqwest::get(url)
            .await
            .is_ok_and(|response| response.status() == 200)
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Writes a `gateway.yaml` that serves REST on `port` with the metrics listener off.
pub fn write_config(dir: &tempfile::TempDir, port: u16) -> std::path::PathBuf {
    let path = dir.path().join("gateway.yaml");
    let mut file = std::fs::File::create(&path).expect("config file");
    writeln!(file, "gateway.rest.listen: 127.0.0.1:{port}").expect("write");
    writeln!(file, "gateway.metrics.enabled: false").expect("write");
    path
}

/// A port that was free a moment ago; the gateway binds it as a real listener afterwards.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

/// Kills the child on drop so a failing assertion never leaks a running gateway that could hold its port
/// into later tests.
pub struct ChildGuard(pub Child);

impl ChildGuard {
    /// Asks the gateway to shut down the way an orchestrator would.
    pub fn send_sigterm(&self) {
        // SAFETY: kill(2) with a live child pid owned by this test.
        unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM) };
    }

    /// Waits for the process to exit, failing the test if it outlasts `within`.
    pub async fn wait_for_exit(&mut self, within: Duration) -> ExitStatus {
        let start = Instant::now();
        loop {
            if let Some(status) = self.0.try_wait().expect("wait") {
                return status;
            }
            assert!(
                start.elapsed() < within,
                "the gateway exited within {within:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
