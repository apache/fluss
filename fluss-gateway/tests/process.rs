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

//! End-to-end checks of the compiled binary: startup, health, SIGTERM draining, and exit codes.
//!
//! These spawn the real `fluss-gateway` executable, so they exercise CLI parsing, config loading, logging
//! setup, and the production lifecycle exactly as an operator would. No Fluss cluster is involved; the
//! suite that adds one lives in `e2e_cluster.rs`.

mod support;

use std::io::Read;
use std::time::Duration;
use support::{ChildGuard, await_http_ok, binary, free_port, write_config};

#[tokio::test]
async fn an_invalid_configuration_fails_before_binding_with_exit_code_2() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("gateway.yaml");
    std::fs::write(&path, "gateway.unknown.key: true\n").expect("write");
    let output = binary().arg("--config").arg(&path).output().expect("run");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("gateway.unknown.key"),
        "stderr names the offending key: {stderr}"
    );
}

#[tokio::test]
async fn invalid_bootstrap_servers_fail_before_binding_with_exit_code_2() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("gateway.yaml");
    for (bootstrap_servers, detail) in [
        (" , ", "must configure at least one server"),
        ("host", "expected host:port"),
        ("host:99999", "expected host:port"),
        ("http://host:9123", "expected host:port"),
    ] {
        std::fs::write(
            &path,
            format!(
                "gateway.rest.listen: 127.0.0.1:0\n\
                 gateway.metrics.enabled: false\n\
                 gateway.cluster.default.bootstrap.servers: {bootstrap_servers:?}\n"
            ),
        )
        .expect("write");

        let output = binary().arg("--config").arg(&path).output().expect("run");
        assert_eq!(output.status.code(), Some(2), "{bootstrap_servers}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("gateway.cluster.default.bootstrap.servers") && stderr.contains(detail),
            "{bootstrap_servers}: {stderr}"
        );
    }
}

#[tokio::test]
async fn a_native_client_override_fails_before_binding_without_leaking_credentials() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("gateway.yaml");
    std::fs::write(
        &path,
        "gateway.cluster.default.connection.service.account: gateway-user\n\
         gateway.cluster.default.connection.service.secret: canonical-secret\n\
         gateway.security.authentication: token\n\
         gateway.security.tokens: token-secret:alice\n\
         gateway.cluster.default.client.writer.batch-size: client-secret-value\n",
    )
    .expect("write");

    let output = binary().arg("--config").arg(&path).output().expect("run");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("gateway.cluster.default.client.writer.batch-size"),
        "{stderr}"
    );
    assert!(stderr.contains("not supported yet"), "{stderr}");
    for credential in ["canonical-secret", "token-secret", "client-secret-value"] {
        assert!(
            !stderr.contains(credential),
            "stderr leaked {credential}: {stderr}"
        );
    }
}

#[tokio::test]
async fn canonical_credentials_are_redacted_from_startup_diagnostics() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let path = dir.path().join("gateway.yaml");
    std::fs::write(
        &path,
        format!(
            "gateway.rest.listen: 127.0.0.1:{port}\n\
             gateway.metrics.enabled: false\n\
             gateway.cluster.default.connection.service.account: canonical-user\n\
             gateway.cluster.default.connection.service.secret: canonical-secret\n\
             gateway.security.authentication: token\n\
             gateway.security.tokens: token-secret:alice\n"
        ),
    )
    .expect("write");

    let child = binary()
        .arg("--config")
        .arg(&path)
        .env("RUST_LOG", "debug")
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn");
    let mut guard = ChildGuard(child);
    assert!(
        await_http_ok(
            &format!("http://127.0.0.1:{port}/health"),
            Duration::from_secs(15)
        )
        .await,
        "health"
    );
    guard.send_sigterm();
    let status = guard.wait_for_exit(Duration::from_secs(35)).await;
    assert_eq!(status.code(), Some(0));

    let mut stderr = String::new();
    guard
        .0
        .stderr
        .take()
        .expect("piped stderr")
        .read_to_string(&mut stderr)
        .expect("read stderr");
    assert!(stderr.contains("effective configuration"), "{stderr}");
    assert!(stderr.contains("canonical-user"), "{stderr}");
    for credential in ["canonical-secret", "token-secret"] {
        assert!(
            !stderr.contains(credential),
            "stderr leaked {credential}: {stderr}"
        );
    }
}

#[tokio::test]
async fn the_binary_starts_serves_health_and_drains_on_sigterm_with_exit_code_0() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let config = write_config(&dir, port);
    // The gateway inherits the test's stdout/stderr: piping without draining could fill the pipe
    // buffer and stall the child, and its few startup/drain log lines are useful on failure.
    let child = binary()
        .arg("--config")
        .arg(&config)
        .spawn()
        .expect("spawn");
    let mut guard = ChildGuard(child);
    let base = format!("http://127.0.0.1:{port}");
    assert!(
        await_http_ok(&format!("{base}/health"), Duration::from_secs(15)).await,
        "health"
    );
    guard.send_sigterm();
    let status = guard.wait_for_exit(Duration::from_secs(35)).await;
    assert_eq!(status.code(), Some(0), "clean drain exits 0");
}

#[tokio::test]
async fn a_bind_conflict_fails_serving_with_exit_code_1() {
    let holder = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = holder.local_addr().expect("addr").port();
    let dir = tempfile::tempdir().expect("tempdir");
    let config = write_config(&dir, port);
    let output = binary().arg("--config").arg(&config).output().expect("run");
    assert_eq!(output.status.code(), Some(1));
}
