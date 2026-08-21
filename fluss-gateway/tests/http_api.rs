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

//! End-to-end HTTP tests over the full production wiring.
//!
//! These drive a real listener through a real HTTP client, so they exercise the lifecycle, the middleware stack,
//! and the router exactly as deployed.

mod support;

use fluss_gateway::lifecycle::RunningGateway;
use support::{Api, start_gateway};

/// One in-process gateway plus a client bound to its address.
async fn gateway() -> (RunningGateway, Api) {
    let gateway = start_gateway().await;
    let api = Api::new(format!("http://{}", gateway.local_addr()));
    (gateway, api)
}

#[tokio::test]
async fn health_and_ready_answer_over_the_real_listener() {
    let (gateway, api) = gateway().await;

    let health = api.get_ok("/health").await;
    assert_eq!(health["status"], "ok");
    assert!(health["uptime_ms"].is_u64(), "{health}");
    assert_eq!(api.get_ok("/ready").await["status"], "ready");

    gateway.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn the_openapi_document_is_served_and_generated_from_the_router() {
    let (gateway, api) = gateway().await;

    let document = api.get_ok("/v1/openapi.json").await;
    assert_eq!(document["openapi"], "3.1.0");
    assert_eq!(document["info"]["license"]["name"], "Apache-2.0");
    assert!(document["paths"]["/health"]["get"].is_object());
    assert!(document["components"]["schemas"]["ErrorEnvelope"].is_object());

    gateway.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn an_unknown_route_returns_the_shared_error_envelope() {
    let (gateway, api) = gateway().await;

    let response = api.get("/v1/nope").await;
    assert_eq!(response.status(), 404);
    assert!(response.headers().contains_key("x-request-id"));
    let body: serde_json::Value = response.json().await.expect("JSON body");
    assert_eq!(body["error"]["code"], "not_found");
    assert!(body["error"]["request_id"].as_str().is_some());
    assert_eq!(
        body["error"].as_object().expect("error object").len(),
        3,
        "the FIP-49 envelope carries code, message, and the correlating request id: {body}"
    );

    gateway.shutdown().await.expect("clean shutdown");
}

/// The duration families are exported as Prometheus histograms, which aggregate across gateway instances.
/// Without explicit buckets the exporter emits pre-computed summary quantiles instead, which do not.
#[tokio::test]
async fn request_durations_are_exported_as_histograms() {
    let gateway = support::start_gateway_with_metrics().await;
    let api = Api::new(format!("http://{}", gateway.local_addr()));
    let metrics_address = gateway
        .metrics_addr()
        .expect("the metrics listener is bound");

    api.get_ok("/health").await;
    let exposition = Api::new(format!("http://{metrics_address}"))
        .get("/metrics")
        .await
        .text()
        .await
        .expect("metrics body");

    assert!(
        exposition.contains("# TYPE fluss_gateway_rest_request_duration_seconds histogram"),
        "duration is a histogram: {exposition}"
    );
    assert!(
        exposition.contains("fluss_gateway_rest_request_duration_seconds_bucket"),
        "histogram buckets are exported: {exposition}"
    );

    gateway.shutdown().await.expect("clean shutdown");
}

/// A request whose declared body exceeds the configured limit answers 413 with the shared envelope.
///
/// Sent as a raw HTTP request that never writes the body: the gateway answers 413 (never 429)
/// before any payload exists, and reading instead of writing avoids racing the early close.
#[tokio::test]
async fn an_oversized_body_is_rejected_with_413_and_never_429() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (gateway, _api) = gateway().await;
    let address = gateway.local_addr();

    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    let request = format!(
        "POST /v1/openapi.json HTTP/1.1\r\n\
         Host: {address}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         \r\n",
        64 * 1024 * 1024
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send headers");

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read response");
    let response = String::from_utf8_lossy(&response);
    assert!(
        response.starts_with("HTTP/1.1 413"),
        "expected 413, got: {response}"
    );
    assert!(response.contains("limit_exceeded"), "got: {response}");

    gateway.shutdown().await.expect("clean shutdown");
}

/// A gateway with a short header read timeout, for the connection-level tests.
async fn short_header_timeout_gateway() -> fluss_gateway::lifecycle::RunningGateway {
    let mut config = fluss_gateway::config::GatewayConfig::default();
    config.server.rest.bind_address = "127.0.0.1:0".parse().expect("valid");
    config.server.metrics.enabled = false;
    config.server.rest.header_read_timeout =
        fluss_gateway::config::ConfigDuration::from_millis(300);
    fluss_gateway::lifecycle::start(config)
        .await
        .expect("gateway starts")
}

/// A connection that never completes a request head is closed, and none of these heads negotiates
/// HTTP/2. The per-request deadline cannot defend here, as it runs only after a complete head; the
/// plain http1 builder starts its timer at connection setup, so a silent socket is covered too, and
/// the HTTP/2 preface only fails head parsing.
#[tokio::test]
async fn connections_without_a_complete_head_are_closed_and_never_upgraded() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let gateway = short_header_timeout_gateway().await;
    for (case, head) in [
        ("a silent connection", b"".as_slice()),
        ("half a request head", b"GET /heal".as_slice()),
        (
            "an HTTP/2 preface",
            b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".as_slice(),
        ),
    ] {
        let mut socket = tokio::net::TcpStream::connect(gateway.local_addr())
            .await
            .expect("connect");
        socket.write_all(head).await.expect("send the head");

        // Reading to EOF returns only once the server closes, so a parked connection times out.
        let mut received = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            socket.read_to_end(&mut received),
        )
        .await
        .unwrap_or_else(|_| panic!("{case} was not closed"))
        .expect("read");
        let response = String::from_utf8_lossy(&received);
        assert!(
            received.is_empty() || response.starts_with("HTTP/1.1 4"),
            "{case}: expected a close or an HTTP/1 error, got: {response}"
        );
    }

    gateway.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn draining_rejects_guarded_routes_but_keeps_health_answering() {
    let gateway = support::start_gateway().await;
    let api = Api::new(format!("http://{}", gateway.local_addr()));
    gateway.begin_shutdown();
    assert_eq!(api.get("/health").await.status(), 200);
    assert_eq!(api.get("/ready").await.status(), 503);
    assert_eq!(api.get("/v1/openapi.json").await.status(), 503);
}
