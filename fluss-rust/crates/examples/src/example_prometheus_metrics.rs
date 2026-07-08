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

//! Exposes Fluss client metrics on a Prometheus scrape endpoint.
//!
//! Run a local cluster, then:
//! ```shell
//! cargo run -p fluss-examples --example example-prometheus-metrics
//! curl http://localhost:9000/metrics
//! ```
//! The endpoint exposes `fluss_client_writer_*`, `fluss_client_scanner_*`, and
//! `fluss_client_requests_*` series produced by the workload below. The example
//! runs until interrupted with Ctrl-C so the endpoint stays scrapeable.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::GenericRow;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::time::Duration;

#[tokio::main]
pub async fn main() -> Result<()> {
    // Install the global Prometheus recorder BEFORE creating any connection,
    // writer, or scanner: the client caches metric handles on first use and
    // binds them to whichever recorder is installed at that moment.
    //
    // `build()` (rather than `install()`) hands back a `PrometheusHandle` so the
    // example can read its own metrics back and self-verify; the returned
    // exporter future runs the HTTP scrape endpoint.
    let (recorder, exporter) = PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9000))
        .build()
        .expect("failed to build Prometheus recorder");
    let metrics_handle = recorder.handle();
    metrics::set_global_recorder(recorder).expect("failed to install global recorder");
    tokio::spawn(exporter);
    println!("Metrics exposed on http://localhost:9000/metrics");

    let mut config = Config::parse();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();

    let conn = FlussConnection::new(config).await?;
    let admin = conn.get_admin()?;

    let table_path = TablePath::new("fluss", "rust_prometheus_metrics");
    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("message", DataTypes::string())
                .build()?,
        )
        .build()?;
    admin
        .create_table(&table_path, &table_descriptor, true)
        .await?;

    let table = conn.get_table(&table_path).await?;
    let append_writer = table.new_append()?.create_writer()?;
    let log_scanner = table.new_scan().create_log_scanner()?;
    log_scanner.subscribe(0, 0).await?;

    // The loop runs forever on purpose: a Prometheus exporter is a long-running
    // scrape target, so the process must stay alive -- and keep producing fresh
    // samples -- for `curl /metrics` to return data across repeated scrapes.
    // Breaking out would shut the HTTP endpoint down. Stop it with Ctrl-C.
    let rows_per_iter = 100;
    let mut id = 0i32;
    let mut verified = false;
    loop {
        for _ in 0..rows_per_iter {
            let mut row = GenericRow::new(2);
            row.set_field(0, id);
            row.set_field(1, "metrics demo");
            append_writer.append(&row)?;
            id += 1;
        }
        append_writer.flush().await?;

        // Calling `poll` is what produces the `fluss_client_scanner_*` series,
        // so we do it every iteration. The returned records aren't needed for a
        // metrics demo, so we just count them for the log line.
        let polled = log_scanner.poll(Duration::from_secs(1)).await?.count();
        println!(
            "appended {rows_per_iter} rows, polled {polled} rows; scrape /metrics to see counters"
        );

        // One-off sanity check, run only on the first iteration: after the first
        // flush every appended row has been acknowledged, so the writer counter
        // must have advanced by at least the rows we sent (retries can push it
        // higher). This only proves the recorder is wired up correctly -- it is
        // not a stop condition, so the loop keeps running afterwards.
        if !verified {
            let rendered = metrics_handle.render();
            let sent = counter_value(&rendered, "fluss_client_writer_records_send_total");
            assert!(
                sent.is_some_and(|v| v >= rows_per_iter as f64),
                "expected fluss_client_writer_records_send_total >= {rows_per_iter}, got {sent:?}\n{rendered}"
            );
            println!("self-check OK: records_send_total = {}", sent.unwrap());
            verified = true;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Parse the value of an unlabeled counter/gauge line from rendered Prometheus
/// exposition text (lines shaped `metric_name <value>`).
fn counter_value(rendered: &str, name: &str) -> Option<f64> {
    let prefix = format!("{name} ");
    rendered
        .lines()
        .find(|line| line.starts_with(&prefix))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
}
