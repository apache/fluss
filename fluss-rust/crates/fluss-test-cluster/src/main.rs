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

use clap::{Parser, Subcommand};
use fluss::ServerType;
use fluss::config::Config;
use fluss_test_cluster::FlussTestingClusterBuilder;
use std::time::Duration;

#[derive(Parser)]
#[command(about = "Manage a Fluss test cluster via testcontainers")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start a Fluss test cluster (idempotent). Prints cluster info as JSON to stdout.
    Start {
        #[arg(long, default_value = "shared-test")]
        name: String,
        #[arg(long)]
        sasl: bool,
        #[arg(long, default_value_t = 9123)]
        port: u16,
    },
    /// Stop and remove all containers for a cluster.
    Stop {
        #[arg(long, default_value = "shared-test")]
        name: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Start { name, sasl, port } => {
            eprintln!("Starting Fluss test cluster '{}'...", name);

            let mut builder = FlussTestingClusterBuilder::new(&name).with_port(port);

            if sasl {
                builder = builder.with_sasl(vec![
                    ("admin".to_string(), "admin-secret".to_string()),
                    ("alice".to_string(), "alice-secret".to_string()),
                ]);
            }

            let info = builder.build_detached().await;
            let start = std::time::Instant::now();

            // Check plaintext endpoint only — can't verify SASL without credentials.
            eprintln!("Waiting for cluster to be ready...");
            loop {
                let config = Config {
                    bootstrap_servers: info.bootstrap_servers.clone(),
                    ..Default::default()
                };
                if let Ok(conn) = fluss::client::FlussConnection::new(config).await {
                    if let Ok(admin) = conn.get_admin() {
                        if let Ok(nodes) = admin.get_server_nodes().await {
                            if nodes
                                .iter()
                                .any(|n| *n.server_type() == ServerType::TabletServer)
                            {
                                break;
                            }
                        }
                    }
                }
                if start.elapsed() >= Duration::from_secs(60) {
                    eprintln!("TIMEOUT: cluster did not become ready within 60s");
                    std::process::exit(1);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            eprintln!("Cluster ready.");
            println!("CLUSTER_JSON: {}", serde_json::to_string(&info).unwrap());
        }
        Command::Stop { name } => {
            eprintln!("Stopping Fluss test cluster '{}'...", name);
            fluss_test_cluster::stop_cluster(&name);
            eprintln!("Cluster stopped.");
        }
    }
}
