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

//! `fluss-gateway` binary: parse the CLI, load and validate the config, initialise logging, then run the
//! lifecycle. Exits nonzero on configuration errors (2) or startup/serving failures (1), e.g. a listener bind
//! failure.

use clap::Parser;
use fluss_gateway::config::{self, CliOverrides};
use fluss_gateway::{lifecycle, observability};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Command-line arguments. Everything else is configured through the `gateway.yaml` file or the environment.
#[derive(Debug, Parser)]
#[command(
    name = "fluss-gateway",
    about = "Stateless REST gateway for Apache Fluss",
    version
)]
struct Cli {
    /// Path to the `gateway.yaml` configuration file (YAML with flat dotted keys).
    #[arg(long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Overrides `gateway.rest.listen` (e.g. `127.0.0.1:8080`).
    #[arg(long, value_name = "ADDR")]
    bind_address: Option<String>,
}

/// Loads configuration and runs the gateway process with stable exit codes.
#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let env: BTreeMap<String, String> = std::env::vars().collect();
    let overrides = CliOverrides {
        bind_address: cli.bind_address,
    };

    let config = match config::load(cli.config.as_deref(), &env, &overrides) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("fluss-gateway: {error}");
            std::process::exit(2);
        }
    };

    observability::init_logging();

    if let Err(error) = lifecycle::run(config).await {
        log::error!("fluss-gateway failed: {error}");
        std::process::exit(1);
    }
}
