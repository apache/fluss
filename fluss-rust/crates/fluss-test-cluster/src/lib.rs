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

use fluss::client::FlussConnection;
use fluss::config::Config;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::bollard::Docker;
use testcontainers::bollard::query_parameters::{
    ListContainersOptionsBuilder, RemoveContainerOptionsBuilder,
};
use testcontainers::core::ContainerPort;
use testcontainers::core::client::docker_client_instance;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// testcontainers' own shared `bollard::Docker` — the same client (and daemon
/// resolution) it uses for `image.start()`, so teardown and existence checks act on
/// the exact daemon `start` created containers on. `None` (with a warning) if the
/// client is unreachable.
async fn docker_client() -> Option<Docker> {
    match docker_client_instance().await {
        Ok(docker) => Some(docker),
        Err(e) => {
            eprintln!("warning: cannot reach the testcontainers Docker client: {e}");
            None
        }
    }
}

fn zookeeper_container_name(cluster_name: &str) -> String {
    format!("zookeeper-{cluster_name}")
}

fn coordinator_server_container_name(cluster_name: &str) -> String {
    format!("coordinator-server-{cluster_name}")
}

fn tablet_server_container_name(cluster_name: &str, server_id: u16) -> String {
    format!("tablet-server-{cluster_name}-{server_id}")
}

fn is_cluster_container_name(container_name: &str, cluster_name: &str) -> bool {
    let container_name = container_name.trim_start_matches('/');
    let tablet_server_prefix = format!("tablet-server-{cluster_name}-");
    container_name == zookeeper_container_name(cluster_name)
        || container_name == coordinator_server_container_name(cluster_name)
        || container_name
            .strip_prefix(&tablet_server_prefix)
            .is_some_and(|suffix| !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()))
}

/// Force-removes a container by name. Best-effort: a 404 (already gone) is success;
/// other errors warn rather than panic.
async fn force_remove_container(docker: &Docker, name: &str) {
    let options = RemoveContainerOptionsBuilder::default().force(true).build();
    match docker.remove_container(name, Some(options)).await {
        Ok(())
        | Err(testcontainers::bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            ..
        }) => {}
        Err(e) => eprintln!("warning: failed to remove container '{name}': {e}"),
    }
}

/// Runs an async teardown future to completion from a sync caller. `stop()` runs
/// both inside a tokio runtime (async tests) and from a runtime-less atexit handler,
/// so we use a dedicated thread with its own runtime instead of blocking the caller.
fn run_blocking<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for container teardown")
            .block_on(future)
    })
    .join()
    .expect("container teardown thread panicked")
}

pub const FLUSS_IMAGE: &str = env!("FLUSS_IMAGE");
pub const FLUSS_VERSION: &str = env!("FLUSS_VERSION");
pub const ZOOKEEPER_IMAGE: &str = env!("ZOOKEEPER_IMAGE");
pub const ZOOKEEPER_VERSION: &str = env!("ZOOKEEPER_VERSION");

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ClusterInfo {
    pub bootstrap_servers: String,
    pub sasl_bootstrap_servers: Option<String>,
}

pub struct FlussTestingClusterBuilder {
    number_of_tablet_servers: u16,
    network: &'static str,
    cluster_conf: HashMap<String, String>,
    testing_name: String,
    remote_data_dir: Option<std::path::PathBuf>,
    sasl_enabled: bool,
    sasl_users: Vec<(String, String)>,
    coordinator_host_port: u16,
    plain_client_port: Option<u16>,
    image: String,
    image_tag: String,
}

impl FlussTestingClusterBuilder {
    pub fn new(testing_name: impl Into<String>) -> Self {
        Self::new_with_cluster_conf(testing_name.into(), &HashMap::default())
    }

    pub fn with_remote_data_dir(mut self, dir: std::path::PathBuf) -> Self {
        std::fs::create_dir_all(&dir).expect("Failed to create remote data directory");
        self.remote_data_dir = Some(dir);
        self
    }

    pub fn with_sasl(mut self, users: Vec<(String, String)>) -> Self {
        self.sasl_enabled = true;
        self.sasl_users = users;
        self.plain_client_port = Some(self.coordinator_host_port + 100);
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.coordinator_host_port = port;
        // Re-derive SASL port if SASL was already enabled.
        if self.sasl_enabled {
            self.plain_client_port = Some(port + 100);
        }
        self
    }

    pub fn new_with_cluster_conf(
        testing_name: impl Into<String>,
        conf: &HashMap<String, String>,
    ) -> Self {
        let mut cluster_conf = conf.clone();
        cluster_conf.insert(
            "netty.server.num-network-threads".to_string(),
            "1".to_string(),
        );
        cluster_conf.insert(
            "netty.server.num-worker-threads".to_string(),
            "3".to_string(),
        );

        FlussTestingClusterBuilder {
            number_of_tablet_servers: 1,
            cluster_conf,
            network: "fluss-cluster-network",
            testing_name: testing_name.into(),
            remote_data_dir: None,
            sasl_enabled: false,
            sasl_users: Vec::new(),
            coordinator_host_port: 9123,
            plain_client_port: None,
            // runtime env overrides the compile-time default (server-compat CI lane)
            image: std::env::var("FLUSS_IMAGE").unwrap_or_else(|_| FLUSS_IMAGE.to_string()),
            image_tag: std::env::var("FLUSS_VERSION").unwrap_or_else(|_| FLUSS_VERSION.to_string()),
        }
    }

    fn tablet_server_container_name(&self, server_id: u16) -> String {
        tablet_server_container_name(&self.testing_name, server_id)
    }

    fn coordinator_server_container_name(&self) -> String {
        coordinator_server_container_name(&self.testing_name)
    }

    fn zookeeper_container_name(&self) -> String {
        zookeeper_container_name(&self.testing_name)
    }

    fn container_names(&self) -> Vec<String> {
        std::iter::once(self.zookeeper_container_name())
            .chain(std::iter::once(self.coordinator_server_container_name()))
            .chain(
                (0..self.number_of_tablet_servers).map(|id| self.tablet_server_container_name(id)),
            )
            .collect()
    }

    fn inject_sasl_conf(&mut self) {
        if self.sasl_enabled
            && !self.sasl_users.is_empty()
            && !self.cluster_conf.contains_key("security.protocol.map")
        {
            self.cluster_conf.insert(
                "security.protocol.map".to_string(),
                "CLIENT:sasl".to_string(),
            );
            self.cluster_conf.insert(
                "security.sasl.enabled.mechanisms".to_string(),
                "plain".to_string(),
            );
            let user_entries: Vec<String> = self
                .sasl_users
                .iter()
                .map(|(u, p)| format!("user_{}=\"{}\"", u, p))
                .collect();
            let jaas_config = format!(
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required {};",
                user_entries.join(" ")
            );
            self.cluster_conf
                .insert("security.sasl.plain.jaas.config".to_string(), jaas_config);
        }
    }

    fn bootstrap_addresses(&self) -> (String, Option<String>) {
        if let Some(plain_port) = self.plain_client_port {
            (
                format!("127.0.0.1:{}", plain_port),
                Some(format!("127.0.0.1:{}", self.coordinator_host_port)),
            )
        } else {
            (format!("127.0.0.1:{}", self.coordinator_host_port), None)
        }
    }

    fn plaintext_tablet_bootstrap_servers(&self) -> HashMap<u16, String> {
        let base_port = self.plain_client_port.unwrap_or(self.coordinator_host_port);
        (0..self.number_of_tablet_servers)
            .map(|server_id| {
                (
                    server_id,
                    format!("127.0.0.1:{}", base_port + 1 + server_id),
                )
            })
            .collect()
    }

    async fn all_containers_exist(&self) -> bool {
        let Some(docker) = docker_client().await else {
            return false;
        };
        let expected: HashSet<_> = self.container_names().into_iter().collect();

        // Multiple exact-name filters are OR'd by the daemon, so we can query once
        // while still ignoring unrelated containers. `all(false)` = running only, so a
        // stopped leftover counts as absent and gets recreated.
        let filters = HashMap::from([(
            "name".to_string(),
            expected.iter().map(|name| format!("^{name}$")).collect(),
        )]);
        let options = ListContainersOptionsBuilder::default()
            .all(false)
            .filters(&filters)
            .build();
        let running = match docker.list_containers(Some(options)).await {
            Ok(containers) => containers,
            Err(_) => return false,
        };
        let running_names: HashSet<_> = running
            .into_iter()
            .flat_map(|container| container.names.unwrap_or_default())
            .map(|name| name.trim_start_matches('/').to_string())
            .collect();

        expected.iter().all(|name| running_names.contains(name))
    }

    async fn start_all_containers(&mut self) -> Vec<ContainerAsync<GenericImage>> {
        if let Some(docker) = docker_client().await {
            for name in self.container_names() {
                force_remove_container(&docker, &name).await;
            }
        }
        self.inject_sasl_conf();

        let mut containers = Vec::new();
        containers.push(self.start_zookeeper().await);
        containers.push(self.start_coordinator_server().await);
        for server_id in 0..self.number_of_tablet_servers {
            containers.push(self.start_tablet_server(server_id).await);
        }
        containers
    }

    /// Containers stop when the returned struct is dropped.
    pub async fn build(&mut self) -> FlussTestingCluster {
        let container_names = self.container_names();
        let containers = self.start_all_containers().await;

        let mut iter = containers.into_iter();
        let zookeeper = Arc::new(iter.next().unwrap());
        let coordinator_server = Arc::new(iter.next().unwrap());
        let mut tablet_servers = HashMap::new();
        for server_id in 0..self.number_of_tablet_servers {
            tablet_servers.insert(server_id, Arc::new(iter.next().unwrap()));
        }

        let (bootstrap_servers, sasl_bootstrap_servers) = self.bootstrap_addresses();
        let plaintext_tablet_bootstrap_servers = self.plaintext_tablet_bootstrap_servers();

        FlussTestingCluster {
            zookeeper,
            coordinator_server,
            tablet_servers,
            bootstrap_servers,
            plaintext_tablet_bootstrap_servers,
            sasl_bootstrap_servers,
            remote_data_dir: self.remote_data_dir.clone(),
            sasl_users: self.sasl_users.clone(),
            container_names,
        }
    }

    /// Containers outlive the process. Clean up via `stop_cluster()`.
    /// Idempotent: if the cluster is already running, returns its info.
    pub async fn build_detached(&mut self) -> ClusterInfo {
        if !self.all_containers_exist().await {
            let containers = self.start_all_containers().await;
            let _ = ManuallyDrop::new(containers);
        }

        let (bootstrap_servers, sasl_bootstrap_servers) = self.bootstrap_addresses();
        ClusterInfo {
            bootstrap_servers,
            sasl_bootstrap_servers,
        }
    }

    async fn start_zookeeper(&self) -> ContainerAsync<GenericImage> {
        GenericImage::new(ZOOKEEPER_IMAGE, ZOOKEEPER_VERSION)
            .with_network(self.network)
            .with_container_name(self.zookeeper_container_name())
            .start()
            .await
            .unwrap()
    }

    async fn start_coordinator_server(&mut self) -> ContainerAsync<GenericImage> {
        let port = self.coordinator_host_port;
        let container_name = self.coordinator_server_container_name();
        let mut coordinator_confs = HashMap::new();
        coordinator_confs.insert(
            "zookeeper.address",
            format!("{}:2181", self.zookeeper_container_name()),
        );

        if let Some(plain_port) = self.plain_client_port {
            coordinator_confs.insert(
                "bind.listeners",
                format!(
                    "INTERNAL://{}:0, CLIENT://{}:{}, PLAIN_CLIENT://{}:{}",
                    container_name, container_name, port, container_name, plain_port
                ),
            );
            coordinator_confs.insert(
                "advertised.listeners",
                format!(
                    "CLIENT://localhost:{}, PLAIN_CLIENT://localhost:{}",
                    port, plain_port
                ),
            );
        } else {
            coordinator_confs.insert(
                "bind.listeners",
                format!(
                    "INTERNAL://{}:0, CLIENT://{}:{}",
                    container_name, container_name, port
                ),
            );
            coordinator_confs.insert(
                "advertised.listeners",
                format!("CLIENT://localhost:{}", port),
            );
        }

        coordinator_confs.insert("internal.listener.name", "INTERNAL".to_string());

        let mut image = GenericImage::new(&self.image, &self.image_tag)
            .with_container_name(self.coordinator_server_container_name())
            .with_mapped_port(port, ContainerPort::Tcp(port))
            .with_network(self.network)
            .with_cmd(vec!["coordinatorServer"])
            .with_env_var(
                "FLUSS_PROPERTIES",
                self.to_fluss_properties_with(coordinator_confs),
            );

        if let Some(plain_port) = self.plain_client_port {
            image = image.with_mapped_port(plain_port, ContainerPort::Tcp(plain_port));
        }

        image.start().await.unwrap()
    }

    async fn start_tablet_server(&self, server_id: u16) -> ContainerAsync<GenericImage> {
        let port = self.coordinator_host_port;
        let container_name = self.tablet_server_container_name(server_id);
        let mut tablet_server_confs = HashMap::new();
        let expose_host_port = port + 1 + server_id;
        let tablet_server_id = format!("{}", server_id);

        if let Some(plain_port) = self.plain_client_port {
            let bind_listeners = format!(
                "INTERNAL://{}:0, CLIENT://{}:{}, PLAIN_CLIENT://{}:{}",
                container_name, container_name, port, container_name, plain_port,
            );
            let plain_expose_host_port = plain_port + 1 + server_id;
            let advertised_listeners = format!(
                "CLIENT://localhost:{}, PLAIN_CLIENT://localhost:{}",
                expose_host_port, plain_expose_host_port
            );
            tablet_server_confs.insert("bind.listeners", bind_listeners);
            tablet_server_confs.insert("advertised.listeners", advertised_listeners);
        } else {
            let bind_listeners = format!(
                "INTERNAL://{}:0, CLIENT://{}:{}",
                container_name, container_name, port,
            );
            let advertised_listeners = format!("CLIENT://localhost:{}", expose_host_port);
            tablet_server_confs.insert("bind.listeners", bind_listeners);
            tablet_server_confs.insert("advertised.listeners", advertised_listeners);
        }

        tablet_server_confs.insert(
            "zookeeper.address",
            format!("{}:2181", self.zookeeper_container_name()),
        );
        tablet_server_confs.insert("internal.listener.name", "INTERNAL".to_string());
        tablet_server_confs.insert("tablet-server.id", tablet_server_id);

        if let Some(remote_data_dir) = &self.remote_data_dir {
            tablet_server_confs.insert(
                "remote.data.dir",
                remote_data_dir.to_string_lossy().to_string(),
            );
        }
        let mut image = GenericImage::new(&self.image, &self.image_tag)
            .with_cmd(vec!["tabletServer"])
            .with_mapped_port(expose_host_port, ContainerPort::Tcp(port))
            .with_network(self.network)
            .with_container_name(self.tablet_server_container_name(server_id))
            .with_env_var(
                "FLUSS_PROPERTIES",
                self.to_fluss_properties_with(tablet_server_confs),
            );

        if let Some(plain_port) = self.plain_client_port {
            let plain_expose_host_port = plain_port + 1 + server_id;
            image = image.with_mapped_port(plain_expose_host_port, ContainerPort::Tcp(plain_port));
        }

        if let Some(ref remote_data_dir) = self.remote_data_dir {
            use testcontainers::core::Mount;
            std::fs::create_dir_all(remote_data_dir)
                .expect("Failed to create remote data directory for mount");
            let host_path = remote_data_dir.to_string_lossy().to_string();
            let container_path = remote_data_dir.to_string_lossy().to_string();
            image = image.with_mount(Mount::bind_mount(host_path, container_path));
        }

        image.start().await.unwrap()
    }

    fn to_fluss_properties_with(&self, extra_properties: HashMap<&str, String>) -> String {
        let mut fluss_properties = Vec::new();
        for (k, v) in self.cluster_conf.iter() {
            fluss_properties.push(format!("{}: {}", k, v));
        }
        for (k, v) in extra_properties.iter() {
            fluss_properties.push(format!("{}: {}", k, v));
        }
        fluss_properties.join("\n")
    }
}

#[derive(Clone)]
#[allow(dead_code)] // Fields held for RAII.
pub struct FlussTestingCluster {
    zookeeper: Arc<ContainerAsync<GenericImage>>,
    coordinator_server: Arc<ContainerAsync<GenericImage>>,
    tablet_servers: HashMap<u16, Arc<ContainerAsync<GenericImage>>>,
    bootstrap_servers: String,
    plaintext_tablet_bootstrap_servers: HashMap<u16, String>,
    sasl_bootstrap_servers: Option<String>,
    remote_data_dir: Option<std::path::PathBuf>,
    sasl_users: Vec<(String, String)>,
    container_names: Vec<String>,
}

impl FlussTestingCluster {
    pub fn stop(&self) {
        let names = self.container_names.clone();
        run_blocking(async move {
            if let Some(docker) = docker_client().await {
                for name in &names {
                    force_remove_container(&docker, name).await;
                }
            }
        });
        if let Some(ref dir) = self.remote_data_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    pub fn sasl_users(&self) -> &[(String, String)] {
        &self.sasl_users
    }

    pub fn plaintext_bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    pub fn plaintext_tablet_bootstrap_server(&self, server_id: u16) -> Option<&str> {
        self.plaintext_tablet_bootstrap_servers
            .get(&server_id)
            .map(String::as_str)
    }

    pub async fn get_fluss_connection(&self) -> FlussConnection {
        let config = Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: self.bootstrap_servers.clone(),
            ..Default::default()
        };

        self.connect_with_retry(config).await
    }

    pub async fn get_fluss_connection_with_sasl(
        &self,
        username: &str,
        password: &str,
    ) -> FlussConnection {
        let bootstrap = self
            .sasl_bootstrap_servers
            .clone()
            .unwrap_or_else(|| self.bootstrap_servers.clone());
        let config = Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: bootstrap,
            security_protocol: "sasl".to_string(),
            security_sasl_mechanism: "PLAIN".to_string(),
            security_sasl_username: username.to_string(),
            security_sasl_password: password.to_string(),
            ..Default::default()
        };

        self.connect_with_retry(config).await
    }

    pub async fn try_fluss_connection_with_sasl(
        &self,
        username: &str,
        password: &str,
    ) -> fluss::error::Result<FlussConnection> {
        let bootstrap = self
            .sasl_bootstrap_servers
            .clone()
            .unwrap_or_else(|| self.bootstrap_servers.clone());
        let config = Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: bootstrap,
            security_protocol: "sasl".to_string(),
            security_sasl_mechanism: "PLAIN".to_string(),
            security_sasl_username: username.to_string(),
            security_sasl_password: password.to_string(),
            ..Default::default()
        };

        FlussConnection::new(config).await
    }

    async fn connect_with_retry(&self, config: Config) -> FlussConnection {
        let max_retries = 60;
        let retry_interval = Duration::from_secs(1);

        for attempt in 1..=max_retries {
            match FlussConnection::new(config.clone()).await {
                Ok(connection) => {
                    return connection;
                }
                Err(e) => {
                    if attempt == max_retries {
                        panic!(
                            "Failed to connect to Fluss cluster after {} attempts: {}",
                            max_retries, e
                        );
                    }
                    tokio::time::sleep(retry_interval).await;
                }
            }
        }
        unreachable!()
    }
}

pub fn stop_cluster(name: &str) {
    let name = name.to_string();
    run_blocking(async move { stop_cluster_async(&name).await });
}

/// Force-removes every container of cluster `name` on the testcontainers daemon —
/// the same daemon `build_detached` started them on.
async fn stop_cluster_async(name: &str) {
    let Some(docker) = docker_client().await else {
        return;
    };

    // Multiple values for the `name` filter are OR'd by the daemon, so we use
    // cluster-specific prefixes to narrow the list, then exact local checks before
    // removing to avoid touching similarly-prefixed clusters such as `test` and `test2`.
    let filters = HashMap::from([(
        "name".to_string(),
        vec![
            zookeeper_container_name(name),
            coordinator_server_container_name(name),
            format!("tablet-server-{name}-"),
        ],
    )]);
    let options = ListContainersOptionsBuilder::default()
        .all(true)
        .filters(&filters)
        .build();

    let containers = match docker.list_containers(Some(options)).await {
        Ok(containers) => containers,
        Err(e) => {
            eprintln!("warning: failed to list cluster containers: {e}");
            return;
        }
    };

    for container in containers {
        // The daemon prefixes names with '/'. These testcontainers-managed containers
        // are created with a single explicit container name here, so checking the
        // first returned name is sufficient. Only remove containers whose exact name
        // belongs to this cluster; if names are missing, skip rather than guessing by id.
        if let Some(cname) = container.names.and_then(|n| n.into_iter().next()) {
            let cname = cname.trim_start_matches('/');
            if is_cluster_container_name(cname, name) {
                force_remove_container(&docker, cname).await;
            }
        }
    }
}
