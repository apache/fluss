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

//! End-to-end Gateway tests against a Dockerized Fluss cluster (`just test-e2e`).

mod support;

use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TablePath};
use fluss::row::{DataGetters, GenericRow};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use support::{Api, ChildGuard, await_http_ok, binary, free_port};

const DATABASE: &str = "gateway_e2e";
const TABLE: &str = "events";
const LOG_TABLE: &str = "applog";
const KV_TABLE: &str = "profiles";
const JOURNEY_DATABASE: &str = "gateway_e2e_journey";
const SERVICE_ACCOUNT: &str = "admin";
const SERVICE_SECRET: &str = "admin-secret";

/// A detached Docker cluster managed through the same helper used by the other language bindings.
///
/// Drop is a best-effort backstop for assertions that panic; the happy path checks cleanup explicitly.
struct FlussCluster {
    helper: PathBuf,
    name: String,
    plaintext_bootstrap_servers: String,
    sasl_bootstrap_servers: String,
    stopped: bool,
}

impl FlussCluster {
    fn start(port: u16) -> Self {
        let helper = std::env::var_os("FLUSS_TEST_CLUSTER_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("../fluss-rust/target/debug/fluss-test-cluster"));
        assert!(
            helper.is_file(),
            "Fluss test cluster helper does not exist at {}; run `just test-e2e`",
            helper.display()
        );
        let name = format!("gateway-e2e-{}-{port}", std::process::id());
        // Construct the guard before invoking the helper, so partial startup is cleaned on any panic.
        let mut cluster = Self {
            helper,
            name,
            plaintext_bootstrap_servers: String::new(),
            sasl_bootstrap_servers: String::new(),
            stopped: false,
        };
        let output = Command::new(&cluster.helper)
            .args(["start", "--name"])
            .arg(&cluster.name)
            .arg("--sasl")
            .args(["--port", &port.to_string()])
            .output()
            .expect("start the Fluss test cluster helper");
        assert!(
            output.status.success(),
            "Fluss test cluster failed to start:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("cluster helper stdout is UTF-8");
        let json = stdout
            .lines()
            .find_map(|line| line.strip_prefix("CLUSTER_JSON: "))
            .expect("cluster helper returns CLUSTER_JSON");
        let info: serde_json::Value = serde_json::from_str(json).expect("valid cluster JSON");
        cluster.plaintext_bootstrap_servers = info["bootstrap_servers"]
            .as_str()
            .expect("cluster JSON contains bootstrap_servers")
            .to_string();
        cluster.sasl_bootstrap_servers = info["sasl_bootstrap_servers"]
            .as_str()
            .expect("SASL cluster JSON contains sasl_bootstrap_servers")
            .to_string();
        cluster
    }

    fn stop(mut self) {
        let output =
            stop_cluster(&self.helper, &self.name).expect("stop the Fluss test cluster helper");
        assert!(
            output.status.success(),
            "Fluss test cluster failed to stop:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        self.stopped = true;
    }
}

impl Drop for FlussCluster {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = stop_cluster(&self.helper, &self.name);
        }
    }
}

fn stop_cluster(helper: &Path, name: &str) -> std::io::Result<std::process::Output> {
    Command::new(helper).args(["stop", "--name", name]).output()
}

/// Finds the SASL and plaintext host-port pairs for the coordinator and single tablet server.
fn free_cluster_port_pair() -> u16 {
    for _ in 0..100 {
        let candidate = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a candidate port");
        let port = candidate.local_addr().expect("candidate address").port();
        drop(candidate);
        let Some(sasl_tablet) = port.checked_add(1) else {
            continue;
        };
        let Some(plaintext_coordinator) = port.checked_add(100) else {
            continue;
        };
        let Some(plaintext_tablet) = port.checked_add(101) else {
            continue;
        };
        let mut reservations = Vec::new();
        for candidate in [port, sasl_tablet, plaintext_coordinator, plaintext_tablet] {
            match std::net::TcpListener::bind(("127.0.0.1", candidate)) {
                Ok(listener) => reservations.push(listener),
                Err(_) => break,
            }
        }
        if reservations.len() == 4 {
            return port;
        }
    }
    panic!("failed to find the four ports for a SASL Fluss test cluster");
}

/// Writes the production Gateway configuration that points its default cluster at the test cluster.
fn write_gateway_config(
    directory: &tempfile::TempDir,
    port: u16,
    bootstrap_servers: &str,
    security_protocol: &str,
) -> std::path::PathBuf {
    let path = directory.path().join("gateway.yaml");
    let mut file = std::fs::File::create(&path).expect("create gateway config");
    writeln!(file, "gateway.rest.listen: 127.0.0.1:{port}").expect("write REST listener");
    writeln!(file, "gateway.metrics.enabled: false").expect("disable metrics listener");
    writeln!(
        file,
        "gateway.cluster.default.bootstrap.servers: {bootstrap_servers}"
    )
    .expect("write Fluss bootstrap servers");
    writeln!(
        file,
        "gateway.cluster.default.connection.security.protocol: {security_protocol}"
    )
    .expect("write Fluss security protocol");
    if security_protocol == "sasl" {
        writeln!(
            file,
            "gateway.cluster.default.connection.service.account: {SERVICE_ACCOUNT}"
        )
        .expect("write Gateway service account");
        writeln!(
            file,
            "gateway.cluster.default.connection.service.secret: {SERVICE_SECRET}"
        )
        .expect("write Gateway service secret");
    }
    path
}

async fn assert_metadata_apis(
    bootstrap_servers: &str,
    security_protocol: &str,
    catalog_journey: bool,
) {
    let gateway_port = free_port();
    let directory = tempfile::tempdir().expect("temporary Gateway config directory");
    let config = write_gateway_config(
        &directory,
        gateway_port,
        bootstrap_servers,
        security_protocol,
    );
    let child = binary()
        .arg("--config")
        .arg(config)
        .spawn()
        .expect("start the Gateway binary");
    let mut gateway = ChildGuard(child);
    let base = format!("http://127.0.0.1:{gateway_port}");
    assert!(
        await_http_ok(&format!("{base}/ready"), Duration::from_secs(15)).await,
        "Gateway becomes ready"
    );

    let api = Api::new(base);
    assert_eq!(
        api.get_ok("/v1/clusters").await,
        serde_json::json!({"clusters": ["default"]})
    );

    let databases = api.get_ok("/v1/clusters/default/databases").await;
    assert!(
        databases["databases"]
            .as_array()
            .expect("database array")
            .iter()
            .any(|database| database == DATABASE),
        "created database is returned: {databases}"
    );
    assert_eq!(
        api.get_ok(&format!("/v1/clusters/default/databases/{DATABASE}/tables"))
            .await,
        serde_json::json!({"tables": [LOG_TABLE, TABLE, KV_TABLE]})
    );
    if catalog_journey {
        assert_catalog_journey(&api).await;
    }

    gateway.send_sigterm();
    let status = gateway.wait_for_exit(Duration::from_secs(35)).await;
    assert_eq!(status.code(), Some(0), "Gateway drains cleanly");
}

/// Drives the catalog life cycle using only the REST API.
async fn assert_catalog_journey(api: &Api) {
    let databases = "/v1/clusters/default/databases";
    let database = format!("{databases}/{JOURNEY_DATABASE}");
    let tables = format!("{database}/tables");
    let table = format!("{tables}/orders");
    let partitions = format!("{table}/partitions");

    let (location, created) = api
        .post_created(
            databases,
            &serde_json::json!({"database": JOURNEY_DATABASE}),
        )
        .await;
    assert_eq!(location, database);
    assert_eq!(created, serde_json::json!({"database": JOURNEY_DATABASE}));

    // A dry run reaches Fluss for nothing, so the table is still absent afterwards.
    let definition = serde_json::json!({
        "table_name": "orders",
        "columns": [
            {"name": "id", "data_type": {"type": "BIGINT"}, "nullable": false},
            {"name": "dt", "data_type": {"type": "STRING"}, "nullable": false},
            {"name": "amount", "data_type": {"type": "DECIMAL", "precision": 18, "scale": 2}},
        ],
        "primary_key": ["id", "dt"],
        "partitioned_by": ["dt"],
        "distribution": {"bucket_count": 2, "bucket_keys": ["id"]},
        "configs": {"table.log.ttl": "7d"},
        "custom_properties": {"app.owner": "sales", "app.stage": "draft"},
        "comment": "the E2E journey table",
    });
    let mut dry_run = definition.clone();
    dry_run["validate_only"] = serde_json::json!(true);
    let (status, _, summary) = api.post(&tables, &dry_run).await;
    assert_eq!(status, 200, "the dry run is accepted: {summary}");
    assert_eq!(summary["validate_only"], true);
    assert_eq!(
        api.get(&table).await.status(),
        404,
        "the dry run created nothing"
    );

    for key in ["nope.key", "table.nope"] {
        let mut invalid = definition.clone();
        invalid["configs"][key] = serde_json::json!("1");
        let (status, _, error) = api.post(&tables, &invalid).await;
        assert_eq!(status, 400, "{key}: {error}");
        assert_eq!(error["error"]["code"], "invalid_argument");
    }

    let (location, body) = api.post_created(&tables, &definition).await;
    assert_eq!(location, table);
    assert_eq!(body, serde_json::Value::Null);
    let created = api.get_ok(&table).await;
    assert_eq!(created["distribution"]["bucket_count"], 2);
    assert_eq!(created["configs"]["table.log.ttl"], "7d");
    assert_eq!(
        created["custom_properties"],
        definition["custom_properties"]
    );
    // The declared type survives the round trip through Fluss's own schema serialization.
    assert_eq!(
        created["columns"][2]["data_type"],
        serde_json::json!({"type": "DECIMAL", "precision": 18, "scale": 2})
    );
    let (status, _, error) = api
        .patch(
            &table,
            &serde_json::json!({
                "changes": [{"kind": "set_config", "key": "table.nope", "value": "1"}]
            }),
        )
        .await;
    assert_eq!(status, 400, "{error}");
    assert_eq!(error["error"]["code"], "invalid_argument");
    assert_eq!(api.get_ok(&table).await, created);

    // Fluss currently requires schema and config changes in separate requests.
    let (status, _, body) = api
        .patch(
            &table,
            &serde_json::json!({
                "changes": [
                    {"kind": "add_column", "name": "note", "data_type": {"type": "STRING"}}
                ]
            }),
        )
        .await;
    assert_eq!(status, 204, "the alteration is applied: {body}");
    assert_eq!(body, serde_json::Value::Null);
    let altered = api.get_ok(&table).await;
    assert_eq!(altered["columns"][3]["name"], "note");

    for (changes, expected) in [
        (
            serde_json::json!([
                {"kind": "set_config", "key": "app.owner", "value": "finance"},
                {"kind": "reset_config", "key": "app.stage"}
            ]),
            Some(serde_json::json!({"app.owner": "finance"})),
        ),
        (
            serde_json::json!([{"kind": "reset_config", "key": "app.owner"}]),
            None,
        ),
    ] {
        let (status, _, body) = api
            .patch(&table, &serde_json::json!({"changes": changes}))
            .await;
        assert_eq!(status, 204, "{body}");
        assert_eq!(body, serde_json::Value::Null);
        let altered = api.get_ok(&table).await;
        assert_eq!(altered["configs"], created["configs"]);
        assert_eq!(altered.get("custom_properties"), expected.as_ref());
    }

    let (status, _, body) = api
        .patch(
            &table,
            &serde_json::json!({
                "changes": [
                    {"kind": "set_config", "key": "table.log.ttl", "value": "30d"}
                ]
            }),
        )
        .await;
    assert_eq!(status, 204, "the alteration is applied: {body}");
    assert_eq!(body, serde_json::Value::Null);
    let altered = api.get_ok(&table).await;
    assert_eq!(altered["configs"]["table.log.ttl"], "30d");

    let (location, partition) = api
        .post_created(
            &partitions,
            &serde_json::json!({"partition": {"dt": "2026-08-25"}}),
        )
        .await;
    assert_eq!(location, format!("{partitions}/2026-08-25"));
    assert_eq!(
        partition["partition"],
        serde_json::json!({"dt": "2026-08-25"})
    );
    let listed = api.get_ok(&partitions).await;
    assert_eq!(
        listed,
        serde_json::json!({
            "partitions": [{"name": "2026-08-25", "partition": {"dt": "2026-08-25"}}]
        })
    );

    // Drop without cascade requires leaf-to-root cleanup.
    assert_eq!(
        api.delete(&database).await,
        409,
        "the database is not empty"
    );
    let name = listed["partitions"][0]["name"].as_str().unwrap();
    assert_eq!(api.delete(&format!("{partitions}/{name}")).await, 204);
    assert_eq!(api.delete(&table).await, 204);
    assert_eq!(api.delete(&database).await, 204);
    assert!(
        !api.get_ok(databases).await["databases"]
            .as_array()
            .expect("database list")
            .contains(&serde_json::json!(JOURNEY_DATABASE))
    );
}

/// Writes over HTTP and reads back with the native client.
async fn assert_write_apis(
    bootstrap_servers: &str,
    security_protocol: &str,
    connection: &FlussConnection,
) {
    let gateway_port = free_port();
    let directory = tempfile::tempdir().expect("temporary Gateway config directory");
    let config = write_gateway_config(
        &directory,
        gateway_port,
        bootstrap_servers,
        security_protocol,
    );
    let child = binary()
        .arg("--config")
        .arg(config)
        .spawn()
        .expect("start the Gateway binary");
    let mut gateway = ChildGuard(child);
    let base = format!("http://127.0.0.1:{gateway_port}");
    assert!(
        await_http_ok(&format!("{base}/ready"), Duration::from_secs(15)).await,
        "Gateway becomes ready"
    );
    let api = Api::new(base);

    let appended = api
        .post_json_text_ok(
            &format!("/v1/clusters/default/databases/{DATABASE}/tables/{LOG_TABLE}/records"),
            r#"{"entries":[
                {"id":"a1","append":{"ts":"1700000000000","message":"first"}},
                {"id":"a2","append":{"ts":"1700000000001","message":"second"}}
            ]}"#,
        )
        .await;
    assert_eq!(appended["success_count"], 2, "{appended}");

    // Upsert rows, then delete one.
    let records = format!("/v1/clusters/default/databases/{DATABASE}/tables/{KV_TABLE}/records");
    let upserted = api
        .post_json_text_ok(
            &records,
            r#"{"entries":[
                {"id":"u1","upsert":{"id":1,"name":"ada","note":"first"}},
                {"id":"u2","upsert":{"id":2,"name":"bob","note":"second"}},
                {"id":"u3","upsert":{"id":3,"name":"cyd","note":"third"}},
                {"id":"u4","upsert":{"id":4,"name":"dee","note":"fourth"}},
                {"id":"u5","upsert":{"id":5,"name":"eve","note":"fifth"}}
            ]}"#,
        )
        .await;
    assert_eq!(upserted["success_count"], 5, "{upserted}");

    let deleted = api
        .post_json_text_ok(&records, r#"{"entries":[{"id":"d1","delete":{"id":3}}]}"#)
        .await;
    assert_eq!(deleted["success_count"], 1, "{deleted}");

    let updated = api
        .post_json_text_ok(
            &records,
            r#"{"partial_update_columns":["id","note"],
                "entries":[{"id":"p1","upsert":{"id":2,"note":"amended"}},
                           {"id":"p2","upsert":{"id":4}},
                           {"id":"p3","upsert":{"id":5,"note":null}}]}"#,
        )
        .await;
    assert_eq!(updated["success_count"], 3, "{updated}");

    let rejected = api
        .post_json_text(
            &records,
            r#"{"entries":[
                {"id":"ok","upsert":{"id":9,"name":"nine","note":"nine"}},
                {"id":"bad","upsert":{"id":10,"nope":"unknown column"}}
            ]}"#,
        )
        .await;
    assert_eq!(rejected.status(), 400);

    assert_schema_recreation(&api, connection).await;

    // The Gateway is shut down before the read-back, which also proves the rows were durable rather
    // than buffered in the process that wrote them.
    gateway.send_sigterm();
    let status = gateway.wait_for_exit(Duration::from_secs(35)).await;
    assert_eq!(status.code(), Some(0), "Gateway drains cleanly");

    assert_log_rows_reached_fluss(connection).await;
    assert_kv_rows_reached_fluss(connection).await;
}

async fn assert_schema_recreation(api: &Api, connection: &FlussConnection) {
    let path = TablePath::new(DATABASE, "recreated_profiles");
    let table = format!("/v1/clusters/default/databases/{DATABASE}/tables/recreated_profiles");
    let records = format!("{table}/records");
    let descriptor = |columns: [&str; 2]| {
        TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column(columns[0], DataTypes::string())
                    .column(columns[1], DataTypes::string())
                    .primary_key(["id"])
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .distributed_by(Some(1), Vec::new())
            .build()
            .unwrap()
    };
    let admin = connection.get_admin().unwrap();
    admin
        .create_table(&path, &descriptor(["name", "note"]), false)
        .await
        .unwrap();
    let body = r#"{"entries":[{"id":"e1","upsert":{"id":1,"name":"expected-name","note":"expected-note"}}]}"#;
    let warm = api.post_json_text_ok(&records, body).await;
    assert_eq!(warm["success_count"], 1, "{warm}");
    let original = connection
        .get_table(&path)
        .await
        .unwrap()
        .get_table_info()
        .clone();

    admin.drop_table(&path, false).await.unwrap();
    admin
        .create_table(&path, &descriptor(["note", "name"]), false)
        .await
        .unwrap();

    let refreshed = api.get_ok(&table).await;
    assert_eq!(refreshed["columns"][1]["name"], "note");
    assert_eq!(refreshed["columns"][2]["name"], "name");

    let table = connection.get_table(&path).await.unwrap();
    assert_eq!(table.get_table_info().schema_id, original.schema_id);
    assert_ne!(table.get_table_info().table_id, original.table_id);
    let mut lookuper = table.new_lookup().unwrap().create_lookuper().unwrap();
    let mut key = GenericRow::new(1);
    key.set_field(0, 1);
    assert!(
        lookuper
            .lookup(&key)
            .await
            .unwrap()
            .get_single_row()
            .unwrap()
            .is_none(),
        "the recreated table starts empty"
    );

    let written = api.post_json_text_ok(&records, body).await;
    assert_eq!(written["success_count"], 1, "{written}");
    let result = lookuper.lookup(&key).await.unwrap();
    let row = result
        .get_single_row()
        .unwrap()
        .expect("the retried row is written");
    assert_eq!(row.get_string(1).unwrap(), "expected-note");
    assert_eq!(row.get_string(2).unwrap(), "expected-name");
}

/// Reads the appended rows back with a bounded scan of the log table's only bucket.
async fn assert_log_rows_reached_fluss(connection: &FlussConnection) {
    let path = TablePath::new(DATABASE, LOG_TABLE);
    let table = connection
        .get_table(&path)
        .await
        .expect("open the log table");
    let table_id = table.get_table_info().get_table_id();
    let mut scanner = table
        .new_scan()
        .limit(16)
        .expect("a positive scan limit")
        .create_bucket_batch_scanner(TableBucket::new(table_id, 0))
        .expect("a bounded scanner over the only bucket");
    let batches = scanner
        .collect_all_batches()
        .await
        .expect("scan the appended rows");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.num_records())
            .sum::<usize>(),
        2
    );
}

async fn assert_kv_rows_reached_fluss(connection: &FlussConnection) {
    let path = TablePath::new(DATABASE, KV_TABLE);
    let table = connection
        .get_table(&path)
        .await
        .expect("open the KV table");
    let mut lookuper = table
        .new_lookup()
        .expect("prepare the lookup")
        .create_lookuper()
        .expect("create the lookuper");

    let mut key = GenericRow::new(1);
    key.set_field(0, 1);
    let first = lookuper.lookup(&key).await.expect("look up id 1");
    let row = first
        .get_single_row()
        .expect("decode the row")
        .expect("id 1 was upserted");
    assert_eq!(row.get_string(1).expect("name"), "ada");
    assert_eq!(row.get_string(2).expect("note"), "first");

    let mut key = GenericRow::new(1);
    key.set_field(0, 2);
    let second = lookuper.lookup(&key).await.expect("look up id 2");
    let row = second
        .get_single_row()
        .expect("decode the row")
        .expect("id 2 was upserted");
    assert_eq!(row.get_string(1).expect("name"), "bob");
    assert_eq!(row.get_string(2).expect("note"), "amended");

    for (id, name) in [(4, "dee"), (5, "eve")] {
        let mut key = GenericRow::new(1);
        key.set_field(0, id);
        let result = lookuper.lookup(&key).await.unwrap();
        let row = result
            .get_single_row()
            .unwrap()
            .expect("partial-update row exists");
        assert_eq!(row.get_string(1).unwrap(), name);
        assert!(
            row.is_null_at(2).unwrap(),
            "missing and explicit-null targets clear note"
        );
    }

    let mut key = GenericRow::new(1);
    key.set_field(0, 3);
    let third = lookuper.lookup(&key).await.expect("look up id 3");
    assert!(
        third.get_single_row().expect("decode the row").is_none(),
        "id 3 was deleted"
    );

    // The rejected batch was all-or-nothing, so neither of its entries reached the table.
    let mut key = GenericRow::new(1);
    key.set_field(0, 9);
    let rejected = lookuper.lookup(&key).await.expect("look up id 9");
    assert!(
        rejected.get_single_row().expect("decode the row").is_none(),
        "a batch rejected by preflight writes nothing"
    );
}

#[tokio::test]
async fn metadata_apis_support_plaintext_and_sasl_fluss_clusters() {
    let cluster = tokio::task::spawn_blocking(|| FlussCluster::start(free_cluster_port_pair()))
        .await
        .expect("cluster startup task");

    let connection = FlussConnection::new(Config {
        bootstrap_servers: cluster.sasl_bootstrap_servers.clone(),
        security_protocol: "sasl".to_string(),
        security_sasl_mechanism: "PLAIN".to_string(),
        security_sasl_username: SERVICE_ACCOUNT.to_string(),
        security_sasl_password: SERVICE_SECRET.to_string(),
        ..Default::default()
    })
    .await
    .expect("connect the catalog setup client");
    let admin = connection.get_admin().expect("get Fluss admin client");
    admin
        .create_database(DATABASE, None, false)
        .await
        .expect("create the E2E database");
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("payload", DataTypes::string())
                .build()
                .expect("build the E2E schema"),
        )
        .build()
        .expect("build the E2E table descriptor");
    admin
        .create_table(&TablePath::new(DATABASE, TABLE), &descriptor, false)
        .await
        .expect("create the E2E table");

    let log_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("ts", DataTypes::bigint())
                .column("message", DataTypes::string())
                .build()
                .expect("build the log schema"),
        )
        .distributed_by(Some(1), Vec::new())
        .build()
        .expect("build the log descriptor");
    admin
        .create_table(&TablePath::new(DATABASE, LOG_TABLE), &log_descriptor, false)
        .await
        .expect("create the log table");

    let kv_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .column("note", DataTypes::string())
                .primary_key(["id"])
                .unwrap()
                .build()
                .expect("build the KV schema"),
        )
        .distributed_by(Some(1), Vec::new())
        .build()
        .expect("build the KV descriptor");
    admin
        .create_table(&TablePath::new(DATABASE, KV_TABLE), &kv_descriptor, false)
        .await
        .expect("create the KV table");

    assert_metadata_apis(&cluster.plaintext_bootstrap_servers, "plaintext", false).await;
    assert_metadata_apis(&cluster.sasl_bootstrap_servers, "sasl", true).await;
    assert_write_apis(&cluster.sasl_bootstrap_servers, "sasl", &connection).await;

    connection
        .close(Duration::from_secs(10))
        .await
        .expect("close the setup client");
    tokio::task::spawn_blocking(|| cluster.stop())
        .await
        .expect("cluster cleanup task");
}
