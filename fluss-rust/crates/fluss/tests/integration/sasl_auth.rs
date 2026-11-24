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

#[cfg(test)]
mod sasl_auth_test {
    use crate::integration::fluss_cluster::FlussTestingClusterBuilder;
    use crate::integration::utils::{
        get_shared_cluster, wait_for_cluster_ready_with_sasl, wait_for_table_ready,
    };
    use fluss::client::FlussConnection;
    use fluss::config::Config;
    use fluss::error::FlussError;
    use fluss::metadata::{
        AclInfo, DataTypes, DatabaseDescriptorBuilder, OperationType, PermissionType, ResourceType,
        Schema, TableDescriptor, TablePath,
    };
    use fluss::rpc::message::OffsetSpec;
    use std::collections::HashMap;
    use std::time::Duration;

    const SASL_USERNAME: &str = "admin";
    const SASL_PASSWORD: &str = "admin-secret";

    /// Verify that a client with correct SASL credentials can connect and perform operations.
    #[tokio::test]
    async fn test_sasl_connect_with_valid_credentials() {
        let cluster = get_shared_cluster();
        let connection = cluster
            .get_fluss_connection_with_sasl(SASL_USERNAME, SASL_PASSWORD)
            .await;

        let admin = connection
            .get_admin()
            .expect("Should get admin with valid SASL credentials");

        // Perform a basic operation to confirm the connection is fully functional
        let db_name = "sasl_test_valid_db";
        let descriptor = DatabaseDescriptorBuilder::default()
            .comment("created via SASL auth")
            .build();

        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .expect("Should create database with SASL auth");

        assert!(admin.database_exists(db_name).await.unwrap());

        // Cleanup
        admin
            .drop_database(db_name, true, true)
            .await
            .expect("Should drop database");
    }

    /// Verify that a second user can also authenticate successfully.
    #[tokio::test]
    async fn test_sasl_connect_with_second_user() {
        let cluster = get_shared_cluster();
        let connection = cluster
            .get_fluss_connection_with_sasl("alice", "alice-secret")
            .await;

        let admin = connection
            .get_admin()
            .expect("Should get admin with alice credentials");

        // Basic operation to confirm functional connection
        assert!(
            admin
                .database_exists("some_nonexistent_db_alice")
                .await
                .is_ok()
        );
    }

    /// Verify that wrong credentials are rejected with a typed AuthenticateException error.
    #[tokio::test]
    async fn test_sasl_connect_with_wrong_password() {
        let cluster = get_shared_cluster();
        let result = cluster
            .try_fluss_connection_with_sasl(SASL_USERNAME, "wrong-password")
            .await;

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("Connection with wrong password should fail"),
        };

        // The server error code must be preserved (not wrapped in a generic string).
        // Code 46 = AuthenticateException — this is what C++ and Python bindings
        // use to distinguish auth failures from network errors.
        assert_eq!(
            err.api_error(),
            Some(FlussError::AuthenticateException),
            "Wrong password should produce AuthenticateException, got: {err}"
        );
    }

    /// Verify that a SASL-configured client fails when connecting to a plaintext server.
    #[tokio::test]
    async fn test_sasl_client_to_plaintext_server() {
        let cluster = get_shared_cluster();
        let plaintext_addr = cluster.plaintext_bootstrap_servers().to_string();

        let config = Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: plaintext_addr,
            security_protocol: "sasl".to_string(),
            security_sasl_mechanism: "PLAIN".to_string(),
            security_sasl_username: SASL_USERNAME.to_string(),
            security_sasl_password: SASL_PASSWORD.to_string(),
            ..Default::default()
        };

        let result = FlussConnection::new(config).await;
        assert!(
            result.is_err(),
            "SASL client connecting to plaintext server should fail"
        );
    }

    #[tokio::test]
    #[ignore = "requires a fixed server image containing SASL proxy authorization (#3857)"]
    async fn test_sasl_proxy_authorization() {
        let cluster_conf = HashMap::from([
            (
                "security.protocol.map".to_string(),
                "CLIENT:SASL,INTERNAL:PLAINTEXT".to_string(),
            ),
            (
                "security.sasl.enabled.mechanisms".to_string(),
                "PLAIN".to_string(),
            ),
            (
                "security.sasl.plain.credentials".to_string(),
                "admin:admin-secret,gateway:gateway-secret".to_string(),
            ),
            (
                "security.sasl.listener.name.client.plain.proxy-users".to_string(),
                "gateway:alice".to_string(),
            ),
            ("authorizer.enabled".to_string(), "true".to_string()),
            ("super.users".to_string(), "User:admin".to_string()),
        ]);
        let cluster = FlussTestingClusterBuilder::new_with_cluster_conf(
            format!("proxy-{}", uuid::Uuid::new_v4().simple()),
            &cluster_conf,
        )
        .with_port(19323)
        .with_sasl(vec![(SASL_USERNAME.to_string(), SASL_PASSWORD.to_string())])
        .build()
        .await;
        wait_for_cluster_ready_with_sasl(&cluster).await;
        let admin_connection = cluster
            .get_fluss_connection_with_sasl(SASL_USERNAME, SASL_PASSWORD)
            .await;
        let admin = admin_connection.get_admin().unwrap();
        let database = format!("sasl_proxy_{}", uuid::Uuid::new_v4().simple());
        admin.create_database(&database, None, false).await.unwrap();
        let alice_table = TablePath::new(database.clone(), "alice_table");
        let gateway_table = TablePath::new(database.clone(), "gateway_table");
        let describe_only_table = TablePath::new(database.clone(), "describe_only_table");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .build()
                    .unwrap(),
            )
            .distributed_by(Some(1), vec![])
            .build()
            .unwrap();
        for table_path in [&alice_table, &gateway_table, &describe_only_table] {
            admin
                .create_table(table_path, &descriptor, false)
                .await
                .unwrap();
            wait_for_table_ready(&admin, table_path).await;
        }
        let results = admin
            .create_acls(
                [
                    (&alice_table, "alice", OperationType::All),
                    (&gateway_table, "gateway", OperationType::All),
                    (&describe_only_table, "gateway", OperationType::All),
                    (&describe_only_table, "alice", OperationType::Describe),
                ]
                .into_iter()
                .map(|(path, principal, operation_type)| AclInfo {
                    resource_name: path.to_string(),
                    resource_type: ResourceType::Table,
                    principal_name: principal.to_string(),
                    principal_type: "User".to_string(),
                    host: "*".to_string(),
                    operation_type,
                    permission_type: PermissionType::Allow,
                })
                .collect(),
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 4);
        assert!(
            results.iter().all(|result| {
                result
                    .error
                    .as_ref()
                    .is_none_or(|error| error.code == FlussError::None.code())
            }),
            "{results:?}"
        );
        let gateway_config = Config {
            bootstrap_servers: admin_connection.config().bootstrap_servers.clone(),
            security_protocol: "sasl".to_string(),
            security_sasl_username: "gateway".to_string(),
            security_sasl_password: "gateway-secret".to_string(),
            ..Default::default()
        };
        for authorization_id in [None, Some(""), Some("alice")] {
            let config = Config {
                security_sasl_authorization_id: authorization_id.map(str::to_string),
                ..gateway_config.clone()
            };
            let connection = FlussConnection::new(config).await.unwrap();
            let effective_admin = connection.get_admin().unwrap();
            let (allowed, denied) = if authorization_id == Some("alice") {
                (&alice_table, &gateway_table)
            } else {
                (&gateway_table, &alice_table)
            };
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let access = async {
                        effective_admin.get_table_info(allowed).await?;
                        effective_admin
                            .list_offsets(allowed, &[0], OffsetSpec::Latest)
                            .await?;
                        Ok::<_, fluss::error::Error>(())
                    }
                    .await;
                    match access {
                        Ok(()) => break,
                        Err(error)
                            if error.api_error() == Some(FlussError::AuthorizationException) =>
                        {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Err(error) => panic!("{error}"),
                    }
                }
            })
            .await
            .expect("ACL grant did not become visible");
            let denied_error = effective_admin.get_table_info(denied).await.unwrap_err();
            assert_eq!(
                denied_error.api_error(),
                Some(FlussError::AuthorizationException)
            );
            let table = connection.get_table(allowed).await.unwrap();
            let writer = table.new_append().unwrap().create_writer().unwrap();
            let ack = writer
                .append_arrow_batch(arrow::array::record_batch!(("id", Int32, [1])).unwrap())
                .unwrap();
            writer.flush().await.unwrap();
            ack.await.unwrap();
            if authorization_id == Some("alice") {
                let table = connection.get_table(&describe_only_table).await.unwrap();
                let scanner = table.new_scan().create_log_scanner().unwrap();
                scanner.subscribe(0, 0).await.unwrap();
                let error = tokio::time::timeout(Duration::from_secs(30), async {
                    loop {
                        if let Err(error) = scanner.poll(Duration::from_secs(1)).await {
                            break error;
                        }
                    }
                })
                .await
                .expect("Tablet read was not rejected");
                assert_eq!(error.api_error(), Some(FlussError::AuthorizationException));
            }
        }
        for (authorization_id, password) in [
            (Some("bob"), "gateway-secret"),
            (Some("admin"), "gateway-secret"),
            (Some("alice"), "wrong-password"),
        ] {
            let config = Config {
                security_sasl_authorization_id: authorization_id.map(str::to_string),
                security_sasl_password: password.to_string(),
                ..gateway_config.clone()
            };
            let error = FlussConnection::new(config)
                .await
                .err()
                .expect("Impersonation must fail");
            assert_eq!(error.api_error(), Some(FlussError::AuthenticateException));
            for output in [format!("{error}"), format!("{error:?}")] {
                for secret in ["gateway", authorization_id.unwrap(), password] {
                    assert!(!output.contains(secret));
                }
            }
        }
        admin.drop_database(&database, false, true).await.unwrap();
    }

    /// Verify that a nonexistent user is rejected with a typed error.
    #[tokio::test]
    async fn test_sasl_connect_with_unknown_user() {
        let cluster = get_shared_cluster();
        let result = cluster
            .try_fluss_connection_with_sasl("nonexistent_user", "some-password")
            .await;

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("Connection with unknown user should fail"),
        };

        assert_eq!(
            err.api_error(),
            Some(FlussError::AuthenticateException),
            "Unknown user should produce AuthenticateException, got: {err}"
        );
    }
}
