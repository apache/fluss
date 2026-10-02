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
    use crate::integration::utils::get_shared_cluster;
    use fluss::client::FlussConnection;
    use fluss::config::Config;
    use fluss::error::FlussError;
    use fluss::metadata::DatabaseDescriptorBuilder;

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
