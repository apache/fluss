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
mod admin_test {
    use crate::integration::utils::{
        create_partitions, create_table, get_shared_cluster, wait_for_partitions_ready,
        wait_for_table_buckets_ready,
    };
    use fluss::client::{FlussAdmin, FlussConnection};
    use fluss::config::Config;
    use fluss::error::FlussError;
    use fluss::metadata::{
        BucketStatsRequest, DataTypes, DatabaseDescriptorBuilder, KvFormat, LogFormat,
        PartitionSpec, Schema, TableDescriptor, TablePath,
    };
    use fluss::row::GenericRow;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_create_database() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("should get admin");

        let db_descriptor = DatabaseDescriptorBuilder::default()
            .comment("test_db")
            .custom_properties([("k1", "v1"), ("k2", "v2")].into())
            .build();

        let db_name = "test_create_database";

        assert!(!admin.database_exists(db_name).await.unwrap());

        // create database
        admin
            .create_database(db_name, Some(&db_descriptor), false)
            .await
            .expect("should create database");

        // database should exist
        assert!(admin.database_exists(db_name).await.unwrap());

        // get database
        let db_info = admin
            .get_database_info(db_name)
            .await
            .expect("should get database info");

        assert_eq!(db_info.database_name(), db_name);
        assert_eq!(db_info.database_descriptor(), &db_descriptor);

        // drop database
        admin
            .drop_database(db_name, false, true)
            .await
            .expect("should drop_database");

        // database shouldn't exist now
        assert!(!admin.database_exists(db_name).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_table() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin client");

        let test_db_name = "test_create_table_db";
        let db_descriptor = DatabaseDescriptorBuilder::default()
            .comment("Database for test_create_table")
            .build();

        assert!(!admin.database_exists(test_db_name).await.unwrap());
        admin
            .create_database(test_db_name, Some(&db_descriptor), false)
            .await
            .expect("Failed to create test database");

        let test_table_name = "test_user_table";
        let table_path = TablePath::new(test_db_name, test_table_name);

        // build table schema
        let table_schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .column("age", DataTypes::int())
            .with_comment("User's age (optional)")
            .column("email", DataTypes::string())
            .primary_key(vec!["id".to_string()])
            .unwrap()
            .build()
            .expect("Failed to build table schema");

        // build table descriptor
        let table_descriptor = TableDescriptor::builder()
            .schema(table_schema.clone())
            .comment("Test table for user data (id, name, age, email)")
            .distributed_by(Some(3), vec!["id".to_string()])
            .property("table.replication.factor", "1")
            .log_format(LogFormat::ARROW)
            .kv_format(KvFormat::INDEXED)
            .build()
            .expect("Failed to build table descriptor");

        // create test table
        admin
            .create_table(&table_path, &table_descriptor, false)
            .await
            .expect("Failed to create test table");

        assert!(
            admin.table_exists(&table_path).await.unwrap(),
            "Table {:?} should exist after creation",
            table_path
        );

        let tables = admin.list_tables(test_db_name).await.unwrap();
        assert_eq!(
            tables.len(),
            1,
            "There should be exactly one table in the database"
        );
        assert!(
            tables.contains(&test_table_name.to_string()),
            "Table list should contain the created table"
        );

        let table_info = admin
            .get_table_info(&table_path)
            .await
            .expect("Failed to get table info");

        // verify table comment
        assert_eq!(
            table_info.get_comment(),
            Some("Test table for user data (id, name, age, email)"),
            "Table comment mismatch"
        );

        // verify schema columns
        let actual_schema = table_info.get_schema();
        assert_eq!(actual_schema, table_descriptor.schema(), "Schema mismatch");

        // verify primary key
        assert_eq!(
            table_info.get_primary_keys(),
            &vec!["id".to_string()],
            "Primary key columns mismatch"
        );

        // verify distribution and properties
        assert_eq!(table_info.get_num_buckets(), 3, "Bucket count mismatch");
        assert_eq!(
            table_info.get_bucket_keys(),
            &vec!["id".to_string()],
            "Bucket keys mismatch"
        );

        // The server may add extra default properties, so verify that all
        // expected properties are present rather than requiring an exact match.
        let actual_props = table_info.get_properties();
        for (key, value) in table_descriptor.properties() {
            assert_eq!(
                actual_props.get(key),
                Some(value),
                "Property mismatch for key '{}'",
                key
            );
        }

        // drop table
        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
        // table shouldn't exist now
        assert!(!admin.table_exists(&table_path).await.unwrap());

        // drop database
        admin
            .drop_database(test_db_name, false, true)
            .await
            .expect("Should drop database");

        // database shouldn't exist now
        assert!(!admin.database_exists(test_db_name).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_table_with_auto_increment_column() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin client");

        let test_db_name = "test_auto_increment_db";
        admin
            .create_database(test_db_name, None, true)
            .await
            .expect("Failed to create test database");

        let table_path = TablePath::new(test_db_name, "test_auto_increment_table");
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .column("seq", DataTypes::bigint())
                    .primary_key(vec!["id".to_string()])
                    .unwrap()
                    .enable_auto_increment("seq")
                    .expect("Failed to enable auto increment")
                    .build()
                    .expect("Failed to build table schema"),
            )
            .distributed_by(Some(1), vec!["id".to_string()])
            .property("table.replication.factor", "1")
            .build()
            .expect("Failed to build table descriptor");

        admin
            .create_table(&table_path, &table_descriptor, false)
            .await
            .expect("Failed to create test table");

        let table_info = admin
            .get_table_info(&table_path)
            .await
            .expect("Failed to get table info");
        assert_eq!(
            table_info.get_schema().auto_increment_col_names(),
            &vec!["seq".to_string()],
            "auto increment column should survive create_table and get_table_info"
        );

        // a full row write is not allowed on an auto increment table
        let table = connection.get_table(&table_path).await.unwrap();
        let error = match table
            .new_upsert()
            .expect("Failed to create upsert")
            .create_writer()
        {
            Ok(_) => panic!("A full row write should be rejected on an auto increment table"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("auto increment column"),
            "unexpected error: {error}"
        );

        admin
            .drop_table(&table_path, true)
            .await
            .expect("Failed to drop test table");
        admin
            .drop_database(test_db_name, true, true)
            .await
            .expect("Failed to drop test database");
    }

    #[tokio::test]
    async fn test_partition_apis() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin client");

        let test_db_name = "test_partition_apis_db";
        let db_descriptor = DatabaseDescriptorBuilder::default()
            .comment("Database for test_partition_apis")
            .build();

        admin
            .create_database(test_db_name, Some(&db_descriptor), true)
            .await
            .expect("Failed to create test database");

        let test_table_name = "partitioned_table";
        let table_path = TablePath::new(test_db_name, test_table_name);

        let table_schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .column("dt", DataTypes::string())
            .column("region", DataTypes::string())
            .primary_key(vec!["id", "dt", "region"])
            .unwrap()
            .build()
            .expect("Failed to build table schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(table_schema)
            .distributed_by(Some(3), vec!["id".to_string()])
            .partitioned_by(vec!["dt", "region"])
            .property("table.replication.factor", "1")
            .log_format(LogFormat::ARROW)
            .kv_format(KvFormat::COMPACTED)
            .build()
            .expect("Failed to build table descriptor");

        admin
            .create_table(&table_path, &table_descriptor, true)
            .await
            .expect("Failed to create partitioned table");

        let partitions = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partitions");
        assert!(
            partitions.is_empty(),
            "Expected no partitions initially, found {}",
            partitions.len()
        );

        let mut partition_values = HashMap::new();
        partition_values.insert("dt", "2024-01-15");
        partition_values.insert("region", "EMEA");
        let partition_spec = PartitionSpec::new(partition_values);

        admin
            .create_partition(&table_path, &partition_spec, false)
            .await
            .expect("Failed to create partition");

        let partitions = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partitions");
        assert_eq!(
            partitions.len(),
            1,
            "Expected exactly one partition after creation"
        );
        assert_eq!(
            partitions[0].get_partition_name(),
            "2024-01-15$EMEA",
            "Partition name mismatch"
        );

        // list with partial spec filter - should find the partition
        let mut partition_values = HashMap::new();
        partition_values.insert("dt", "2024-01-15");
        let partial_partition_spec = PartitionSpec::new(partition_values);

        let partitions_with_spec = admin
            .list_partition_infos_with_spec(&table_path, Some(&partial_partition_spec))
            .await
            .expect("Failed to list partitions with spec");
        assert_eq!(
            partitions_with_spec.len(),
            1,
            "Expected one partition matching the spec"
        );
        assert_eq!(
            partitions_with_spec[0].get_partition_name(),
            "2024-01-15$EMEA",
            "Partition name mismatch with spec filter"
        );

        // list with non-matching spec - should find no partitions
        let mut non_matching_values = HashMap::new();
        non_matching_values.insert("dt", "2024-01-16");
        let non_matching_spec = PartitionSpec::new(non_matching_values);
        let partitions_non_matching = admin
            .list_partition_infos_with_spec(&table_path, Some(&non_matching_spec))
            .await
            .expect("Failed to list partitions with non-matching spec");
        assert!(
            partitions_non_matching.is_empty(),
            "Expected no partitions for non-matching spec"
        );

        admin
            .drop_partition(&table_path, &partition_spec, false)
            .await
            .expect("Failed to drop partition");

        let partitions = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partitions");
        assert!(
            partitions.is_empty(),
            "Expected no partitions after drop, found {}",
            partitions.len()
        );

        admin
            .drop_table(&table_path, true)
            .await
            .expect("Failed to drop table");
        admin
            .drop_database(test_db_name, true, true)
            .await
            .expect("Should drop database");
    }

    #[tokio::test]
    async fn test_fluss_error_response() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin client");

        let table_path = TablePath::new("fluss", "not_exist");

        let result = admin.get_table_info(&table_path).await;
        assert!(result.is_err(), "Expected error but got Ok");

        let error = result.unwrap_err();
        assert_eq!(
            error.api_error(),
            Some(FlussError::TableNotExist),
            "Expected TableNotExist error, got {:?}",
            error
        );
    }

    /// Helper to assert that an error is a FlussAPIError with the expected code.
    fn assert_api_error(error: fluss::error::Error, expected: FlussError) {
        assert_eq!(
            error.api_error(),
            Some(expected),
            "Expected {:?}, got {:?}",
            expected,
            error
        );
    }

    #[tokio::test]
    async fn test_error_database_not_exist() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().unwrap();

        // get_database_info for non-existent database
        let result = admin.get_database_info("no_such_db").await;
        assert_api_error(result.unwrap_err(), FlussError::DatabaseNotExist);

        // drop_database without ignore flag
        let result = admin.drop_database("no_such_db", false, false).await;
        assert_api_error(result.unwrap_err(), FlussError::DatabaseNotExist);

        // list_tables for non-existent database
        let result = admin.list_tables("no_such_db").await;
        assert_api_error(result.unwrap_err(), FlussError::DatabaseNotExist);
    }

    #[tokio::test]
    async fn test_error_database_already_exist() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().unwrap();

        let db_name = "test_error_db_already_exist";
        let descriptor = DatabaseDescriptorBuilder::default().build();

        admin
            .create_database(db_name, Some(&descriptor), false)
            .await
            .unwrap();

        // create same database again without ignore flag
        let result = admin
            .create_database(db_name, Some(&descriptor), false)
            .await;
        assert_api_error(result.unwrap_err(), FlussError::DatabaseAlreadyExist);

        // with ignore flag should succeed
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .expect("create_database with ignore_if_exists should succeed");

        // cleanup
        admin.drop_database(db_name, true, true).await.unwrap();
    }

    #[tokio::test]
    async fn test_error_table_already_exist() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().unwrap();

        let db_name = "test_error_tbl_already_exist_db";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let table_path = TablePath::new(db_name, "my_table");
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .build()
            .unwrap();
        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec![])
            .property("table.replication.factor", "1")
            .build()
            .unwrap();

        admin
            .create_table(&table_path, &table_descriptor, false)
            .await
            .unwrap();

        // create same table again without ignore flag
        let result = admin
            .create_table(&table_path, &table_descriptor, false)
            .await;
        assert_api_error(result.unwrap_err(), FlussError::TableAlreadyExist);

        // with ignore flag should succeed
        admin
            .create_table(&table_path, &table_descriptor, true)
            .await
            .expect("create_table with ignore_if_exists should succeed");

        // cleanup
        admin.drop_table(&table_path, true).await.unwrap();
        admin.drop_database(db_name, true, true).await.unwrap();
    }

    #[tokio::test]
    async fn test_error_table_not_exist() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().unwrap();

        let table_path = TablePath::new("fluss", "no_such_table");

        // drop without ignore flag
        let result = admin.drop_table(&table_path, false).await;
        assert_api_error(result.unwrap_err(), FlussError::TableNotExist);

        // drop with ignore flag should succeed
        admin
            .drop_table(&table_path, true)
            .await
            .expect("drop_table with ignore_if_not_exists should succeed");
    }

    #[tokio::test]
    async fn test_get_server_nodes() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().unwrap();

        let nodes = admin
            .get_server_nodes()
            .await
            .expect("should get server nodes");

        assert!(
            !nodes.is_empty(),
            "Expected at least one server node in the cluster"
        );

        let has_coordinator = nodes
            .iter()
            .any(|n| *n.server_type() == fluss::ServerType::CoordinatorServer);
        assert!(has_coordinator, "Expected a coordinator server node");

        let tablet_count = nodes
            .iter()
            .filter(|n| *n.server_type() == fluss::ServerType::TabletServer)
            .count();
        assert!(
            tablet_count >= 1,
            "Expected at least one tablet server node"
        );

        for node in &nodes {
            assert!(
                !node.host().is_empty(),
                "Server node host should not be empty"
            );
            assert!(node.port() > 0, "Server node port should be > 0");
            assert!(
                !node.uid().is_empty(),
                "Server node uid should not be empty"
            );
        }
    }

    #[tokio::test]
    async fn test_bootstrap_from_tablet_server() {
        let cluster = get_shared_cluster();
        let bootstrap_servers = cluster
            .plaintext_tablet_bootstrap_server(0)
            .expect("shared cluster should expose tablet server bootstrap")
            .to_string();

        let connection = FlussConnection::new(Config {
            writer_acks: "all".to_string(),
            bootstrap_servers,
            ..Default::default()
        })
        .await
        .expect("should bootstrap from tablet server");
        let admin = connection.get_admin().expect("should get admin");

        let nodes = admin
            .get_server_nodes()
            .await
            .expect("should get server nodes");

        let has_coordinator = nodes
            .iter()
            .any(|n| *n.server_type() == fluss::ServerType::CoordinatorServer);
        assert!(has_coordinator, "Expected a coordinator server node");

        let has_tablet = nodes
            .iter()
            .any(|n| *n.server_type() == fluss::ServerType::TabletServer);
        assert!(has_tablet, "Expected a tablet server node");
    }

    #[tokio::test]
    async fn test_bootstrap_server_failover() {
        let cluster = get_shared_cluster();
        let bootstrap_servers = format!("127.0.0.1:1,{}", cluster.plaintext_bootstrap_servers());

        FlussConnection::new(Config {
            bootstrap_servers,
            ..Default::default()
        })
        .await
        .expect("should fall back to the second bootstrap server");
    }

    #[tokio::test]
    async fn test_error_table_not_partitioned() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().unwrap();

        let db_name = "test_error_not_partitioned_db";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let table_path = TablePath::new(db_name, "non_partitioned_table");
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .build()
            .unwrap();
        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec![])
            .property("table.replication.factor", "1")
            .build()
            .unwrap();

        admin
            .create_table(&table_path, &table_descriptor, false)
            .await
            .unwrap();

        // list_partition_infos on non-partitioned table
        let result = admin.list_partition_infos(&table_path).await;
        assert_api_error(
            result.unwrap_err(),
            FlussError::TableNotPartitionedException,
        );

        // cleanup
        admin.drop_table(&table_path, true).await.unwrap();
        admin.drop_database(db_name, true, true).await.unwrap();
    }

    async fn row_count(admin: &FlussAdmin, table_id: i64, buckets: Vec<BucketStatsRequest>) -> i64 {
        let stats = admin
            .get_table_stats(table_id, buckets, vec![])
            .await
            .expect("table stats");
        stats
            .buckets
            .iter()
            .map(|bucket| {
                assert_eq!(bucket.error, None, "{bucket:?}");
                bucket.row_count.expect("row count")
            })
            .sum()
    }

    fn id_row(id: i32, name: &str) -> GenericRow<'static> {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, name.to_string());
        row
    }

    #[tokio::test]
    async fn test_get_table_stats() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");
        let buckets = |partition_id| {
            (0..3)
                .map(|bucket| BucketStatsRequest::new(partition_id, bucket))
                .collect::<Vec<_>>()
        };

        let log_path = TablePath::new("fluss", "test_table_stats_log");
        let log_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .build()
                    .unwrap(),
            )
            .distributed_by(Some(3), vec!["id".to_string()])
            .build()
            .unwrap();
        create_table(&admin, &log_path, &log_descriptor).await;
        wait_for_table_buckets_ready(&admin, &log_path, &[0, 1, 2]).await;
        let log_table = connection.get_table(&log_path).await.unwrap();
        let writer = log_table.new_append().unwrap().create_writer().unwrap();
        for id in 0..7 {
            writer.append(&id_row(id, "v")).unwrap();
        }
        writer.flush().await.unwrap();
        let table_id = log_table.get_table_info().table_id;
        assert_eq!(row_count(&admin, table_id, buckets(None)).await, 7);

        let pk_path = TablePath::new("fluss", "test_table_stats_pk");
        let pk_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .primary_key(vec!["id"])
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .distributed_by(Some(3), vec![])
            .build()
            .unwrap();
        create_table(&admin, &pk_path, &pk_descriptor).await;
        wait_for_table_buckets_ready(&admin, &pk_path, &[0, 1, 2]).await;
        let pk_table = connection.get_table(&pk_path).await.unwrap();
        let writer = pk_table.new_upsert().unwrap().create_writer().unwrap();
        for id in (0..10).chain(0..3) {
            writer.upsert(&id_row(id, "v")).unwrap();
        }
        writer.flush().await.unwrap();
        let table_id = pk_table.get_table_info().table_id;
        assert_eq!(row_count(&admin, table_id, buckets(None)).await, 10);

        let partitioned_path = TablePath::new("fluss", "test_table_stats_partitioned");
        let partitioned_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .primary_key(vec!["id", "name"])
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .partitioned_by(vec!["name"])
            .distributed_by(Some(3), vec![])
            .build()
            .unwrap();
        create_table(&admin, &partitioned_path, &partitioned_descriptor).await;
        create_partitions(&admin, &partitioned_path, "name", &["a", "b"]).await;
        wait_for_partitions_ready(&admin, &partitioned_path, &["a", "b"]).await;
        let partitioned_table = connection.get_table(&partitioned_path).await.unwrap();
        let writer = partitioned_table
            .new_upsert()
            .unwrap()
            .create_writer()
            .unwrap();
        for id in 0..5 {
            writer.upsert(&id_row(id, "a")).unwrap();
            writer.upsert(&id_row(id, "b")).unwrap();
        }
        writer.flush().await.unwrap();
        let table_id = partitioned_table.get_table_info().table_id;
        let partition_buckets = admin
            .list_partition_infos(&partitioned_path)
            .await
            .unwrap()
            .iter()
            .flat_map(|partition| buckets(Some(partition.get_partition_id())))
            .collect();
        assert_eq!(row_count(&admin, table_id, partition_buckets).await, 10);

        for path in [&log_path, &pk_path, &partitioned_path] {
            admin.drop_table(path, true).await.unwrap();
        }
    }
}
