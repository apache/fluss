/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#[cfg(test)]
mod multi_table_writer_test {
    use crate::integration::utils::{
        DEFAULT_POLL_TIMEOUT, create_table, get_shared_cluster, poll_until_count,
    };
    use fluss::client::{EARLIEST_OFFSET, MultiTableWriteRecord};
    use fluss::metadata::{
        AddColumn, AlterTableChanges, ColumnPositionType, DataTypes, JsonSerde, Schema,
        TableDescriptor, TablePath,
    };
    use fluss::row::{DataGetters, GenericRow};
    use std::time::Duration;

    fn row(id: i32, value: &str) -> GenericRow<'_> {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, value);
        row
    }

    #[tokio::test]
    async fn writes_cdc_records_to_multiple_tables() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");
        let log_path = TablePath::new("fluss", "test_multi_writer_log");
        let first_pk_path = TablePath::new("fluss", "test_multi_writer_pk_1");
        let second_pk_path = TablePath::new("fluss", "test_multi_writer_pk_2");

        let log_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("value", DataTypes::string())
                    .build()
                    .expect("log schema"),
            )
            .build()
            .expect("log table");
        let pk_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("value", DataTypes::string())
                    .primary_key(vec!["id"])
                    .build()
                    .expect("pk schema"),
            )
            .build()
            .expect("pk table");
        create_table(&admin, &log_path, &log_descriptor).await;
        create_table(&admin, &first_pk_path, &pk_descriptor).await;
        create_table(&admin, &second_pk_path, &pk_descriptor).await;

        let log_schema_id = admin
            .get_table_info(&log_path)
            .await
            .expect("log info")
            .get_schema_id();
        let first_pk_schema_id = admin
            .get_table_info(&first_pk_path)
            .await
            .expect("first pk info")
            .get_schema_id();
        let second_pk_schema_id = admin
            .get_table_info(&second_pk_path)
            .await
            .expect("second pk info")
            .get_schema_id();

        let mut writer = connection
            .new_multi_table_writer()
            .expect("multi-table writer");
        let log_row = row(1, "created");
        let first_pk_row = row(1, "updated");
        let second_pk_row = row(2, "deleted");

        writer
            .write(MultiTableWriteRecord::for_append(
                &log_path,
                &log_row,
                log_schema_id,
            ))
            .await
            .expect("append");
        writer
            .write(MultiTableWriteRecord::for_upsert(
                &first_pk_path,
                &first_pk_row,
                first_pk_schema_id,
            ))
            .await
            .expect("first upsert");
        writer
            .write(MultiTableWriteRecord::for_upsert(
                &second_pk_path,
                &second_pk_row,
                second_pk_schema_id,
            ))
            .await
            .expect("second upsert");
        writer
            .write(MultiTableWriteRecord::for_delete(
                &second_pk_path,
                &second_pk_row,
                second_pk_schema_id,
            ))
            .await
            .expect("delete");

        let invalid_operation = writer
            .write(MultiTableWriteRecord::for_upsert(
                &log_path,
                &log_row,
                log_schema_id,
            ))
            .await
            .expect_err("upsert on log table should fail");
        assert!(format!("{invalid_operation}").contains("not supported for a log table"));

        let invalid_append = writer
            .write(MultiTableWriteRecord::for_append(
                &first_pk_path,
                &first_pk_row,
                first_pk_schema_id,
            ))
            .await
            .expect_err("append on primary-key table should fail");
        assert!(format!("{invalid_append}").contains("not supported for a primary-key table"));

        let ahead_schema = writer
            .write(MultiTableWriteRecord::for_upsert(
                &first_pk_path,
                &first_pk_row,
                first_pk_schema_id + 1,
            ))
            .await
            .expect_err("ahead schema id should fail");
        assert!(format!("{ahead_schema}").contains("ahead of the current table schema id"));

        writer.flush().await.expect("flush all tables");

        let log_table = connection.get_table(&log_path).await.expect("log table");
        let log_scanner = log_table
            .new_scan()
            .create_log_scanner()
            .expect("log scanner");
        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("subscribe log table");
        let log_rows = poll_until_count(
            1,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |timeout| {
                log_scanner
                    .poll(timeout)
                    .await
                    .expect("poll log table")
                    .into_iter()
                    .map(|record| record.row().get_string(1).expect("log value").to_string())
                    .collect()
            },
        )
        .await;
        assert_eq!(log_rows, vec!["created"]);

        let extra_type_json = serde_json::to_vec(
            &DataTypes::string()
                .serialize_json()
                .expect("serialize string type"),
        )
        .expect("serialize add-column payload");
        admin
            .alter_table(
                &log_path,
                false,
                AlterTableChanges {
                    add_columns: vec![AddColumn {
                        column_name: "extra".to_string(),
                        data_type_json: extra_type_json,
                        comment: None,
                        position: ColumnPositionType::Last,
                    }],
                    ..Default::default()
                },
            )
            .await
            .expect("alter log table");
        let latest_log_schema_id = admin
            .get_table_info(&log_path)
            .await
            .expect("updated log info")
            .get_schema_id();

        let mut schema_writer = connection
            .new_multi_table_writer()
            .expect("schema-aware writer");
        let old_schema_row = row(2, "old-schema");
        let mut new_schema_row = GenericRow::new(3);
        new_schema_row.set_field(0, 3);
        new_schema_row.set_field(1, "new-schema");
        new_schema_row.set_field(2, "extra");
        schema_writer
            .write(MultiTableWriteRecord::for_append(
                &log_path,
                &old_schema_row,
                log_schema_id,
            ))
            .await
            .expect("historical schema append");
        schema_writer
            .write(MultiTableWriteRecord::for_append(
                &log_path,
                &new_schema_row,
                latest_log_schema_id,
            ))
            .await
            .expect("latest schema append");
        schema_writer.flush().await.expect("flush schema writes");

        let mut schema_ids = poll_until_count(
            2,
            DEFAULT_POLL_TIMEOUT,
            Duration::from_millis(500),
            async |timeout| {
                log_scanner
                    .poll(timeout)
                    .await
                    .expect("poll schema writes")
                    .into_iter()
                    .map(|record| record.row().get_int(0).expect("log id"))
                    .collect()
            },
        )
        .await;
        schema_ids.sort();
        assert_eq!(schema_ids, vec![2, 3]);

        let first_pk_table = connection
            .get_table(&first_pk_path)
            .await
            .expect("first pk");
        let mut first_lookup = first_pk_table
            .new_lookup()
            .expect("first lookup")
            .create_lookuper()
            .expect("first lookuper");
        let mut key = GenericRow::new(2);
        key.set_field(0, 1);
        assert!(
            first_lookup
                .lookup(&key)
                .await
                .expect("first lookup result")
                .get_single_row()
                .expect("first row decode")
                .is_some()
        );

        let second_pk_table = connection
            .get_table(&second_pk_path)
            .await
            .expect("second pk");
        let mut second_lookup = second_pk_table
            .new_lookup()
            .expect("second lookup")
            .create_lookuper()
            .expect("second lookuper");
        key.set_field(0, 2);
        assert!(
            second_lookup
                .lookup(&key)
                .await
                .expect("second lookup result")
                .get_single_row()
                .expect("second row decode")
                .is_none()
        );
    }
}
