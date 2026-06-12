/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
mod kv_table_test {
    use crate::integration::utils::{
        ColumnPlan, array_dt_basics_columns, as_row_type, create_partitions, create_table,
        dt_array_int, dt_map_string_int, dt_row_seq_label, get_shared_cluster, make_int_array,
        make_string_array, map_dt_basics_columns, row_dt_basics_columns, scalar_dt_columns,
    };
    use fluss::client::TableUpsert;
    use fluss::metadata::{DataField, DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::row::binary_array::FlussArrayWriter;
    use fluss::row::binary_map::FlussMapWriter;
    use fluss::row::{
        Date, Datum, Decimal, GenericRow, InternalRow, Time, TimestampLtz, TimestampNtz,
    };
    use futures::stream::{FuturesUnordered, StreamExt};

    fn make_key(id: i32) -> GenericRow<'static> {
        make_key_with_field_count(id, 3)
    }

    fn make_key_with_field_count(id: i32, field_count: usize) -> GenericRow<'static> {
        let mut row = GenericRow::new(field_count);
        row.set_field(0, id);
        row
    }

    #[tokio::test]
    async fn upsert_delete_and_lookup() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().unwrap();

        let table_path = TablePath::new("fluss", "test_upsert_and_lookup");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .column("age", DataTypes::bigint())
                    .primary_key(vec!["id"])
                    .build()
                    .expect("Failed to build schema"),
            )
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection.get_table(&table_path).await.unwrap();

        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let upsert_writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        let test_data = [(1, "Verso", 32i64), (2, "Noco", 25), (3, "Esquie", 35)];

        // Upsert rows (fire-and-forget, then flush)
        for (id, name, age) in &test_data {
            let mut row = GenericRow::new(3);
            row.set_field(0, *id);
            row.set_field(1, *name);
            row.set_field(2, *age);
            upsert_writer.upsert(&row).expect("Failed to upsert row");
        }
        upsert_writer.flush().await.expect("Failed to flush");

        // Lookup records
        let mut lookuper = table
            .new_lookup()
            .expect("Failed to create lookup")
            .create_lookuper()
            .expect("Failed to create lookuper");

        // Verify lookup results
        for (id, expected_name, expected_age) in &test_data {
            let result = lookuper
                .lookup(&make_key(*id))
                .await
                .expect("Failed to lookup");
            let row = result.get_single_row().unwrap().expect("Row should exist");

            assert_eq!(row.get_int(0).unwrap(), *id, "id mismatch");
            assert_eq!(row.get_string(1).unwrap(), *expected_name, "name mismatch");
            assert_eq!(row.get_long(2).unwrap(), *expected_age, "age mismatch");
        }

        // Update the record with new age (await acknowledgment)
        let mut updated_row = GenericRow::new(3);
        updated_row.set_field(0, 1);
        updated_row.set_field(1, "Verso");
        updated_row.set_field(2, 33i64);
        upsert_writer
            .upsert(&updated_row)
            .expect("Failed to upsert updated row")
            .await
            .expect("Failed to wait for upsert acknowledgment");

        // Verify the update
        let result = lookuper
            .lookup(&make_key(1))
            .await
            .expect("Failed to lookup after update");
        let found_row = result.get_single_row().unwrap().expect("Row should exist");
        assert_eq!(
            found_row.get_long(2).unwrap(),
            updated_row.get_long(2).unwrap(),
            "Age should be updated"
        );
        assert_eq!(
            found_row.get_string(1).unwrap(),
            updated_row.get_string(1).unwrap(),
            "Name should remain unchanged"
        );

        // Delete record with id=1 (await acknowledgment)
        let mut delete_row = GenericRow::new(3);
        delete_row.set_field(0, 1);
        upsert_writer
            .delete(&delete_row)
            .expect("Failed to delete")
            .await
            .expect("Failed to wait for delete acknowledgment");

        // Verify deletion
        let result = lookuper
            .lookup(&make_key(1))
            .await
            .expect("Failed to lookup deleted record");
        assert!(
            result.get_single_row().unwrap().is_none(),
            "Record 1 should not exist after delete"
        );

        // Verify other records still exist
        for i in [2, 3] {
            let result = lookuper
                .lookup(&make_key(i))
                .await
                .expect("Failed to lookup");
            assert!(
                result.get_single_row().unwrap().is_some(),
                "Record {} should still exist after deleting record 1",
                i
            );
        }

        // Lookup non-existent key
        let result = lookuper
            .lookup(&make_key(999))
            .await
            .expect("Failed to lookup non-existent key");
        assert!(
            result.get_single_row().unwrap().is_none(),
            "Non-existent key should return None"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn composite_primary_keys() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().unwrap();

        let table_path = TablePath::new("fluss", "test_composite_pk");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("region", DataTypes::string())
                    .column("user_id", DataTypes::int())
                    .column("score", DataTypes::bigint())
                    .primary_key(vec!["region", "user_id"])
                    .build()
                    .expect("Failed to build schema"),
            )
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection.get_table(&table_path).await.unwrap();

        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let upsert_writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        // Insert records with composite keys
        let test_data = [
            ("US", 1, 100i64),
            ("US", 2, 200i64),
            ("EU", 1, 150i64),
            ("EU", 2, 250i64),
        ];

        for (region, user_id, score) in &test_data {
            let mut row = GenericRow::new(3);
            row.set_field(0, *region);
            row.set_field(1, *user_id);
            row.set_field(2, *score);
            upsert_writer.upsert(&row).expect("Failed to upsert");
        }
        upsert_writer.flush().await.expect("Failed to flush");

        // Lookup with composite key
        let mut lookuper = table
            .new_lookup()
            .expect("Failed to create lookup")
            .create_lookuper()
            .expect("Failed to create lookuper");

        // Lookup (US, 1) - should return score 100
        let mut key = GenericRow::new(3);
        key.set_field(0, "US");
        key.set_field(1, 1);
        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        let row = result.get_single_row().unwrap().expect("Row should exist");
        assert_eq!(
            row.get_long(2).unwrap(),
            100,
            "Score for (US, 1) should be 100"
        );

        // Lookup (EU, 2) - should return score 250
        let mut key = GenericRow::new(3);
        key.set_field(0, "EU");
        key.set_field(1, 2);
        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        let row = result.get_single_row().unwrap().expect("Row should exist");
        assert_eq!(
            row.get_long(2).unwrap(),
            250,
            "Score for (EU, 2) should be 250"
        );

        // Update (US, 1) score (await acknowledgment)
        let mut update_row = GenericRow::new(3);
        update_row.set_field(0, "US");
        update_row.set_field(1, 1);
        update_row.set_field(2, 500i64);
        upsert_writer
            .upsert(&update_row)
            .expect("Failed to update")
            .await
            .expect("Failed to wait for update acknowledgment");

        // Verify update
        let mut key = GenericRow::new(3);
        key.set_field(0, "US");
        key.set_field(1, 1);
        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        let row = result.get_single_row().unwrap().expect("Row should exist");
        assert_eq!(
            row.get_long(2).unwrap(),
            update_row.get_long(2).unwrap(),
            "Row score should be updated"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Partial-update preserves columns absent from the partial-write set.
    #[tokio::test]
    async fn partial_update() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_partial_update");

        let nested_type = DataTypes::row(vec![
            DataField::new("seq", DataTypes::int(), None),
            DataField::new("label", DataTypes::string(), None),
        ]);

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .column("score", DataTypes::bigint())
                    .column("nested", nested_type)
                    .column(
                        "attrs",
                        DataTypes::map(DataTypes::string(), DataTypes::int()),
                    )
                    .column("tags", DataTypes::array(DataTypes::string()))
                    .primary_key(vec!["id"])
                    .build()
                    .expect("schema"),
            )
            .build()
            .expect("table descriptor");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let table_upsert = table.new_upsert().expect("upsert");
        let upsert_writer = table_upsert.create_writer().expect("writer");

        let mut nested0 = GenericRow::new(2);
        nested0.set_field(0, 10_i32);
        nested0.set_field(1, "alpha");
        let attrs0 = {
            let mut w = FlussMapWriter::new(2, &DataTypes::string(), &DataTypes::int());
            w.write_entry("a".into(), 1.into()).unwrap();
            w.write_entry("b".into(), 2.into()).unwrap();
            w.complete().expect("attrs0")
        };
        let tags0 = make_string_array(&[Some("alpha-tag"), Some("beta-tag")]);

        let mut row = GenericRow::new(6);
        row.set_field(0, 1);
        row.set_field(1, "Verso");
        row.set_field(2, 100i64);
        row.set_field(3, Datum::Row(Box::new(nested0)));
        row.set_field(4, Datum::Map(attrs0));
        row.set_field(5, tags0);
        upsert_writer
            .upsert(&row)
            .expect("upsert initial")
            .await
            .expect("ack initial");

        let mut lookuper = table
            .new_lookup()
            .expect("lookup")
            .create_lookuper()
            .expect("lookuper");

        // Helper to issue a partial upsert against a specific column set.
        async fn partial_upsert(table_upsert: &TableUpsert, cols: &[&str], row: GenericRow<'_>) {
            let pu = table_upsert
                .partial_update_with_column_names(cols)
                .expect("partial upsert");
            let pw = pu.create_writer().expect("partial writer");
            pw.upsert(&row)
                .expect("partial upsert")
                .await
                .expect("partial ack");
        }

        // === Partial update on a scalar column — compound columns preserved ===
        let mut p1 = GenericRow::new(6);
        p1.set_field(0, 1);
        p1.set_field(1, Datum::Null);
        p1.set_field(2, 420i64);
        p1.set_field(3, Datum::Null);
        p1.set_field(4, Datum::Null);
        p1.set_field(5, Datum::Null);
        partial_upsert(&table_upsert, &["id", "score"], p1).await;

        let result = lookuper.lookup(&make_key(1)).await.expect("lookup");
        let r = result
            .get_single_row()
            .expect("get row")
            .expect("row exists");
        assert_eq!(r.get_string(1).unwrap(), "Verso", "name preserved");
        assert_eq!(r.get_long(2).unwrap(), 420, "score updated");
        let n = r.get_row(3).unwrap();
        assert_eq!(n.get_int(0).unwrap(), 10, "ROW preserved");
        assert_eq!(r.get_map(4).unwrap().size(), 2, "MAP preserved");
        assert_eq!(r.get_array(5).unwrap().size(), 2, "ARRAY preserved");

        // === Partial update on the ROW column ===
        let mut new_nested = GenericRow::new(2);
        new_nested.set_field(0, 99_i32);
        new_nested.set_field(1, "omega");
        let mut p2 = GenericRow::new(6);
        p2.set_field(0, 1);
        p2.set_field(1, Datum::Null);
        p2.set_field(2, Datum::Null);
        p2.set_field(3, Datum::Row(Box::new(new_nested)));
        p2.set_field(4, Datum::Null);
        p2.set_field(5, Datum::Null);
        partial_upsert(&table_upsert, &["id", "nested"], p2).await;

        let result = lookuper.lookup(&make_key(1)).await.expect("lookup");
        let r = result
            .get_single_row()
            .expect("get row")
            .expect("row exists");
        assert_eq!(r.get_string(1).unwrap(), "Verso", "name preserved");
        assert_eq!(r.get_long(2).unwrap(), 420, "score preserved");
        let n = r.get_row(3).unwrap();
        assert_eq!(n.get_int(0).unwrap(), 99);
        assert_eq!(n.get_string(1).unwrap(), "omega");
        assert_eq!(r.get_map(4).unwrap().size(), 2, "MAP preserved");
        assert_eq!(r.get_array(5).unwrap().size(), 2, "ARRAY preserved");

        // === Partial update on the MAP column ===
        let new_attrs = {
            let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::int());
            w.write_entry("z".into(), 99.into()).unwrap();
            w.complete().expect("new_attrs")
        };
        let mut p3 = GenericRow::new(6);
        p3.set_field(0, 1);
        p3.set_field(1, Datum::Null);
        p3.set_field(2, Datum::Null);
        p3.set_field(3, Datum::Null);
        p3.set_field(4, Datum::Map(new_attrs));
        p3.set_field(5, Datum::Null);
        partial_upsert(&table_upsert, &["id", "attrs"], p3).await;

        let result = lookuper.lookup(&make_key(1)).await.expect("lookup");
        let r = result
            .get_single_row()
            .expect("get row")
            .expect("row exists");
        assert_eq!(r.get_string(1).unwrap(), "Verso", "name preserved");
        let n = r.get_row(3).unwrap();
        assert_eq!(n.get_int(0).unwrap(), 99, "ROW preserved");
        let m = r.get_map(4).unwrap();
        assert_eq!(m.size(), 1);
        assert_eq!(m.get(&Datum::from("z")).unwrap(), Some(Datum::from(99_i32)));
        assert_eq!(r.get_array(5).unwrap().size(), 2, "ARRAY preserved");

        // === Partial update on the ARRAY column ===
        let new_tags = make_string_array(&[Some("gamma-tag")]);
        let mut p4 = GenericRow::new(6);
        p4.set_field(0, 1);
        p4.set_field(1, Datum::Null);
        p4.set_field(2, Datum::Null);
        p4.set_field(3, Datum::Null);
        p4.set_field(4, Datum::Null);
        p4.set_field(5, new_tags);
        partial_upsert(&table_upsert, &["id", "tags"], p4).await;

        let result = lookuper.lookup(&make_key(1)).await.expect("lookup");
        let r = result
            .get_single_row()
            .expect("get row")
            .expect("row exists");
        assert_eq!(r.get_string(1).unwrap(), "Verso", "name preserved");
        let n = r.get_row(3).unwrap();
        assert_eq!(n.get_int(0).unwrap(), 99, "ROW preserved");
        assert_eq!(r.get_map(4).unwrap().size(), 1, "MAP preserved");
        let a = r.get_array(5).unwrap();
        assert_eq!(a.size(), 1);
        assert_eq!(a.get_string(0).unwrap(), "gamma-tag");

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    /// Partitioned KV upsert + lookup against every compound type.
    #[tokio::test]
    async fn partitioned_table_upsert_and_lookup() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_partitioned_kv_table");

        let nested_type = DataTypes::row(vec![
            DataField::new("seq", DataTypes::int(), None),
            DataField::new("label", DataTypes::string(), None),
        ]);

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("region", DataTypes::string())
                    .column("user_id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .column("score", DataTypes::bigint())
                    .column("nested", nested_type)
                    .column(
                        "attrs",
                        DataTypes::map(DataTypes::string(), DataTypes::int()),
                    )
                    .column("tags", DataTypes::array(DataTypes::string()))
                    .primary_key(vec!["region", "user_id"])
                    .build()
                    .expect("schema"),
            )
            .partitioned_by(vec!["region"])
            .build()
            .expect("table descriptor");

        create_table(&admin, &table_path, &table_descriptor).await;
        create_partitions(&admin, &table_path, "region", &["US", "EU", "APAC"]).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let table_upsert = table.new_upsert().expect("upsert");
        let upsert_writer = table_upsert.create_writer().expect("writer");

        let test_data = [
            ("US", 1_i32, "Gustave", 100_i64, 11_i32, "a", 1_i32, "alpha"),
            ("US", 2, "Lune", 200, 22, "b", 2, "beta"),
            ("EU", 1, "Sciel", 150, 33, "c", 3, "gamma"),
            ("EU", 2, "Maelle", 250, 44, "d", 4, "delta"),
            ("APAC", 1, "Noco", 300, 55, "e", 5, "epsilon"),
        ];

        for (region, user_id, name, score, seq, label, attr_v, tag) in &test_data {
            let mut nested = GenericRow::new(2);
            nested.set_field(0, *seq);
            nested.set_field(1, *label);
            let attrs = {
                let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::int());
                w.write_entry((*label).into(), (*attr_v).into()).unwrap();
                w.complete().expect("attrs")
            };
            let tags = make_string_array(&[Some(*tag)]);

            let mut row = GenericRow::new(7);
            row.set_field(0, *region);
            row.set_field(1, *user_id);
            row.set_field(2, *name);
            row.set_field(3, *score);
            row.set_field(4, Datum::Row(Box::new(nested)));
            row.set_field(5, Datum::Map(attrs));
            row.set_field(6, tags);
            upsert_writer.upsert(&row).expect("upsert");
        }
        upsert_writer.flush().await.expect("flush");

        let mut lookuper = table
            .new_lookup()
            .expect("lookup")
            .create_lookuper()
            .expect("lookuper");

        // === Per-partition lookup verifies all compound columns ===
        for (region, user_id, name, score, seq, label, attr_v, tag) in &test_data {
            let mut key = GenericRow::new(7);
            key.set_field(0, *region);
            key.set_field(1, *user_id);

            let result = lookuper.lookup(&key).await.expect("lookup");
            let row = result
                .get_single_row()
                .expect("get row")
                .expect("row exists");

            assert_eq!(row.get_string(0).unwrap(), *region);
            assert_eq!(row.get_int(1).unwrap(), *user_id);
            assert_eq!(row.get_string(2).unwrap(), *name);
            assert_eq!(row.get_long(3).unwrap(), *score);
            let nested = row.get_row(4).unwrap();
            assert_eq!(nested.get_int(0).unwrap(), *seq);
            assert_eq!(nested.get_string(1).unwrap(), *label);
            let attrs = row.get_map(5).unwrap();
            assert_eq!(attrs.size(), 1);
            assert_eq!(
                attrs.get(&Datum::from(*label)).unwrap(),
                Some(Datum::from(*attr_v))
            );
            let tags = row.get_array(6).unwrap();
            assert_eq!(tags.size(), 1);
            assert_eq!(tags.get_string(0).unwrap(), *tag);
        }

        // === Update a row in US partition ===
        let mut updated_nested = GenericRow::new(2);
        updated_nested.set_field(0, 999_i32);
        updated_nested.set_field(1, "updated");
        let updated_attrs = {
            let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::int());
            w.write_entry("u".into(), 999.into()).unwrap();
            w.complete().expect("updated_attrs")
        };
        let updated_tags = make_string_array(&[Some("renamed")]);
        let mut updated_row = GenericRow::new(7);
        updated_row.set_field(0, "US");
        updated_row.set_field(1, 1);
        updated_row.set_field(2, "Gustave Updated");
        updated_row.set_field(3, 999_i64);
        updated_row.set_field(4, Datum::Row(Box::new(updated_nested)));
        updated_row.set_field(5, Datum::Map(updated_attrs));
        updated_row.set_field(6, updated_tags);
        upsert_writer
            .upsert(&updated_row)
            .expect("upsert updated")
            .await
            .expect("ack updated");

        let mut key = GenericRow::new(7);
        key.set_field(0, "US");
        key.set_field(1, 1);
        let result = lookuper.lookup(&key).await.expect("lookup");
        let row = result
            .get_single_row()
            .expect("get row")
            .expect("row exists");
        assert_eq!(row.get_string(2).unwrap(), "Gustave Updated");
        assert_eq!(row.get_long(3).unwrap(), 999);
        let nested = row.get_row(4).unwrap();
        assert_eq!(nested.get_int(0).unwrap(), 999);
        let attrs = row.get_map(5).unwrap();
        assert_eq!(
            attrs.get(&Datum::from("u")).unwrap(),
            Some(Datum::from(999_i32))
        );
        let tags = row.get_array(6).unwrap();
        assert_eq!(tags.get_string(0).unwrap(), "renamed");

        // === Lookup in non-existent partition returns None ===
        let mut missing = GenericRow::new(7);
        missing.set_field(0, "UNKNOWN_REGION");
        missing.set_field(1, 1);
        let result = lookuper
            .lookup(&missing)
            .await
            .expect("lookup unknown partition");
        assert!(result.get_single_row().expect("get").is_none());

        // === Delete a row within a partition ===
        let mut delete_key = GenericRow::new(7);
        delete_key.set_field(0, "EU");
        delete_key.set_field(1, 1);
        upsert_writer
            .delete(&delete_key)
            .expect("delete")
            .await
            .expect("ack delete");
        let mut key = GenericRow::new(7);
        key.set_field(0, "EU");
        key.set_field(1, 1);
        let result = lookuper.lookup(&key).await.expect("lookup");
        assert!(result.get_single_row().expect("get").is_none());

        // === Sibling row in same partition still exists ===
        let mut key = GenericRow::new(7);
        key.set_field(0, "EU");
        key.set_field(1, 2);
        let result = lookuper.lookup(&key).await.expect("lookup");
        let row = result
            .get_single_row()
            .expect("get row")
            .expect("row exists");
        assert_eq!(row.get_string(2).unwrap(), "Maelle");
        assert_eq!(row.get_array(6).unwrap().get_string(0).unwrap(), "delta");

        admin.drop_table(&table_path, false).await.expect("drop");
    }

    /// Integration test covering put and get operations for all supported datatypes.
    /// Integration test for concurrent batched lookups across partitions.
    #[tokio::test]
    async fn batched_concurrent_lookups_partitioned() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_batched_lookups_partitioned");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("region", DataTypes::string())
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .primary_key(vec!["region", "id"])
                    .build()
                    .expect("Failed to build schema"),
            )
            .partitioned_by(vec!["region"])
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;
        create_partitions(&admin, &table_path, "region", &["US", "EU", "APAC"]).await;

        let connection = cluster.get_fluss_connection().await;
        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        // Insert records across all partitions
        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        let regions = ["US", "EU", "APAC"];
        for region in &regions {
            for id in 0..5i32 {
                let mut row = GenericRow::new(3);
                row.set_field(0, *region);
                row.set_field(1, id);
                row.set_field(2, format!("{}-{}", region, id));
                writer.upsert(&row).expect("Failed to upsert");
            }
        }
        writer.flush().await.expect("Failed to flush");

        let mut lookupers: Vec<_> = (0..regions.len() * 5)
            .map(|_| {
                table
                    .new_lookup()
                    .expect("Failed to create lookup")
                    .create_lookuper()
                    .expect("Failed to create lookuper")
            })
            .collect();

        let mut futures = FuturesUnordered::new();
        for (i, lookuper) in lookupers.iter_mut().enumerate() {
            let region = regions[i / 5];
            let id = (i % 5) as i32;

            futures.push(async move {
                let mut key = GenericRow::new(3);
                key.set_field(0, region);
                key.set_field(1, id);

                let result = lookuper.lookup(&key).await.expect("Failed to lookup");
                let row = result
                    .get_single_row()
                    .expect("Failed to get row")
                    .expect("Row should exist");

                let actual_region = row.get_string(0).unwrap();
                let actual_id = row.get_int(1).unwrap();
                let actual_name = row.get_string(2).unwrap();

                assert_eq!(actual_region, region, "region mismatch");
                assert_eq!(actual_id, id, "id mismatch");
                assert_eq!(actual_name, format!("{}-{}", region, id), "name mismatch");

                (region.to_string(), id)
            });
        }

        let mut results = Vec::new();
        while let Some(result) = futures.next().await {
            results.push(result);
        }

        assert_eq!(
            results.len(),
            regions.len() * 5,
            "Not all lookups completed"
        );

        // Verify we got results from all partitions
        for region in &regions {
            let count = results.iter().filter(|(r, _)| r == region).count();
            assert_eq!(count, 5, "Expected 5 results for region {}", region);
        }

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    // Strings >7 chars for `b` force the encoder's variable-length area,
    // which is where prefix-key / primary-key byte layouts diverge.
    #[tokio::test]
    async fn prefix_lookup() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_prefix_lookup");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("a", DataTypes::int())
                    .column("b", DataTypes::string())
                    .column("c", DataTypes::bigint())
                    .column("d", DataTypes::string())
                    .primary_key(vec!["a", "b", "c"])
                    .build()
                    .expect("Failed to build schema"),
            )
            .distributed_by(Some(3), vec!["a".to_string(), "b".to_string()])
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        let test_data: &[(i32, &str, i64, &str)] = &[
            (1, "aaaaaaaaa", 1, "value1"),
            (1, "aaaaaaaaa", 2, "value2"),
            (1, "aaaaaaaaa", 3, "value3"),
            (2, "aaaaaaaaa", 4, "value4"),
        ];
        for (a, b, c, d) in test_data {
            let mut row = GenericRow::new(4);
            row.set_field(0, *a);
            row.set_field(1, *b);
            row.set_field(2, *c);
            row.set_field(3, *d);
            writer.upsert(&row).expect("Failed to upsert");
        }
        writer.flush().await.expect("Failed to flush");

        let mut prefix_lookuper = table
            .new_lookup()
            .expect("Failed to create lookup")
            .lookup_by(vec!["a".to_string(), "b".to_string()])
            .create_lookuper()
            .expect("Failed to create prefix lookuper");

        let mut prefix = GenericRow::new(2);
        prefix.set_field(0, 1);
        prefix.set_field(1, "aaaaaaaaa");
        let result = prefix_lookuper
            .lookup(&prefix)
            .await
            .expect("Failed to prefix lookup");
        let rows = result.get_rows().expect("Failed to decode rows");
        assert_eq!(rows.len(), 3, "Prefix (1, 'aaaaaaaaa') should match 3 rows");
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.get_int(0).unwrap(), 1);
            assert_eq!(row.get_string(1).unwrap(), "aaaaaaaaa");
            assert_eq!(row.get_long(2).unwrap(), (i as i64) + 1);
            assert_eq!(row.get_string(3).unwrap(), format!("value{}", i + 1));
        }

        let mut prefix = GenericRow::new(2);
        prefix.set_field(0, 2);
        prefix.set_field(1, "aaaaaaaaa");
        let result = prefix_lookuper
            .lookup(&prefix)
            .await
            .expect("Failed to prefix lookup");
        let rows = result.get_rows().expect("Failed to decode rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_int(0).unwrap(), 2);
        assert_eq!(rows[0].get_string(1).unwrap(), "aaaaaaaaa");
        assert_eq!(rows[0].get_long(2).unwrap(), 4);
        assert_eq!(rows[0].get_string(3).unwrap(), "value4");

        let mut prefix = GenericRow::new(2);
        prefix.set_field(0, 3);
        prefix.set_field(1, "a");
        let result = prefix_lookuper
            .lookup(&prefix)
            .await
            .expect("Failed to prefix lookup");
        let rows = result.get_rows().expect("Failed to decode rows");
        assert_eq!(rows.len(), 0);

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn prefix_lookup_partitioned() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_prefix_lookup_partitioned");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("region", DataTypes::string())
                    .column("a", DataTypes::int())
                    .column("b", DataTypes::string())
                    .column("c", DataTypes::bigint())
                    .column("d", DataTypes::string())
                    .primary_key(vec!["region", "a", "b", "c"])
                    .build()
                    .expect("Failed to build schema"),
            )
            .partitioned_by(vec!["region"])
            .distributed_by(Some(3), vec!["a".to_string(), "b".to_string()])
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;
        create_partitions(&admin, &table_path, "region", &["US", "EU"]).await;

        let connection = cluster.get_fluss_connection().await;
        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        let test_data: &[(&str, i32, &str, i64, &str)] = &[
            ("US", 1, "aaaaaaaaa", 1, "us-1"),
            ("US", 1, "aaaaaaaaa", 2, "us-2"),
            ("US", 2, "aaaaaaaaa", 3, "us-3"),
            ("EU", 1, "aaaaaaaaa", 4, "eu-1"),
            ("EU", 1, "bbbbbbbbb", 5, "eu-2"),
        ];
        for (region, a, b, c, d) in test_data {
            let mut row = GenericRow::new(5);
            row.set_field(0, *region);
            row.set_field(1, *a);
            row.set_field(2, *b);
            row.set_field(3, *c);
            row.set_field(4, *d);
            writer.upsert(&row).expect("Failed to upsert");
        }
        writer.flush().await.expect("Failed to flush");

        let mut prefix_lookuper = table
            .new_lookup()
            .expect("Failed to create lookup")
            .lookup_by(vec!["region".to_string(), "a".to_string(), "b".to_string()])
            .create_lookuper()
            .expect("Failed to create prefix lookuper");

        // Prefix (US, 1, "aaaaaaaaa") — 2 rows.
        let mut prefix = GenericRow::new(3);
        prefix.set_field(0, "US");
        prefix.set_field(1, 1);
        prefix.set_field(2, "aaaaaaaaa");
        let result = prefix_lookuper
            .lookup(&prefix)
            .await
            .expect("Failed to prefix lookup");
        let rows = result.get_rows().expect("Failed to decode rows");
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row.get_string(0).unwrap(), "US");
            assert_eq!(row.get_int(1).unwrap(), 1);
            assert_eq!(row.get_string(2).unwrap(), "aaaaaaaaa");
        }

        // Prefix (EU, 1, "bbbbbbbbb") — 1 row.
        let mut prefix = GenericRow::new(3);
        prefix.set_field(0, "EU");
        prefix.set_field(1, 1);
        prefix.set_field(2, "bbbbbbbbb");
        let result = prefix_lookuper
            .lookup(&prefix)
            .await
            .expect("Failed to prefix lookup");
        let rows = result.get_rows().expect("Failed to decode rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_string(4).unwrap(), "eu-2");

        let mut prefix = GenericRow::new(3);
        prefix.set_field(0, "APAC");
        prefix.set_field(1, 1);
        prefix.set_field(2, "aaaaaaaaa");
        let result = prefix_lookuper
            .lookup(&prefix)
            .await
            .expect("Failed to prefix lookup");
        let rows = result.get_rows().expect("Failed to decode rows");
        assert_eq!(rows.len(), 0);

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn prefix_lookup_validation_errors() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_prefix_lookup_validation");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("a", DataTypes::int())
                    .column("b", DataTypes::string())
                    .column("c", DataTypes::bigint())
                    .primary_key(vec!["a", "b", "c"])
                    .build()
                    .expect("Failed to build schema"),
            )
            .distributed_by(Some(3), vec!["a".to_string(), "b".to_string()])
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        let err = table
            .new_lookup()
            .expect("Failed to create lookup")
            .lookup_by(vec!["b".to_string(), "a".to_string()])
            .create_lookuper()
            .err()
            .expect("Expected validation error for wrong order");
        assert!(err.to_string().contains("must contain all bucket keys"));

        let err = table
            .new_lookup()
            .expect("Failed to create lookup")
            .lookup_by(vec!["a".to_string(), "b".to_string(), "c".to_string()])
            .create_lookuper()
            .err()
            .expect("Expected validation error for extra lookup columns");
        assert!(err.to_string().contains("must contain all bucket keys"));

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Integration test for concurrent batched lookups.
    #[tokio::test]
    async fn batched_concurrent_lookups() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss".to_string(), "test_batched_lookups".to_string());

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .column("value", DataTypes::bigint())
                    .primary_key(vec!["id".to_string()])
                    .build()
                    .expect("Failed to build schema"),
            )
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        let table_upsert = table.new_upsert().expect("Failed to create upsert");

        // Insert only even-numbered records (0, 2, 4, ..., 98) in parallel
        let num_records = 100i32;
        let mut upsert_futures = FuturesUnordered::new();
        for i in (0..num_records).step_by(2) {
            let writer = table_upsert
                .create_writer()
                .expect("Failed to create writer");
            upsert_futures.push(async move {
                let mut row = GenericRow::new(3);
                row.set_field(0, i);
                row.set_field(1, format!("name_{}", i));
                row.set_field(2, (i * 100) as i64);
                writer
                    .upsert(&row)
                    .expect("Failed to upsert")
                    .await
                    .expect("Failed to await upsert ack");
            });
        }
        // Wait for all upserts to be acknowledged
        while upsert_futures.next().await.is_some() {}

        // Create multiple lookupers for concurrent lookups
        let num_lookupers = 50i32;
        let mut lookupers: Vec<_> = (0..num_lookupers)
            .map(|_| {
                table
                    .new_lookup()
                    .expect("Failed to create lookup")
                    .create_lookuper()
                    .expect("Failed to create lookuper")
            })
            .collect();

        // Run concurrent lookups
        let mut futures = FuturesUnordered::new();
        for (i, lookuper) in lookupers.iter_mut().enumerate() {
            // First 10 lookupers all lookup id=0 (same key multiple times)
            let id = if i < 10 { 0 } else { i as i32 };
            let expects_result = id % 2 == 0; // Even IDs exist

            futures.push(async move {
                let key = make_key(id);
                let result = lookuper.lookup(&key).await.expect("Failed to lookup");
                let row_opt = result.get_single_row().expect("Failed to get row");

                if expects_result {
                    let row = row_opt.unwrap_or_else(|| panic!("Row {} should exist", id));
                    assert_eq!(row.get_int(0).unwrap(), id, "id mismatch for key {}", id);
                    assert_eq!(
                        row.get_string(1).unwrap(),
                        format!("name_{}", id),
                        "name mismatch for key {}",
                        id
                    );
                    assert_eq!(
                        row.get_long(2).unwrap(),
                        (id * 100) as i64,
                        "value mismatch for key {}",
                        id
                    );
                } else {
                    assert!(row_opt.is_none(), "Row {} should not exist", id);
                }
                (id, expects_result)
            });
        }

        // Collect all results and verify
        let mut results = Vec::with_capacity(num_lookupers as usize);
        while let Some(result) = futures.next().await {
            results.push(result);
        }

        // Verify all lookups completed successfully
        assert_eq!(
            results.len(),
            num_lookupers as usize,
            "Not all lookups completed"
        );

        // Verify we had the expected mix of scenarios
        let same_key_lookups = results.iter().filter(|(id, _)| *id == 0).count();
        assert_eq!(same_key_lookups, 10, "Should have 10 lookups for same key");

        let non_existing_lookups = results.iter().filter(|(_, exists)| !exists).count();
        assert!(
            non_existing_lookups > 0,
            "Should have some non-existing key lookups"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Test that KV format v2 tables with non-default bucket key reject v0 clients.
    /// The Rust client currently only supports API version 0 for PutKv/Lookup/PrefixLookup.
    /// When the server creates a table with kv_format_version=2 and a non-default bucket key,
    /// it rejects v0 clients because CompactedKeyEncoder (v1) is required.
    // TODO(key-encoding-v1): Once v1 key encoding is implemented and the client advertises
    //  PutKv/Lookup/PrefixLookup v1, this test should be updated to verify that v1 clients
    //  can successfully write to and read from kv_format_v2 tables with non-default bucket keys.
    #[tokio::test]
    async fn kv_format_v2_table_rejects_v0_client() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().unwrap();

        let table_path = TablePath::new("fluss", "test_kv_format_v2_reject_v0");

        // Create a KV table with:
        // 1. kv_format_version = 2
        // 2. non-default bucket key ("a" is a subset of pk ("a", "b"))
        // 3. datalake format is exist.
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("a", DataTypes::int())
                    .column("b", DataTypes::string())
                    .column("c", DataTypes::string())
                    .primary_key(vec!["a", "b"])
                    .build()
                    .expect("Failed to build schema"),
            )
            .distributed_by(Some(2), vec!["a".to_string()])
            .property("table.kv.format-version", "2")
            .property("table.datalake.format", "lance")
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection.get_table(&table_path).await.unwrap();

        // Test PutKv with v0 client - should fail with UNSUPPORTED_VERSION
        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let upsert_writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        let mut row = GenericRow::new(3);
        row.set_field(0, 1);
        row.set_field(1, "a");
        row.set_field(2, "value1");
        let upsert_result = upsert_writer
            .upsert(&row)
            .expect("Failed to upsert row")
            .await;
        assert!(
            upsert_result.is_err(),
            "PutKv with v0 client should be rejected for kv_format_v2 table with non-default bucket key"
        );
        let err_msg = upsert_result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Client API version 0 is not supported"),
            "Expected 'Client API version 0 is not supported' error, got: {}",
            err_msg
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// KV upsert + lookup against a schema covering every supported data type.
    #[tokio::test]
    async fn all_supported_datatypes() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_kv_complex_types");

        let row_seq_label_owned = dt_row_seq_label();
        let row_seq_label = as_row_type(&row_seq_label_owned);
        let inner_array_int = dt_array_int();
        let inner_map_string_int = dt_map_string_int();

        let plan = ColumnPlan::new()
            .add("id", DataTypes::int())
            .start_section("array_basics")
            .extend(array_dt_basics_columns())
            .start_section("row_basics")
            .extend(row_dt_basics_columns())
            .start_section("map_basics")
            .extend(map_dt_basics_columns())
            .start_section("scalars")
            .extend(scalar_dt_columns());
        let table_descriptor = TableDescriptor::builder()
            .schema(plan.build_schema(Some(&["id"])))
            .build()
            .expect("table descriptor");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let upsert_writer = table
            .new_upsert()
            .expect("upsert")
            .create_writer()
            .expect("writer");

        // Row 1 (id=1) — comprehensive: every column populated.
        let column_count = plan.len();
        let mut row1 = GenericRow::new(column_count);
        row1.set_field(0, 1_i32);
        row1.set_field(1, make_int_array(&[Some(10), Some(20), Some(30)]));
        row1.set_field(2, make_string_array(&[Some("hello"), Some("world")]));
        let arr_of_arr_1 = {
            let mut w = FlussArrayWriter::new(2, &inner_array_int);
            w.write_array(0, &make_int_array(&[Some(1), Some(2)]));
            w.write_array(1, &make_int_array(&[Some(3), Some(4)]));
            w.complete().expect("arr_of_arr_1")
        };
        row1.set_field(3, arr_of_arr_1);
        let arr_of_row_1 = {
            let mut w = FlussArrayWriter::new(2, &row_seq_label_owned);
            let mut e0 = GenericRow::new(2);
            e0.set_field(0, 1_i32);
            e0.set_field(1, "open");
            w.write_row(0, &e0).expect("e0");
            let mut e1 = GenericRow::new(2);
            e1.set_field(0, 2_i32);
            e1.set_field(1, "close");
            w.write_row(1, &e1).expect("e1");
            w.complete().expect("arr_of_row_1")
        };
        row1.set_field(4, arr_of_row_1);
        let mut row_basic_1 = GenericRow::new(2);
        row_basic_1.set_field(0, 42_i32);
        row_basic_1.set_field(1, "hello");
        row1.set_field(5, Datum::Row(Box::new(row_basic_1)));
        let mut deep_inner_1 = GenericRow::new(1);
        deep_inner_1.set_field(0, 99_i32);
        let mut row_deep_1 = GenericRow::new(1);
        row_deep_1.set_field(0, Datum::Row(Box::new(deep_inner_1)));
        row1.set_field(6, Datum::Row(Box::new(row_deep_1)));
        let mut row_rich_1 = GenericRow::new(14);
        row_rich_1.set_field(0, true);
        row_rich_1.set_field(1, 100_000_i32);
        row_rich_1.set_field(2, 9_876_543_210_i64);
        row_rich_1.set_field(3, f32::INFINITY);
        row_rich_1.set_field(4, std::f64::consts::PI);
        row_rich_1.set_field(5, "hello world");
        row_rich_1.set_field(6, b"binary".as_slice());
        row_rich_1.set_field(7, Decimal::from_unscaled_long(12345, 10, 2).unwrap());
        row_rich_1.set_field(8, Datum::Date(Date::new(20476)));
        row_rich_1.set_field(9, Datum::Time(Time::new(36_827_123)));
        row_rich_1.set_field(
            10,
            Datum::TimestampNtz(TimestampNtz::new(1_769_163_227_123)),
        );
        row_rich_1.set_field(
            11,
            Datum::TimestampLtz(TimestampLtz::new(1_769_163_227_456)),
        );
        row_rich_1.set_field(12, b"\x01\x02\x03\x04".as_slice());
        row_rich_1.set_field(13, make_int_array(&[Some(7), None, Some(11)]));
        row1.set_field(7, Datum::Row(Box::new(row_rich_1)));
        let map_string_int_1 = {
            let mut w = FlussMapWriter::new(3, &DataTypes::string(), &DataTypes::int());
            w.write_entry("a".into(), 1.into()).unwrap();
            w.write_entry("b".into(), Datum::Null).unwrap();
            w.write_entry("c".into(), 3.into()).unwrap();
            w.complete().expect("map_string_int_1")
        };
        row1.set_field(8, Datum::Map(map_string_int_1));
        let map_of_row_1 = {
            let mut e0 = GenericRow::new(2);
            e0.set_field(0, 1_i32);
            e0.set_field(1, "open");
            let mut e1 = GenericRow::new(2);
            e1.set_field(0, 2_i32);
            e1.set_field(1, "close");
            let mut w = FlussMapWriter::new(2, &DataTypes::string(), &row_seq_label_owned);
            w.write_entry("e0".into(), Datum::Row(Box::new(e0)))
                .unwrap();
            w.write_entry("e1".into(), Datum::Row(Box::new(e1)))
                .unwrap();
            w.complete().expect("map_of_row_1")
        };
        row1.set_field(9, Datum::Map(map_of_row_1));
        let map_of_map_1 = {
            let g1 = {
                let mut w = FlussMapWriter::new(2, &DataTypes::string(), &DataTypes::int());
                w.write_entry("a".into(), 1.into()).unwrap();
                w.write_entry("b".into(), 2.into()).unwrap();
                w.complete().expect("g1")
            };
            let g2 = {
                let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::int());
                w.write_entry("c".into(), 3.into()).unwrap();
                w.complete().expect("g2")
            };
            let mut w = FlussMapWriter::new(2, &DataTypes::string(), &inner_map_string_int);
            w.write_entry("g1".into(), Datum::Map(g1)).unwrap();
            w.write_entry("g2".into(), Datum::Map(g2)).unwrap();
            w.complete().expect("map_of_map_1")
        };
        row1.set_field(10, Datum::Map(map_of_map_1));
        let map_of_array_1 = {
            let primes = make_int_array(&[Some(2), Some(3), Some(5)]);
            let squares = make_int_array(&[Some(1), Some(4)]);
            let mut w = FlussMapWriter::new(2, &DataTypes::string(), &inner_array_int);
            w.write_entry("primes".into(), Datum::Array(primes))
                .unwrap();
            w.write_entry("squares".into(), Datum::Array(squares))
                .unwrap();
            w.complete().expect("map_of_array_1")
        };
        row1.set_field(11, Datum::Map(map_of_array_1));
        let array_of_map_1 = {
            let m0 = {
                let mut w = FlussMapWriter::new(2, &DataTypes::string(), &DataTypes::int());
                w.write_entry("x".into(), 1.into()).unwrap();
                w.write_entry("y".into(), 2.into()).unwrap();
                w.complete().expect("m0")
            };
            let m1 = {
                let mut w = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::int());
                w.write_entry("z".into(), 9.into()).unwrap();
                w.complete().expect("m1")
            };
            let mut w = FlussArrayWriter::new(2, &inner_map_string_int);
            w.write_map(0, &m0);
            w.write_map(1, &m1);
            w.complete().expect("array_of_map_1")
        };
        row1.set_field(12, array_of_map_1);

        // Scalar values for row 1.
        let s_tinyint = 127_i8;
        let s_smallint = 32_767_i16;
        let s_bigint = 9_223_372_036_854_775_807_i64;
        let s_float = std::f32::consts::PI;
        let s_double = std::f64::consts::E;
        let s_char = "hello";
        let s_string = "world of fluss rust client";
        let s_decimal = Decimal::from_unscaled_long(12345, 10, 2).unwrap();
        let s_date = Date::new(20476);
        let s_time_s = Time::new(36_827_000);
        let s_time_ms = Time::new(36_827_123);
        let s_time_us = Time::new(86_399_999);
        let s_time_ns = Time::new(1);
        let s_ts_s = TimestampNtz::new(1_769_163_227_000);
        let s_ts_ms = TimestampNtz::new(1_769_163_227_123);
        let s_ts_us = TimestampNtz::from_millis_nanos(1_769_163_227_123, 456_000).unwrap();
        let s_ts_ns = TimestampNtz::from_millis_nanos(1_769_163_227_123, 999_999).unwrap();
        let s_ts_ltz_s = TimestampLtz::new(1_769_163_227_000);
        let s_ts_ltz_ms = TimestampLtz::new(1_769_163_227_123);
        let s_ts_ltz_us = TimestampLtz::from_millis_nanos(1_769_163_227_123, 456_000).unwrap();
        let s_ts_ltz_ns = TimestampLtz::from_millis_nanos(1_769_163_227_123, 999_999).unwrap();
        let s_bytes_top: Vec<u8> = b"binary data".to_vec();
        let s_binary_top: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let s_ts_us_neg = TimestampNtz::from_millis_nanos(-301_234_154_877, 456_000).unwrap();
        let s_ts_ns_neg = TimestampNtz::from_millis_nanos(-301_234_154_877, 999_999).unwrap();
        let s_ts_ltz_us_neg = TimestampLtz::from_millis_nanos(-301_234_154_877, 456_000).unwrap();
        let s_ts_ltz_ns_neg = TimestampLtz::from_millis_nanos(-301_234_154_877, 999_999).unwrap();

        row1.set_field(plan.idx("col_tinyint"), s_tinyint);
        row1.set_field(plan.idx("col_smallint"), s_smallint);
        row1.set_field(plan.idx("col_bigint"), s_bigint);
        row1.set_field(plan.idx("col_float"), s_float);
        row1.set_field(plan.idx("col_double"), s_double);
        row1.set_field(plan.idx("col_boolean"), true);
        row1.set_field(plan.idx("col_char"), s_char);
        row1.set_field(plan.idx("col_string"), s_string);
        row1.set_field(plan.idx("col_decimal"), s_decimal.clone());
        row1.set_field(plan.idx("col_date"), Datum::Date(s_date));
        row1.set_field(plan.idx("col_time_s"), s_time_s);
        row1.set_field(plan.idx("col_time_ms"), s_time_ms);
        row1.set_field(plan.idx("col_time_us"), s_time_us);
        row1.set_field(plan.idx("col_time_ns"), s_time_ns);
        row1.set_field(plan.idx("col_ts_s"), s_ts_s);
        row1.set_field(plan.idx("col_ts_ms"), s_ts_ms);
        row1.set_field(plan.idx("col_ts_us"), s_ts_us);
        row1.set_field(plan.idx("col_ts_ns"), s_ts_ns);
        row1.set_field(plan.idx("col_ts_ltz_s"), s_ts_ltz_s);
        row1.set_field(plan.idx("col_ts_ltz_ms"), s_ts_ltz_ms);
        row1.set_field(plan.idx("col_ts_ltz_us"), s_ts_ltz_us);
        row1.set_field(plan.idx("col_ts_ltz_ns"), s_ts_ltz_ns);
        row1.set_field(plan.idx("col_bytes_top"), s_bytes_top.as_slice());
        row1.set_field(plan.idx("col_binary_top"), s_binary_top.as_slice());
        row1.set_field(plan.idx("col_ts_us_neg"), s_ts_us_neg);
        row1.set_field(plan.idx("col_ts_ns_neg"), s_ts_ns_neg);
        row1.set_field(plan.idx("col_ts_ltz_us_neg"), s_ts_ltz_us_neg);
        row1.set_field(plan.idx("col_ts_ltz_ns_neg"), s_ts_ltz_ns_neg);

        upsert_writer
            .upsert(&row1)
            .expect("upsert row1")
            .await
            .expect("ack row1");

        // Row 2 (id=2) — empty MAP, all other compound + scalar columns NULL.
        let mut row2 = GenericRow::new(column_count);
        row2.set_field(0, 2_i32);
        for i in 1..column_count {
            row2.set_field(i, Datum::Null);
        }
        let empty_map = FlussMapWriter::new(0, &DataTypes::string(), &DataTypes::int())
            .complete()
            .expect("empty_map");
        row2.set_field(plan.idx("map_string_int"), Datum::Map(empty_map));
        upsert_writer
            .upsert(&row2)
            .expect("upsert row2")
            .await
            .expect("ack row2");

        // Row 3 (id=3) — every compound + scalar column NULL.
        let mut row3 = GenericRow::new(column_count);
        row3.set_field(0, 3_i32);
        for i in 1..column_count {
            row3.set_field(i, Datum::Null);
        }
        upsert_writer
            .upsert(&row3)
            .expect("upsert row3")
            .await
            .expect("ack row3");

        let mut lookuper = table
            .new_lookup()
            .expect("lookup")
            .create_lookuper()
            .expect("lookuper");

        let result1 = lookuper.lookup(&make_key(1)).await.expect("lookup row1");
        let r1 = result1
            .get_single_row()
            .expect("row1")
            .expect("row1 exists");
        assert_eq!(r1.get_int(0).unwrap(), 1);

        // === ARRAY: basic shapes ===
        let arr_int = r1.get_array(1).unwrap();
        assert_eq!(arr_int.size(), 3);
        assert_eq!(arr_int.get_int(2).unwrap(), 30);
        let arr_string = r1.get_array(2).unwrap();
        assert_eq!(arr_string.size(), 2);
        assert_eq!(arr_string.get_string(0).unwrap(), "hello");
        let arr_of_arr = r1.get_array(3).unwrap();
        assert_eq!(arr_of_arr.size(), 2);
        assert_eq!(arr_of_arr.get_array(1).unwrap().get_int(1).unwrap(), 4);

        // === ARRAY<ROW> ===
        let aor = r1.get_array(4).unwrap();
        assert_eq!(aor.size(), 2);
        let e0 = aor.get_row(0, &row_seq_label).unwrap();
        assert_eq!(e0.get_int(0).unwrap(), 1);
        assert_eq!(e0.get_string(1).unwrap(), "open");

        // === ROW: basic + deep + rich ===
        let rb = r1.get_row(5).unwrap();
        assert_eq!(rb.get_int(0).unwrap(), 42);
        assert_eq!(rb.get_string(1).unwrap(), "hello");
        let rd = r1.get_row(6).unwrap();
        let rd_inner = rd.get_row(0).unwrap();
        assert_eq!(rd_inner.get_int(0).unwrap(), 99);
        let rr = r1.get_row(7).unwrap();
        assert!(rr.get_boolean(0).unwrap());
        assert_eq!(rr.get_int(1).unwrap(), 100_000);
        assert_eq!(rr.get_long(2).unwrap(), 9_876_543_210);
        assert!(rr.get_float(3).unwrap().is_infinite());
        assert!((rr.get_double(4).unwrap() - std::f64::consts::PI).abs() < f64::EPSILON);
        assert_eq!(rr.get_string(5).unwrap(), "hello world");
        assert_eq!(rr.get_bytes(6).unwrap(), b"binary");
        assert_eq!(
            rr.get_decimal(7, 10, 2).unwrap(),
            Decimal::from_unscaled_long(12345, 10, 2).unwrap()
        );
        assert_eq!(rr.get_date(8).unwrap().get_inner(), 20476);
        assert_eq!(rr.get_time(9).unwrap().get_inner(), 36_827_123);
        assert_eq!(
            rr.get_timestamp_ntz(10, 6).unwrap().get_millisecond(),
            1_769_163_227_123
        );
        assert_eq!(
            rr.get_timestamp_ltz(11, 6).unwrap().get_epoch_millisecond(),
            1_769_163_227_456
        );
        assert_eq!(rr.get_binary(12, 4).unwrap(), b"\x01\x02\x03\x04");
        let f_arr = rr.get_array(13).unwrap();
        assert_eq!(f_arr.size(), 3);
        assert!(f_arr.is_null_at(1));

        // === MAP: basic ===
        let m = r1.get_map(8).unwrap();
        assert_eq!(m.size(), 3);
        assert_eq!(m.get(&Datum::from("a")).unwrap(), Some(Datum::from(1_i32)));
        assert_eq!(m.get(&Datum::from("b")).unwrap(), Some(Datum::Null));
        assert_eq!(m.get(&Datum::from("c")).unwrap(), Some(Datum::from(3_i32)));

        // === MAP<K, ROW> ===
        let m = r1.get_map(9).unwrap();
        let v0 = m.value_array().get_row(0, &row_seq_label).unwrap();
        assert_eq!(v0.get_int(0).unwrap(), 1);
        assert_eq!(v0.get_string(1).unwrap(), "open");

        // === MAP<K, MAP> ===
        let m = r1.get_map(10).unwrap();
        let g1 = m
            .value_array()
            .get_map(0, &DataTypes::string(), &DataTypes::int())
            .unwrap();
        assert_eq!(g1.size(), 2);

        // === MAP<K, ARRAY> + ARRAY<MAP> ===
        let m = r1.get_map(11).unwrap();
        assert_eq!(m.value_array().get_array(0).unwrap().size(), 3);
        let am = r1.get_array(12).unwrap();
        assert_eq!(am.size(), 2);
        let am0 = am
            .get_map(0, &DataTypes::string(), &DataTypes::int())
            .unwrap();
        assert_eq!(am0.size(), 2);

        // === Scalars: integers + floating point ===
        assert_eq!(r1.get_byte(plan.idx("col_tinyint")).unwrap(), s_tinyint);
        assert_eq!(r1.get_short(plan.idx("col_smallint")).unwrap(), s_smallint);
        assert_eq!(r1.get_long(plan.idx("col_bigint")).unwrap(), s_bigint);
        assert!((r1.get_float(plan.idx("col_float")).unwrap() - s_float).abs() < f32::EPSILON);
        assert!((r1.get_double(plan.idx("col_double")).unwrap() - s_double).abs() < f64::EPSILON);

        // === Scalars: boolean / char / string / decimal / date ===
        assert!(r1.get_boolean(plan.idx("col_boolean")).unwrap());
        assert_eq!(r1.get_char(plan.idx("col_char"), 10).unwrap(), s_char);
        assert_eq!(r1.get_string(plan.idx("col_string")).unwrap(), s_string);
        assert_eq!(
            r1.get_decimal(plan.idx("col_decimal"), 10, 2).unwrap(),
            s_decimal
        );
        assert_eq!(
            r1.get_date(plan.idx("col_date")).unwrap().get_inner(),
            s_date.get_inner()
        );

        // === Scalars: time across all four precisions ===
        assert_eq!(
            r1.get_time(plan.idx("col_time_s")).unwrap().get_inner(),
            s_time_s.get_inner()
        );
        assert_eq!(
            r1.get_time(plan.idx("col_time_ms")).unwrap().get_inner(),
            s_time_ms.get_inner()
        );
        assert_eq!(
            r1.get_time(plan.idx("col_time_us")).unwrap().get_inner(),
            s_time_us.get_inner()
        );
        assert_eq!(
            r1.get_time(plan.idx("col_time_ns")).unwrap().get_inner(),
            s_time_ns.get_inner()
        );

        // === Scalars: timestamp across all four precisions ===
        assert_eq!(
            r1.get_timestamp_ntz(plan.idx("col_ts_s"), 0)
                .unwrap()
                .get_millisecond(),
            s_ts_s.get_millisecond()
        );
        assert_eq!(
            r1.get_timestamp_ntz(plan.idx("col_ts_ms"), 3)
                .unwrap()
                .get_millisecond(),
            s_ts_ms.get_millisecond()
        );
        let read_ts_us = r1.get_timestamp_ntz(plan.idx("col_ts_us"), 6).unwrap();
        assert_eq!(read_ts_us.get_millisecond(), s_ts_us.get_millisecond());
        assert_eq!(
            read_ts_us.get_nano_of_millisecond(),
            s_ts_us.get_nano_of_millisecond()
        );
        let read_ts_ns = r1.get_timestamp_ntz(plan.idx("col_ts_ns"), 9).unwrap();
        assert_eq!(read_ts_ns.get_millisecond(), s_ts_ns.get_millisecond());
        assert_eq!(
            read_ts_ns.get_nano_of_millisecond(),
            s_ts_ns.get_nano_of_millisecond()
        );

        // === Scalars: timestamp_ltz across all four precisions ===
        assert_eq!(
            r1.get_timestamp_ltz(plan.idx("col_ts_ltz_s"), 0)
                .unwrap()
                .get_epoch_millisecond(),
            s_ts_ltz_s.get_epoch_millisecond()
        );
        assert_eq!(
            r1.get_timestamp_ltz(plan.idx("col_ts_ltz_ms"), 3)
                .unwrap()
                .get_epoch_millisecond(),
            s_ts_ltz_ms.get_epoch_millisecond()
        );
        let read_ltz_us = r1.get_timestamp_ltz(plan.idx("col_ts_ltz_us"), 6).unwrap();
        assert_eq!(
            read_ltz_us.get_epoch_millisecond(),
            s_ts_ltz_us.get_epoch_millisecond()
        );
        assert_eq!(
            read_ltz_us.get_nano_of_millisecond(),
            s_ts_ltz_us.get_nano_of_millisecond()
        );
        let read_ltz_ns = r1.get_timestamp_ltz(plan.idx("col_ts_ltz_ns"), 9).unwrap();
        assert_eq!(
            read_ltz_ns.get_epoch_millisecond(),
            s_ts_ltz_ns.get_epoch_millisecond()
        );
        assert_eq!(
            read_ltz_ns.get_nano_of_millisecond(),
            s_ts_ltz_ns.get_nano_of_millisecond()
        );

        // === Scalars: bytes + fixed binary ===
        assert_eq!(
            r1.get_bytes(plan.idx("col_bytes_top")).unwrap(),
            s_bytes_top.as_slice()
        );
        assert_eq!(
            r1.get_binary(plan.idx("col_binary_top"), 4).unwrap(),
            s_binary_top.as_slice()
        );

        // === Scalars: negative-epoch timestamps (pre-1970) ===
        let read_neg_us = r1.get_timestamp_ntz(plan.idx("col_ts_us_neg"), 6).unwrap();
        assert_eq!(read_neg_us.get_millisecond(), s_ts_us_neg.get_millisecond());
        assert_eq!(
            read_neg_us.get_nano_of_millisecond(),
            s_ts_us_neg.get_nano_of_millisecond()
        );
        let read_neg_ns = r1.get_timestamp_ntz(plan.idx("col_ts_ns_neg"), 9).unwrap();
        assert_eq!(read_neg_ns.get_millisecond(), s_ts_ns_neg.get_millisecond());
        assert_eq!(
            read_neg_ns.get_nano_of_millisecond(),
            s_ts_ns_neg.get_nano_of_millisecond()
        );
        let read_neg_ltz_us = r1
            .get_timestamp_ltz(plan.idx("col_ts_ltz_us_neg"), 6)
            .unwrap();
        assert_eq!(
            read_neg_ltz_us.get_epoch_millisecond(),
            s_ts_ltz_us_neg.get_epoch_millisecond()
        );
        let read_neg_ltz_ns = r1
            .get_timestamp_ltz(plan.idx("col_ts_ltz_ns_neg"), 9)
            .unwrap();
        assert_eq!(
            read_neg_ltz_ns.get_epoch_millisecond(),
            s_ts_ltz_ns_neg.get_epoch_millisecond()
        );

        // === Row 2 lookup — empty map, all other columns NULL ===
        let result2 = lookuper.lookup(&make_key(2)).await.expect("lookup row2");
        let r2 = result2
            .get_single_row()
            .expect("row2")
            .expect("row2 exists");
        assert_eq!(r2.get_int(0).unwrap(), 2);
        let map_idx = plan.idx("map_string_int");
        for i in 1..column_count {
            if i == map_idx {
                assert_eq!(r2.get_map(map_idx).unwrap().size(), 0);
            } else {
                assert!(r2.is_null_at(i).unwrap(), "field {i} should be null");
            }
        }

        // === Row 3 lookup — every compound + scalar field NULL ===
        let result3 = lookuper.lookup(&make_key(3)).await.expect("lookup row3");
        let r3 = result3
            .get_single_row()
            .expect("row3")
            .expect("row3 exists");
        assert_eq!(r3.get_int(0).unwrap(), 3);
        for i in 1..column_count {
            assert!(r3.is_null_at(i).unwrap(), "field {i} should be null");
        }

        admin.drop_table(&table_path, false).await.expect("drop");
    }
}
