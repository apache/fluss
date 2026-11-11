// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::{LakePlannerContext, LakeReadSemantics, LakeReaderContext, LakeSource};
use fluss::metadata::{DataTypes, Schema};
use futures::TryStreamExt;
use paimon::catalog::Catalog;
use paimon::spec::{DataType as PaimonType, IntType, SchemaChange};
use std::sync::Arc;

struct Warehouse(std::path::PathBuf);

impl Drop for Warehouse {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl Warehouse {
    async fn new() -> (Self, Arc<dyn Catalog>) {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "fluss-lake-reader-{}-{nonce}-{id}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        let mut options = Options::default();
        options.set("warehouse", path.to_str().unwrap());
        let catalog = CatalogFactory::create(options).await.unwrap();
        catalog
            .create_database("physical", false, HashMap::new())
            .await
            .unwrap();
        catalog
            .create_database("logical", false, HashMap::new())
            .await
            .unwrap();
        (Self(path), catalog)
    }

    fn source(&self, info: &TableInfo) -> PaimonLakeSource {
        PaimonLakeSource::new(
            info,
            &HashMap::from([("warehouse".into(), self.0.to_str().unwrap().into())]),
        )
        .unwrap()
    }
}

async fn write(
    catalog: &Arc<dyn Catalog>,
    identifier: &Identifier,
    schema: paimon::spec::Schema,
    columns: Vec<Vec<i32>>,
) -> i64 {
    catalog
        .create_table(identifier, schema, false)
        .await
        .unwrap();
    let table = catalog.get_table(identifier).await.unwrap();
    let schema = paimon::arrow::build_target_arrow_schema(table.schema().fields()).unwrap();
    let batch = arrow_array_58::RecordBatch::try_new(
        schema,
        columns
            .into_iter()
            .map(|values| {
                Arc::new(arrow_array_58::Int32Array::from(values)) as arrow_array_58::ArrayRef
            })
            .collect(),
    )
    .unwrap();
    commit_batch(&table, batch).await
}

async fn commit_batch(table: &Table, batch: arrow_array_58::RecordBatch) -> i64 {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let commits = writer.prepare_commit().await.unwrap();
    assert!(
        commits
            .iter()
            .flat_map(|commit| &commit.new_files)
            .all(|file| file.file_name.ends_with(".parquet"))
    );
    builder.new_commit().commit(commits).await.unwrap();
    table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap()
}

fn info(name: &str, pk: bool, properties: HashMap<String, String>) -> TableInfo {
    let mut schema = Schema::builder().column("id", DataTypes::int());
    if pk {
        schema = schema
            .column("value", DataTypes::int())
            .primary_key(["id"])
            .unwrap();
    }
    TableInfo::new(
        TablePath::new("logical", name),
        7,
        1,
        schema.build().unwrap(),
        if pk { vec!["id".into()] } else { vec![] },
        vec![].into(),
        2,
        properties,
        HashMap::new(),
        None,
        0,
        0,
    )
}

async fn read(
    source: &PaimonLakeSource,
    info: &TableInfo,
    snapshot_id: i64,
    reconcile_primary_key: bool,
) -> Result<Vec<RecordBatch>> {
    let semantics = if info.has_primary_key() {
        LakeReadSemantics::PrimaryKey
    } else {
        LakeReadSemantics::Append
    };
    let tasks = source
        .plan(LakePlannerContext {
            table_info: info,
            snapshot_id,
            semantics,
            reconcile_primary_key,
            filter: &BoundPredicate::AlwaysTrue,
        })
        .await?;
    let schema = fluss::record::to_arrow_schema(info.row_type()).unwrap();
    let projection: Vec<_> = (0..schema.fields().len()).collect();
    let mut batches = Vec::new();
    for task in tasks {
        batches.extend(
            source
                .read(LakeReaderContext {
                    table_info: info,
                    snapshot_id,
                    semantics,
                    reconcile_primary_key,
                    splits: &[task],
                    projection: &projection,
                    schema: schema.clone(),
                    filter: &BoundPredicate::AlwaysTrue,
                })
                .await?
                .try_collect::<Vec<_>>()
                .await?,
        );
    }
    Ok(batches)
}

fn ids(batches: &[RecordBatch]) -> Vec<i32> {
    let mut values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect();
    values.sort_unstable();
    values
}

#[tokio::test]
async fn mapped_physical_table_wins_over_an_unrelated_same_named_table() {
    let (warehouse, catalog) = Warehouse::new().await;
    let schema = || {
        paimon::spec::Schema::builder()
            .column("id", PaimonType::Int(IntType::new()))
            .option("bucket", "-1")
            .option("file.format", "parquet")
            .build()
            .unwrap()
    };
    let snapshot = write(
        &catalog,
        &Identifier::new("physical", "baseline"),
        schema(),
        vec![vec![11, 12]],
    )
    .await;
    write(
        &catalog,
        &Identifier::new("logical", "orders"),
        schema(),
        vec![vec![21]],
    )
    .await;
    let info = info(
        "orders",
        false,
        HashMap::from([
            ("table.datalake.database-name".into(), "physical".into()),
            ("table.datalake.table-name".into(), "baseline".into()),
        ]),
    );
    let source = warehouse.source(&info);
    assert_eq!(
        ids(&read(&source, &info, snapshot, false).await.unwrap()),
        vec![11, 12]
    );

    for polling_failure in [false, true] {
        let failing = source
            .clone()
            .with_baseline_reader(Arc::new(FailingBaseline { polling_failure }));
        assert!(matches!(
            read(&failing, &info, snapshot, false).await,
            Err(FlussLakeError::DataUnavailable(_))
        ));
    }

    catalog
        .alter_table(
            &Identifier::new("physical", "baseline"),
            vec![SchemaChange::rename_column("id".into(), "renamed".into())],
            false,
        )
        .await
        .unwrap();
    // A later schema must not change how the frozen baseline is projected.
    assert_eq!(
        ids(&read(&source, &info, snapshot, false).await.unwrap()),
        vec![11, 12]
    );
    assert!(matches!(
        read(&source, &info, snapshot + 1, false).await,
        Err(FlussLakeError::DataUnavailable(_))
    ));
}

#[tokio::test]
async fn append_baselines_do_not_require_fluss_bucket_alignment() {
    let (warehouse, catalog) = Warehouse::new().await;
    for bucket_count in [-1_i32, 4] {
        let name = format!("append_{}", bucket_count.abs());
        let mut schema = paimon::spec::Schema::builder()
            .column("id", PaimonType::Int(IntType::new()))
            .option("bucket", bucket_count.to_string())
            .option("file.format", "parquet");
        if bucket_count > 0 {
            schema = schema.option("bucket-key", "id");
        }
        let snapshot = write(
            &catalog,
            &Identifier::new("logical", &name),
            schema.build().unwrap(),
            vec![vec![1, 2, 3]],
        )
        .await;
        let info = info(&name, false, HashMap::new());
        let source = warehouse.source(&info);
        let tasks = source
            .plan(LakePlannerContext {
                table_info: &info,
                snapshot_id: snapshot,
                semantics: LakeReadSemantics::Append,
                reconcile_primary_key: false,
                filter: &BoundPredicate::AlwaysTrue,
            })
            .await
            .unwrap();
        assert!(tasks.iter().all(|task| task.bucket_count == bucket_count));
        assert_eq!(
            ids(&read(&source, &info, snapshot, false).await.unwrap()),
            vec![1, 2, 3]
        );
    }
}

#[tokio::test]
async fn lake_only_aggregation_reads_current_view_but_union_remains_rejected() {
    let (warehouse, catalog) = Warehouse::new().await;
    let schema = paimon::spec::Schema::builder()
        .column("id", PaimonType::Int(IntType::new()))
        .column("value", PaimonType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "2")
        .option("bucket-key", "id")
        .option("file.format", "parquet")
        .option("merge-engine", "aggregation")
        .option("fields.value.aggregate-function", "sum")
        .build()
        .unwrap();
    let snapshot = write(
        &catalog,
        &Identifier::new("logical", "aggregation"),
        schema,
        vec![vec![1, 1], vec![2, 3]],
    )
    .await;
    let info = info("aggregation", true, HashMap::new());
    let source = warehouse.source(&info);
    let batches = read(&source, &info, snapshot, false).await.unwrap();
    assert_eq!(ids(&batches), vec![1]);
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap()
            .value(0),
        5
    );
    assert!(matches!(
        read(&source, &info, snapshot, true).await,
        Err(FlussLakeError::UnsupportedMergeEngine(_))
    ));
    let tasks = source
        .plan(LakePlannerContext {
            table_info: &info,
            snapshot_id: snapshot,
            semantics: LakeReadSemantics::PrimaryKey,
            reconcile_primary_key: false,
            filter: &BoundPredicate::AlwaysTrue,
        })
        .await
        .unwrap();
    assert!(matches!(
        source
            .read(LakeReaderContext {
                table_info: &info,
                snapshot_id: snapshot,
                semantics: LakeReadSemantics::PrimaryKey,
                reconcile_primary_key: true,
                splits: &tasks,
                projection: &[0, 1],
                schema: fluss::record::to_arrow_schema(info.row_type()).unwrap(),
                filter: &BoundPredicate::AlwaysTrue,
            })
            .await,
        Err(FlussLakeError::UnsupportedMergeEngine(_))
    ));
}

#[tokio::test]
async fn bucket_unaware_append_preserves_partition_identity() {
    let (warehouse, catalog) = Warehouse::new().await;
    let schema = paimon::spec::Schema::builder()
        .column("id", PaimonType::Int(IntType::new()))
        .column("region", PaimonType::Int(IntType::new()))
        .partition_keys(["region"])
        .option("bucket", "-1")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    let snapshot = write(
        &catalog,
        &Identifier::new("logical", "partitioned"),
        schema,
        vec![vec![1, 2, 3], vec![7, 8, 7]],
    )
    .await;
    let info = TableInfo::new(
        TablePath::new("logical", "partitioned"),
        7,
        1,
        Schema::builder()
            .column("id", DataTypes::int())
            .column("region", DataTypes::int())
            .build()
            .unwrap(),
        vec![],
        vec!["region".into()].into(),
        2,
        HashMap::new(),
        HashMap::new(),
        None,
        0,
        0,
    );
    let source = warehouse.source(&info);
    let tasks = source
        .plan(LakePlannerContext {
            table_info: &info,
            snapshot_id: snapshot,
            semantics: LakeReadSemantics::Append,
            reconcile_primary_key: false,
            filter: &BoundPredicate::AlwaysTrue,
        })
        .await
        .unwrap();
    assert!(
        tasks
            .iter()
            .all(|task| task.bucket_id == -1 && task.bucket_count == -1)
    );
    let partitions: std::collections::HashSet<_> =
        tasks.iter().map(|task| task.partition.clone()).collect();
    assert_eq!(
        partitions,
        ["7", "8"]
            .map(|value| crate::FlussLakePartitionIdentity::KeyValues(vec![(
                "region".into(),
                value.into()
            )]))
            .into_iter()
            .collect()
    );
    assert_eq!(
        ids(&read(&source, &info, snapshot, false).await.unwrap()),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn parquet_reader_adapts_logical_types_for_append_and_primary_key_baselines() {
    use arrow::array::{Array, AsArray};
    use arrow_array_58::{Array as _, Int32Array as Int32Array58};
    use paimon::spec::{ArrayType, BinaryType, TimestampType};

    let (warehouse, catalog) = Warehouse::new().await;
    for primary_key in [false, true] {
        let name = if primary_key { "pk" } else { "append" };
        let identifier = Identifier::new("logical", name);
        let mut lake_schema = paimon::spec::Schema::builder()
            .column("id", PaimonType::Int(IntType::new()))
            .column(
                "items",
                PaimonType::Array(ArrayType::new(PaimonType::Int(IntType::new()))),
            )
            .column("bytes", PaimonType::Binary(BinaryType::new(4).unwrap()))
            .column("ts", PaimonType::Timestamp(TimestampType::new(0).unwrap()))
            .option("bucket", "1")
            .option("bucket-key", "id")
            .option("file.format", "parquet");
        let mut fluss_schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("items", DataTypes::array(DataTypes::int()))
            .column("bytes", DataTypes::binary(4))
            .column("ts", DataTypes::timestamp_with_precision(0));
        if primary_key {
            lake_schema = lake_schema.primary_key(["id"]);
            fluss_schema = fluss_schema.primary_key(vec!["id"]).unwrap();
        }
        catalog
            .create_table(&identifier, lake_schema.build().unwrap(), false)
            .await
            .unwrap();
        let lake_table = catalog.get_table(&identifier).await.unwrap();
        let schema58 =
            paimon::arrow::build_target_arrow_schema(lake_table.schema().fields()).unwrap();
        let list = arrow_array_58::ListArray::from_iter_primitive::<
            arrow_array_58::types::Int32Type,
            _,
            _,
        >(vec![Some(vec![Some(1), None]), None, Some(vec![])]);
        // Use Paimon's actual list child field, rather than Arrow's default.
        let list = arrow_array_58::make_array(
            list.to_data()
                .into_builder()
                .data_type(schema58.field(1).data_type().clone())
                .build()
                .unwrap(),
        );
        let input = arrow_array_58::RecordBatch::try_new(
            schema58,
            vec![
                Arc::new(Int32Array58::from(vec![1, 2, 3])),
                list,
                Arc::new(arrow_array_58::BinaryArray::from(vec![
                    Some(b"abcd".as_slice()),
                    None,
                    Some(b"\0\0\xff\0".as_slice()),
                ])),
                Arc::new(arrow_array_58::TimestampMillisecondArray::from(vec![
                    Some(-2000),
                    None,
                    Some(3000),
                ])),
            ],
        )
        .unwrap();
        let snapshot_id = commit_batch(&lake_table, input).await;

        let info = TableInfo::new(
            TablePath::new("logical", name),
            7,
            1,
            fluss_schema.build().unwrap(),
            vec!["id".to_string()],
            Vec::<String>::new().into(),
            1,
            HashMap::new(),
            HashMap::new(),
            None,
            0,
            0,
        );
        let source = warehouse.source(&info);
        let semantics = if primary_key {
            LakeReadSemantics::PrimaryKey
        } else {
            LakeReadSemantics::Append
        };
        let filter = BoundPredicate::AlwaysTrue;
        let splits = source
            .plan(LakePlannerContext {
                table_info: &info,
                snapshot_id,
                semantics,
                reconcile_primary_key: primary_key,
                filter: &filter,
            })
            .await
            .unwrap();
        assert!(!splits.is_empty());
        let full_schema = fluss::record::to_arrow_schema(info.row_type()).unwrap();
        // Reordering and pruning must be adapted to the physical read
        // schema, not to full-table column positions.
        for projection in [vec![0, 1, 2, 3], vec![3, 2, 1, 0], vec![2, 0]] {
            let schema = Arc::new(full_schema.project(&projection).unwrap());
            let batches: Vec<RecordBatch> = source
                .read(LakeReaderContext {
                    table_info: &info,
                    snapshot_id,
                    semantics,
                    reconcile_primary_key: primary_key,
                    splits: &splits,
                    projection: &projection,
                    schema: schema.clone(),
                    filter: &filter,
                })
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
            let mut ids = Vec::new();
            for batch in &batches {
                assert_eq!(batch.schema(), schema);
                let id_position = projection.iter().position(|index| *index == 0).unwrap();
                for row in 0..batch.num_rows() {
                    let id = batch
                        .column(id_position)
                        .as_primitive::<arrow::datatypes::Int32Type>()
                        .value(row);
                    ids.push(id);
                    for (position, column) in projection.iter().enumerate() {
                        let array = batch.column(position);
                        if *column == 0 {
                            continue;
                        }
                        assert_eq!(array.is_null(row), id == 2);
                        if id == 2 {
                            continue;
                        }
                        match column {
                            1 => {
                                let values = array.as_list::<i32>().value(row);
                                let values = values
                                    .as_primitive::<arrow::datatypes::Int32Type>()
                                    .iter()
                                    .collect::<Vec<_>>();
                                assert_eq!(
                                    values,
                                    if id == 1 { vec![Some(1), None] } else { vec![] }
                                );
                            }
                            2 => assert_eq!(
                                array.as_fixed_size_binary().value(row),
                                if id == 1 { b"abcd" } else { b"\0\0\xff\0" }
                            ),
                            3 => assert_eq!(
                                array
                                    .as_primitive::<arrow::datatypes::TimestampSecondType>()
                                    .value(row),
                                if id == 1 { -2 } else { 3 }
                            ),
                            _ => unreachable!(),
                        }
                    }
                }
            }
            ids.sort_unstable();
            assert_eq!(ids, vec![1, 2, 3]);
        }
    }
}

struct FailingBaseline {
    polling_failure: bool,
}
impl PaimonBaselineReader for FailingBaseline {
    fn read<'a>(
        &'a self,
        _: PaimonReadContext<'a>,
    ) -> futures::future::BoxFuture<'a, Result<Option<RecordBatchStream>>> {
        Box::pin(async move {
            let error = FlussLakeError::DataUnavailable("native input expired".into());
            if self.polling_failure {
                Ok(Some(
                    Box::pin(futures::stream::once(async move { Err(error) })) as RecordBatchStream,
                ))
            } else {
                Err(error)
            }
        })
    }
}
