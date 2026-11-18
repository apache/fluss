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

struct Warehouse(tempfile::TempDir);

impl Warehouse {
    async fn new() -> (Self, Arc<dyn Catalog>) {
        let dir = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.set("warehouse", dir.path().to_str().unwrap());
        let catalog = CatalogFactory::create(options).await.unwrap();
        catalog
            .create_database("physical", false, HashMap::new())
            .await
            .unwrap();
        catalog
            .create_database("logical", false, HashMap::new())
            .await
            .unwrap();
        (Self(dir), catalog)
    }

    fn source(&self, info: &TableInfo) -> PaimonLakeSource {
        PaimonLakeSource::new(
            info,
            &HashMap::from([("warehouse".into(), self.0.path().to_str().unwrap().into())]),
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
    let batch = RecordBatch::try_new(
        schema,
        columns
            .into_iter()
            .map(|values| {
                Arc::new(arrow::array::Int32Array::from(values)) as arrow::array::ArrayRef
            })
            .collect(),
    )
    .unwrap();
    commit_batch(&table, batch).await
}

async fn commit_batch(table: &Table, batch: RecordBatch) -> i64 {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let commits = writer.prepare_commit().await.unwrap();
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
    unpartitioned_info(name, schema.build().unwrap(), 2, properties)
}

fn unpartitioned_info(
    name: &str,
    schema: Schema,
    buckets: i32,
    properties: HashMap<String, String>,
) -> TableInfo {
    let keys = schema
        .primary_key()
        .map(|pk| pk.column_names().to_vec())
        .unwrap_or_default();
    TableInfo::new(
        TablePath::new("logical", name),
        7,
        1,
        schema,
        keys,
        vec![].into(),
        buckets,
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
    read_filtered(
        source,
        info,
        snapshot_id,
        reconcile_primary_key,
        &BoundPredicate::AlwaysTrue,
    )
    .await
}

async fn read_filtered(
    source: &PaimonLakeSource,
    info: &TableInfo,
    snapshot_id: i64,
    reconcile_primary_key: bool,
    filter: &BoundPredicate,
) -> Result<Vec<RecordBatch>> {
    let semantics = if info.has_primary_key() {
        LakeReadSemantics::PrimaryKey
    } else {
        LakeReadSemantics::Append
    };
    let tasks = source
        .plan(LakePlannerContext {
            partition_scope: None,
            selected_bucket: None,
            table_info: info,
            snapshot_id,
            semantics,
            reconcile_primary_key,
            filter,
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
                    filter,
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
async fn mapped_snapshot_reads_external_files_and_keeps_frozen_schema() {
    let (warehouse, catalog) = Warehouse::new().await;
    let schema = || {
        paimon::spec::Schema::builder()
            .column("id", PaimonType::Int(IntType::new()))
            .option("bucket", "-1")
            .option("file.format", "parquet")
            .option("data-file.external-paths.strategy", "round-robin")
            .option(
                "data-file.external-paths",
                format!("file://{}", warehouse.0.path().join("external").display()),
            )
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
    let (first, second) = tokio::join!(
        source.pinned_table(&info, snapshot, false),
        source.pinned_table(&info, snapshot, false),
    );
    let first = first.unwrap();
    assert!(Arc::ptr_eq(&first, &second.unwrap()));
    let plan = first.new_read_builder().new_scan().plan().await.unwrap();
    assert!(
        plan.splits()
            .iter()
            .flat_map(|split| split.data_files())
            .all(|file| {
                file.external_path
                    .as_ref()
                    .is_some_and(|path| path.contains("/external/"))
            })
    );
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

    let latest_table = catalog
        .get_table(&Identifier::new("physical", "baseline"))
        .await
        .unwrap();
    let next_snapshot = commit_batch(
        &latest_table,
        RecordBatch::try_from_iter([(
            "id",
            Arc::new(arrow::array::Int32Array::from(vec![13])) as arrow::array::ArrayRef,
        )])
        .unwrap(),
    )
    .await;
    assert_eq!(
        ids(&read(&source, &info, next_snapshot, false).await.unwrap()),
        vec![11, 12, 13]
    );

    catalog
        .alter_table(
            &Identifier::new("physical", "baseline"),
            vec![SchemaChange::rename_column("id".into(), "renamed".into())],
            false,
        )
        .await
        .unwrap();
    // A fresh worker must resolve the frozen schema even after a later rename.
    assert_eq!(
        ids(&read(&warehouse.source(&info), &info, snapshot, false)
            .await
            .unwrap()),
        vec![11, 12]
    );
    assert!(matches!(
        read(&source, &info, next_snapshot + 1, false).await,
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
                partition_scope: None,
                selected_bucket: None,
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
            partition_scope: None,
            selected_bucket: None,
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
async fn mutable_pk_filter_does_not_resurrect_an_older_lake_version() {
    let (warehouse, catalog) = Warehouse::new().await;
    let identifier = Identifier::new("logical", "filtered_pk");
    let schema = paimon::spec::Schema::builder()
        .column("id", PaimonType::Int(IntType::new()))
        .column("value", PaimonType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "2")
        .option("bucket-key", "id")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    write(
        &catalog,
        &identifier,
        schema,
        vec![vec![1, 2], vec![30, 30]],
    )
    .await;
    let table = catalog.get_table(&identifier).await.unwrap();
    let batch = RecordBatch::try_new(
        paimon::arrow::build_target_arrow_schema(table.schema().fields()).unwrap(),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1])),
            Arc::new(arrow::array::Int32Array::from(vec![10])),
        ],
    )
    .unwrap();
    let snapshot_id = commit_batch(&table, batch).await;
    let info = info("filtered_pk", true, HashMap::new());
    let source = warehouse.source(&info);
    let filter = BoundPredicate::bind(
        Some(&fluss::predicate::col("value").gt(20)),
        info.row_type(),
    )
    .unwrap();
    let tasks = source
        .plan(LakePlannerContext {
            table_info: &info,
            snapshot_id,
            semantics: LakeReadSemantics::PrimaryKey,
            reconcile_primary_key: true,
            partition_scope: None,
            selected_bucket: None,
            filter: &filter,
        })
        .await
        .unwrap();
    let mut result = Vec::new();
    for bucket in 0..2 {
        let splits: Vec<_> = tasks
            .iter()
            .filter(|task| task.bucket_id == bucket)
            .cloned()
            .collect();
        let batches: Vec<_> = source
            .read(LakeReaderContext {
                table_info: &info,
                snapshot_id,
                semantics: LakeReadSemantics::PrimaryKey,
                reconcile_primary_key: true,
                splits: &splits,
                projection: &[0, 1],
                schema: fluss::record::to_arrow_schema(info.row_type()).unwrap(),
                filter: &filter,
            })
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        for batch in batches {
            let mask = filter.evaluate_batch(&batch).unwrap();
            result.push(arrow::compute::filter_record_batch(&batch, &mask).unwrap());
        }
    }
    assert_eq!(ids(&result), vec![2]);
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
            partition_scope: None,
            selected_bucket: None,
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
    let scope = fluss::predicate::col("region").eq(7);
    let scoped = source
        .plan(LakePlannerContext {
            partition_scope: Some(&scope),
            selected_bucket: None,
            table_info: &info,
            snapshot_id: snapshot,
            semantics: LakeReadSemantics::Append,
            reconcile_primary_key: false,
            filter: &BoundPredicate::AlwaysTrue,
        })
        .await
        .unwrap();
    assert!(!scoped.is_empty());
    assert!(scoped.iter().all(|task| task.partition
        == crate::FlussLakePartitionIdentity::KeyValues(vec![("region".into(), "7".into())])));
}

#[tokio::test]
async fn parquet_reader_adapts_logical_types_for_append_and_primary_key_baselines() {
    use arrow::array::Array as _;
    use arrow::array::{FixedSizeBinaryArray, Int32Array, ListArray, TimestampSecondArray};
    use arrow::compute::{concat_batches, sort_to_indices, take_record_batch};
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
        let physical_schema =
            paimon::arrow::build_target_arrow_schema(lake_table.schema().fields()).unwrap();
        let list = arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>(
            vec![Some(vec![Some(1), None]), None, Some(vec![])],
        );
        // Use Paimon's actual list child field, rather than Arrow's default.
        let list = arrow::array::make_array(
            list.to_data()
                .into_builder()
                .data_type(physical_schema.field(1).data_type().clone())
                .build()
                .unwrap(),
        );
        let input = RecordBatch::try_new(
            physical_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                list,
                Arc::new(arrow::array::BinaryArray::from(vec![
                    Some(b"abcd".as_slice()),
                    None,
                    Some(b"\0\0\xff\0".as_slice()),
                ])),
                Arc::new(TimestampSecondArray::from(vec![Some(-2), None, Some(3)])),
            ],
        )
        .unwrap();
        let snapshot_id = commit_batch(&lake_table, input).await;

        let info = unpartitioned_info(name, fluss_schema.build().unwrap(), 1, HashMap::new());
        let source = warehouse.source(&info);
        let semantics = if primary_key {
            LakeReadSemantics::PrimaryKey
        } else {
            LakeReadSemantics::Append
        };
        let filter = BoundPredicate::AlwaysTrue;
        let splits = source
            .plan(LakePlannerContext {
                partition_scope: None,
                selected_bucket: None,
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
        let expected = RecordBatch::try_new(
            full_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(ListArray::from_iter_primitive::<
                    arrow::datatypes::Int32Type,
                    _,
                    _,
                >(vec![
                    Some(vec![Some(1), None]),
                    None,
                    Some(vec![]),
                ])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        vec![
                            Some(b"abcd".as_slice()),
                            None,
                            Some(b"\0\0\xff\0".as_slice()),
                        ]
                        .into_iter(),
                        4,
                    )
                    .unwrap(),
                ),
                Arc::new(TimestampSecondArray::from(vec![Some(-2), None, Some(3)])),
            ],
        )
        .unwrap();
        // Reordering and pruning must be adapted to the physical read
        // schema, not to full-table column positions.
        for projection in [vec![3, 2, 1, 0], vec![2, 0]] {
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
            assert!(batches.iter().all(|batch| batch.schema() == schema));
            let batch = concat_batches(&schema, &batches).unwrap();
            let id_position = projection.iter().position(|index| *index == 0).unwrap();
            let order = sort_to_indices(batch.column(id_position), None, None).unwrap();
            let actual = take_record_batch(&batch, &order).unwrap();
            assert_eq!(actual, expected.project(&projection).unwrap());
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

#[tokio::test]
async fn query_authorization_is_rejected_at_planning_and_native_read() {
    let (warehouse, catalog) = Warehouse::new().await;
    let identifier = Identifier::new("logical", "orders");
    let snapshot = write(
        &catalog,
        &identifier,
        paimon::spec::Schema::builder()
            .column("id", PaimonType::Int(IntType::new()))
            .option("bucket", "-1")
            .option("file.format", "parquet")
            .build()
            .unwrap(),
        vec![vec![1]],
    )
    .await;
    let info = info("orders", false, HashMap::new());
    let source = warehouse
        .source(&info)
        .with_baseline_reader(Arc::new(FailingBaseline {
            polling_failure: false,
        }));
    let planner = || LakePlannerContext {
        partition_scope: None,
        selected_bucket: None,
        table_info: &info,
        snapshot_id: snapshot,
        semantics: LakeReadSemantics::Append,
        reconcile_primary_key: false,
        filter: &BoundPredicate::AlwaysTrue,
    };
    let mut splits = source.plan(planner()).await.unwrap();
    // The transport retains this marker but never carries a live grant.
    let mut payload: serde_json::Value = serde_json::from_slice(&splits[0].payload).unwrap();
    payload["query_auth_required"] = true.into();
    splits[0].payload = serde_json::to_vec(&payload).unwrap();
    assert!(matches!(
        source
            .read(LakeReaderContext {
                table_info: &info,
                snapshot_id: snapshot,
                semantics: LakeReadSemantics::Append,
                reconcile_primary_key: false,
                splits: &splits,
                projection: &[0],
                schema: fluss::record::to_arrow_schema(info.row_type()).unwrap(),
                filter: &BoundPredicate::AlwaysTrue,
            })
            .await,
        Err(FlussLakeError::NotLakeReadable(_))
    ));

    catalog
        .alter_table(
            &identifier,
            vec![SchemaChange::set_option(
                "query-auth.enabled".into(),
                "true".into(),
            )],
            false,
        )
        .await
        .unwrap();
    assert!(matches!(
        warehouse.source(&info).plan(planner()).await,
        Err(FlussLakeError::NotLakeReadable(_))
    ));
}

#[tokio::test]
async fn pushdown_preserves_matches_for_exact_filter() {
    use fluss::predicate::col;
    let (warehouse, catalog) = Warehouse::new().await;
    let identifier = Identifier::new("logical", "predicate_probe");
    let schema = paimon::spec::Schema::builder()
        .column("id", PaimonType::Int(IntType::new()))
        .column(
            "ts",
            PaimonType::Timestamp(paimon::spec::TimestampType::new(3).unwrap()),
        )
        .column(
            "b",
            PaimonType::VarBinary(paimon::spec::VarBinaryType::new(8).unwrap()),
        )
        .option("bucket", "1")
        .option("bucket-key", "id")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    catalog
        .create_table(&identifier, schema, false)
        .await
        .unwrap();
    let table = catalog.get_table(&identifier).await.unwrap();
    let batch = RecordBatch::try_new(
        paimon::arrow::build_target_arrow_schema(table.schema().fields()).unwrap(),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 2])),
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![1, 2])),
            Arc::new(arrow::array::BinaryArray::from(vec![
                &[0x80u8][..],
                &[0u8][..],
            ])),
        ],
    )
    .unwrap();
    let snapshot_id = commit_batch(&table, batch).await;
    let info = unpartitioned_info(
        "predicate_probe",
        Schema::builder()
            .column("id", DataTypes::int())
            .column("ts", DataTypes::timestamp_with_precision(3))
            .column("b", DataTypes::bytes())
            .build()
            .unwrap(),
        1,
        HashMap::new(),
    );
    let source = warehouse.source(&info);
    for predicate in [
        col("ts").lt(fluss::row::TimestampNtz::from_millis_nanos(1, 900_000).unwrap()),
        col("b").lt(vec![0u8]),
    ] {
        let filter = BoundPredicate::bind(Some(&predicate), info.row_type()).unwrap();
        let rows = read_filtered(&source, &info, snapshot_id, false, &filter)
            .await
            .unwrap();
        let filtered: Vec<_> = rows
            .iter()
            .map(|batch| {
                arrow::compute::filter_record_batch(batch, &filter.evaluate_batch(batch).unwrap())
                    .unwrap()
            })
            .collect();
        assert_eq!(ids(&filtered), vec![1]);
    }
}

#[tokio::test]
async fn added_nullable_column_reads_from_older_lake_snapshot() {
    let (warehouse, catalog) = Warehouse::new().await;
    let identifier = Identifier::new("logical", "added_column_probe");
    let schema = paimon::spec::Schema::builder()
        .column("id", PaimonType::Int(IntType::new()))
        .option("bucket", "1")
        .option("bucket-key", "id")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    let snapshot_id = write(&catalog, &identifier, schema, vec![vec![1]]).await;
    catalog
        .alter_table(
            &identifier,
            vec![SchemaChange::add_column(
                "added".into(),
                PaimonType::Int(IntType::new()),
            )],
            false,
        )
        .await
        .unwrap();
    let info = unpartitioned_info(
        "added_column_probe",
        Schema::builder()
            .column("id", DataTypes::int())
            .column("added", DataTypes::int())
            .build()
            .unwrap(),
        1,
        HashMap::new(),
    );
    let source = warehouse.source(&info);
    let rows = read(&source, &info, snapshot_id, false).await.unwrap();
    assert_eq!(ids(&rows), vec![1]);
    assert_eq!(rows[0].column(1).null_count(), 1);

    // Even a projection containing only the new column must retain old rows.
    let predicate = fluss::predicate::col("added").is_null();
    let filter = BoundPredicate::bind(Some(&predicate), info.row_type()).unwrap();
    let tasks = source
        .plan(LakePlannerContext {
            table_info: &info,
            snapshot_id,
            partition_scope: None,
            selected_bucket: None,
            semantics: LakeReadSemantics::Append,
            reconcile_primary_key: false,
            filter: &filter,
        })
        .await
        .unwrap();
    for projection in [&[1][..], &[][..]] {
        let schema = Arc::new(
            fluss::record::to_arrow_schema(info.row_type())
                .unwrap()
                .project(projection)
                .unwrap(),
        );
        let rows = source
            .read(LakeReaderContext {
                table_info: &info,
                snapshot_id,
                semantics: LakeReadSemantics::Append,
                reconcile_primary_key: false,
                splits: &tasks,
                projection,
                schema: schema.clone(),
                filter: &filter,
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(rows[0].schema(), schema);
        assert!(
            rows[0]
                .columns()
                .iter()
                .all(|column| column.null_count() == 1)
        );
    }
}
