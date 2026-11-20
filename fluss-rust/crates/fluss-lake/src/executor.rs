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

//! Executes one frozen task: open inputs, reconcile if needed, then filter and project.

use crate::error::{error_message, execution_client_error};
use crate::table::BoundScan;
use crate::{
    FlussLakeError, FlussLakeReadSplit, FlussLakeScan, FlussLakeSplitDescriptor, RecordBatchStream,
    Result,
};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use fluss::client::{BoundedLogReadRange, DeduplicateCurrentView, FlussTable, ReadMemoryBudget};
use fluss::error::Error as ClientError;
use fluss::metadata::{RowType, TableInfo};
use fluss::predicate::{BoundPredicate, Predicate};
use futures::{StreamExt, TryStreamExt};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::OnceCell;

const DEFAULT_SURVIVOR_BATCH_ROWS: usize = 4096;

/// Resources shared only by tasks in one read attempt.
#[derive(Default)]
pub(crate) struct ReadResources {
    metadata: OnceCell<TableInfo>,
    binding: std::sync::OnceLock<(fluss::TableId, i32, Arc<BoundScan>)>,
    lake_source: OnceCell<Arc<dyn crate::LakeSource>>,
}

/// Executes a frozen task using the reader's immutable scan configuration.
pub(crate) fn execute_split(
    split: &FlussLakeReadSplit,
    scan: &Arc<FlussLakeScan>,
    resources: Arc<ReadResources>,
    memory_budget: Option<Arc<dyn ReadMemoryBudget>>,
) -> Result<RecordBatchStream> {
    if split.table_path() != scan.table_path() {
        return Err(FlussLakeError::PlanningFailed(
            "split and reader refer to different tables".to_string(),
        ));
    }
    let descriptor = split.descriptor();
    if descriptor.kv_snapshot_id().is_some() {
        return Err(FlussLakeError::PlanningFailed(
            "KV snapshot baseline execution is not yet supported by the UnionRead reader".into(),
        ));
    }
    if scan.lake_only() && descriptor.start_offset() != descriptor.stop_offset() {
        return Err(FlussLakeError::PlanningFailed(
            "lake-only reader cannot execute a log range".into(),
        ));
    }
    let info = split.table_info();
    let bound = match resources.binding.get() {
        Some((table_id, schema_id, bound))
            if (*table_id, *schema_id) == (info.table_id, info.schema_id) =>
        {
            bound.clone()
        }
        _ => {
            let bound = Arc::new(scan.bind(info.row_type())?);
            let _ = resources
                .binding
                .set((info.table_id, info.schema_id, bound.clone()));
            bound
        }
    };
    descriptor.validate_filter(&bound.filter_digest)?;
    if split.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }
    Ok(lazy_stream(open_logical_stream(
        split.clone(),
        bound,
        scan.clone(),
        resources,
        memory_budget,
    )))
}

async fn open_logical_stream(
    split: FlussLakeReadSplit,
    bound: Arc<BoundScan>,
    scan: Arc<FlussLakeScan>,
    resources: Arc<ReadResources>,
    memory_budget: Option<Arc<dyn ReadMemoryBudget>>,
) -> Result<RecordBatchStream> {
    // Share the live identity check within one read attempt, not across queries.
    // Task metadata remains fixed even if the live schema has changed.
    let live = resources
        .metadata
        .get_or_try_init(|| async {
            scan.connection()
                .get_table(scan.table_path())
                .await
                .map(|table| table.get_table_info().clone())
                .map_err(|error| execution_client_error("open Fluss table", error))
        })
        .await?;
    split.validate_live_table(live)?;
    let descriptor = split.descriptor();
    let frozen = split.table_info().clone();
    let table = FlussTable::new(scan.connection(), scan.connection().get_metadata(), frozen);
    let table_info = table.get_table_info();
    let BoundScan {
        projection: output_projection,
        filter,
        ..
    } = bound.as_ref();
    let pk_union = split.is_primary_key() && !scan.lake_only();
    if pk_union {
        crate::table::validate_pk_union_merge_engine(table_info)?;
    }
    let has_log_tail = descriptor.start_offset() != descriptor.stop_offset();
    let physical = PhysicalProjection::resolve(
        table_info.row_type(),
        output_projection.as_deref(),
        filter,
        if pk_union && has_log_tail {
            split.primary_key_indexes()
        } else {
            &[]
        },
    )?;
    let physical_schema = physical.arrow_schema(table_info.row_type())?;
    let lake_stream = if descriptor.lake_splits().is_empty() {
        Box::pin(futures::stream::empty()) as RecordBatchStream
    } else {
        let source = resources
            .lake_source
            .get_or_try_init(|| async { scan.resolve_lake_source(table_info) })
            .await?;
        let snapshot_id = descriptor
            .snapshot_id()
            .ok_or_else(|| FlussLakeError::Internal("lake tasks need a snapshot".to_string()))?;
        let tasks = descriptor.lake_splits();
        source
            .read(crate::LakeReaderContext {
                table_info,
                snapshot_id,
                semantics: if split.is_primary_key() {
                    crate::LakeReadSemantics::PrimaryKey
                } else {
                    crate::LakeReadSemantics::Append
                },
                reconcile_primary_key: pk_union,
                splits: tasks,
                projection: &physical.field_indexes,
                schema: physical_schema.clone(),
                filter,
            })
            .await?
    };
    let lake_stream = normalize_stream_schema(lake_stream, physical_schema.clone());
    let output_column_count = output_projection.as_deref().map(|indexes| indexes.len());
    let batch_size = scan.max_batch_size();
    let stream = if !has_log_tail {
        lake_stream
    } else if pk_union {
        let mut current_view = match memory_budget {
            Some(budget) => DeduplicateCurrentView::try_new_with_memory_budget(
                physical_schema,
                physical.key_positions.clone(),
                budget,
            ),
            None => {
                DeduplicateCurrentView::try_new(physical_schema, physical.key_positions.clone())
            }
        }
        .map_err(reconciliation_error)?;
        fold_logical_changelog_tail(&table, descriptor, &physical, &mut current_view).await?;
        reconciled_stream(
            current_view,
            lake_stream,
            batch_size.unwrap_or(DEFAULT_SURVIVOR_BATCH_ROWS),
        )
    } else {
        let log_filter = filter.to_pushdown_predicate();
        let log_stream = normalize_stream_schema(
            open_logical_append_log_stream(&table, descriptor, &physical, log_filter.as_ref())
                .await?,
            physical_schema,
        );
        Box::pin(lake_stream.chain(log_stream))
    };
    Ok(apply_output_processing(
        stream,
        filter,
        output_column_count,
        batch_size,
    ))
}

async fn open_logical_append_log_stream(
    table: &FlussTable<'_>,
    descriptor: &FlussLakeSplitDescriptor,
    physical: &PhysicalProjection,
    filter: Option<&Predicate>,
) -> Result<RecordBatchStream> {
    let mut table_scan = table
        .new_scan()
        .project(&physical.field_indexes)
        .map_err(|error| execution_client_error("apply append-log projection", error))?;
    if let Some(filter) = filter {
        table_scan = table_scan
            .filter(filter.clone())
            .map_err(|error| execution_client_error("apply append-log filter pushdown", error))?;
    }
    let reader = table_scan
        .create_bounded_log_reader(vec![log_range(descriptor)])
        .await
        .map_err(|error| execution_client_error("create bounded append-log reader", error))?;
    Ok(Box::pin(reader.into_stream().map(|result| {
        result
            .map(|scan_batch| scan_batch.into_batch())
            .map_err(|error| execution_client_error("read bounded append-log split", error))
    })))
}

fn log_range(descriptor: &FlussLakeSplitDescriptor) -> BoundedLogReadRange {
    BoundedLogReadRange {
        bucket: descriptor.table_bucket().clone(),
        starting_offset: descriptor.start_offset(),
        stopping_offset: descriptor.stop_offset(),
    }
}

async fn fold_logical_changelog_tail(
    table: &FlussTable<'_>,
    descriptor: &FlussLakeSplitDescriptor,
    physical: &PhysicalProjection,
    current_view: &mut DeduplicateCurrentView,
) -> Result<()> {
    let mut reader = table
        .new_scan()
        .project(&physical.field_indexes)
        .map_err(|error| execution_client_error("apply changelog projection", error))?
        .create_bounded_log_reader(vec![log_range(descriptor)])
        .await
        .map_err(|error| execution_client_error("create bounded changelog reader", error))?;
    while let Some(batch) = reader
        .next_batch()
        .await
        .map_err(|error| execution_client_error("read bounded changelog tail", error))?
    {
        let changes = batch.change_types().ok_or_else(|| {
            FlussLakeError::DataUnavailable(
                "expected primary-key changelog, received an append-only batch".into(),
            )
        })?;
        current_view
            .fold_changelog_batch(batch.batch().clone(), changes)
            .map_err(reconciliation_error)?;
    }
    Ok(())
}

/// Output columns come first so predicate-only columns can be stripped after filtering.
struct PhysicalProjection {
    field_indexes: Vec<usize>,
    key_positions: Vec<usize>,
}

impl PhysicalProjection {
    fn resolve(
        row_type: &RowType,
        output_projection: Option<&[usize]>,
        filter: &BoundPredicate,
        keys: &[usize],
    ) -> Result<Self> {
        let field_count = row_type.fields().len();
        let mut field_indexes = match output_projection {
            Some(projection) => projection.to_vec(),
            None => (0..field_count).collect(),
        };
        for field_index in filter.referenced_field_indexes() {
            if !field_indexes.contains(&field_index) {
                field_indexes.push(field_index);
            }
        }
        let key_positions = keys
            .iter()
            .map(|key| {
                if let Some(index) = field_indexes.iter().position(|field| field == key) {
                    index
                } else {
                    field_indexes.push(*key);
                    field_indexes.len() - 1
                }
            })
            .collect();
        // Some readers cannot preserve row counts for a zero-column physical
        // read. Read one hidden field and project it away after filtering.
        if field_indexes.is_empty() && field_count > 0 {
            field_indexes.push(0);
        }
        Ok(Self {
            field_indexes,
            key_positions,
        })
    }

    fn arrow_schema(&self, row_type: &RowType) -> Result<arrow::datatypes::SchemaRef> {
        let schema = fluss::record::to_arrow_schema(row_type).map_err(|error| {
            FlussLakeError::Internal(format!(
                "failed to convert the physical read schema to Arrow: {error}"
            ))
        })?;
        schema
            .project(&self.field_indexes)
            .map(Arc::new)
            .map_err(|error| {
                FlussLakeError::Internal(format!(
                    "failed to project the physical UnionRead schema: {error}"
                ))
            })
    }
}

/// Defers an asynchronous stream setup until the stream's first poll.
///
/// Setup failures surface as the first (and only) stream item instead of
/// failing the `execute` call, keeping `execute` synchronous and lazy.
pub(crate) fn lazy_stream<F>(setup: F) -> RecordBatchStream
where
    F: Future<Output = Result<RecordBatchStream>> + Send + 'static,
{
    Box::pin(futures::stream::once(setup).try_flatten())
}

/// Reconciles a source-neutral deduplicate state with a lake baseline stream.
fn reconciled_stream(
    current_view: DeduplicateCurrentView,
    baseline_stream: RecordBatchStream,
    batch_size: usize,
) -> RecordBatchStream {
    enum Phase<I> {
        Baseline(Box<DeduplicateCurrentView>, RecordBatchStream),
        Survivors(I),
    }

    Box::pin(futures::stream::try_unfold(
        Phase::Baseline(Box::new(current_view), baseline_stream),
        move |phase| async move {
            let mut survivors = match phase {
                Phase::Baseline(view, mut stream) => {
                    while let Some(batch) = stream.try_next().await? {
                        if let Some(batch) = view
                            .reconcile_baseline_batch(&batch)
                            .map_err(reconciliation_error)?
                        {
                            return Ok(Some((batch, Phase::Baseline(view, stream))));
                        }
                    }
                    view.finish_batches(batch_size)
                        .map_err(reconciliation_error)?
                }
                Phase::Survivors(batches) => batches,
            };
            let batch = survivors.next().transpose().map_err(reconciliation_error)?;
            Ok(batch.map(|batch| (batch, Phase::Survivors(survivors))))
        },
    ))
}

fn reconciliation_error(error: ClientError) -> FlussLakeError {
    let message = error_message("reconcile the primary-key current view", &error);
    match error {
        ClientError::BufferExhausted { .. } => FlussLakeError::MemoryLimitExceeded(message),
        _ => FlussLakeError::Internal(message),
    }
}

/// Restates every source batch under the physical schema frozen from Fluss
/// metadata, rejecting name or type drift before any row is emitted.
///
/// Paimon and Fluss may differ in field metadata or nullability, which does not
/// change the physical values. Names, order, and Arrow data types must still
/// agree so that `plan.schema()` remains the schema of every output batch.
fn normalize_stream_schema(
    stream: RecordBatchStream,
    expected_schema: arrow::datatypes::SchemaRef,
) -> RecordBatchStream {
    Box::pin(stream.map(move |result| {
        result.and_then(|batch| normalize_batch_schema(batch, expected_schema.clone()))
    }))
}

fn normalize_batch_schema(
    batch: RecordBatch,
    expected_schema: arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    if batch.schema_ref() == &expected_schema {
        return Ok(batch);
    }
    if batch.num_columns() != expected_schema.fields().len() {
        return Err(FlussLakeError::SchemaIncompatible(format!(
            "UnionRead source produced {} columns, but the frozen physical schema requires {}",
            batch.num_columns(),
            expected_schema.fields().len()
        )));
    }
    for (position, expected) in expected_schema.fields().iter().enumerate() {
        let actual = batch.schema_ref().field(position);
        if actual.name() != expected.name() || actual.data_type() != expected.data_type() {
            return Err(FlussLakeError::SchemaIncompatible(format!(
                "UnionRead source column {position} is {}:{}, but the frozen physical schema requires {}:{}",
                actual.name(),
                actual.data_type(),
                expected.name(),
                expected.data_type()
            )));
        }
    }
    RecordBatch::try_new(expected_schema, batch.columns().to_vec()).map_err(|error| {
        FlussLakeError::SchemaIncompatible(format!(
            "failed to restate a UnionRead source batch under the frozen physical schema: {error}"
        ))
    })
}

/// Applies exact filtering and projection before enforcing output batch size.
fn apply_output_processing(
    stream: RecordBatchStream,
    filter: &BoundPredicate,
    output_column_count: Option<usize>,
    batch_size: Option<usize>,
) -> RecordBatchStream {
    let stream = apply_filter_and_projection(stream, filter, output_column_count);
    match batch_size {
        Some(batch_size) => resize_batches(stream, batch_size),
        None => stream,
    }
}

/// Applies the exact predicate before stripping hidden physical columns.
fn apply_filter_and_projection(
    stream: RecordBatchStream,
    filter: &BoundPredicate,
    output_column_count: Option<usize>,
) -> RecordBatchStream {
    if matches!(filter, BoundPredicate::AlwaysTrue) && output_column_count.is_none() {
        return stream;
    }
    let evaluator = match filter {
        BoundPredicate::AlwaysTrue => None,
        _ => match filter.clone().into_evaluator() {
            Ok(evaluator) => Some(evaluator),
            Err(error) => {
                return Box::pin(futures::stream::once(futures::future::ready(Err(
                    predicate_evaluation_error(error),
                ))));
            }
        },
    };
    let projection = output_column_count.map(|count| (0..count).collect::<Vec<_>>());
    let process = move |batch: RecordBatch| {
        let batch = if let Some(filter) = &evaluator {
            let mask = filter
                .evaluate_batch(&batch)
                .map_err(predicate_evaluation_error)?;
            match mask.true_count() {
                0 => return Ok(None),
                count if count == batch.num_rows() => batch,
                _ => filter_record_batch(&batch, &mask).map_err(|error| {
                    FlussLakeError::Internal(format!(
                        "failed to apply filter to record batch: {error}"
                    ))
                })?,
            }
        } else {
            batch
        };
        if let Some(indices) = &projection
            && indices.len() != batch.num_columns()
        {
            return batch.project(indices).map(Some).map_err(|error| {
                FlussLakeError::Internal(format!(
                    "failed to strip hidden UnionRead columns after filtering: {error}"
                ))
            });
        }
        Ok(Some(batch))
    };
    Box::pin(stream.try_filter_map(move |batch| futures::future::ready(process(batch))))
}

/// Slices oversized batches so every emitted batch contains at most
/// `batch_size` rows.
///
/// Small source batches are intentionally not coalesced. This keeps the
/// transform lazy and bounded while still honoring the scan's output limit.
fn resize_batches(stream: RecordBatchStream, batch_size: usize) -> RecordBatchStream {
    debug_assert!(batch_size > 0);
    Box::pin(futures::stream::try_unfold(
        (stream, None::<(RecordBatch, usize)>),
        move |(mut stream, mut pending)| async move {
            loop {
                if let Some((batch, offset)) = pending.take()
                    && offset < batch.num_rows()
                {
                    let row_count = batch_size.min(batch.num_rows() - offset);
                    let next_offset = offset + row_count;
                    let next_pending =
                        (next_offset < batch.num_rows()).then_some((batch.clone(), next_offset));
                    return Ok(Some((
                        batch.slice(offset, row_count),
                        (stream, next_pending),
                    )));
                }

                match stream.next().await {
                    Some(Ok(batch)) if batch.num_rows() == 0 => {}
                    Some(Ok(batch)) => pending = Some((batch, 0)),
                    Some(Err(error)) => return Err(error),
                    None => return Ok(None),
                }
            }
        },
    ))
}

fn predicate_evaluation_error(error: ClientError) -> FlussLakeError {
    match error {
        ClientError::IllegalArgument { message } => FlussLakeError::SchemaIncompatible(message),
        error => FlussLakeError::Internal(error_message("evaluate UnionRead predicate", &error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use fluss::metadata::{DataField, DataTypes};
    use fluss::predicate::{Predicate, col};
    use futures::{StreamExt, stream};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn bound(predicate: Predicate) -> BoundPredicate {
        BoundPredicate::bind(Some(&predicate), &pk_row_type()).unwrap()
    }

    fn id_batch(ids: Vec<i32>) -> RecordBatch {
        RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(ids)) as ArrayRef)]).unwrap()
    }

    fn collect_batches(stream: RecordBatchStream) -> Result<Vec<RecordBatch>> {
        futures::executor::block_on(stream.try_collect())
    }

    #[test]
    fn empty_output_projection_preserves_filtered_row_counts() {
        let filter = bound(col("amount").gt(10_i64));
        let physical =
            PhysicalProjection::resolve(&pk_row_type(), Some(&[]), &filter, &[]).unwrap();
        assert_eq!(physical.field_indexes, vec![2]);
        let schema = physical.arrow_schema(&pk_row_type()).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![5, 15, 25]))],
        )
        .unwrap();
        let stream = apply_output_processing(
            Box::pin(futures::stream::iter([Ok(batch)])),
            &filter,
            Some(0),
            Some(1),
        );
        let batches = collect_batches(stream).unwrap();
        assert_eq!(batches.len(), 2);
        assert!(
            batches
                .iter()
                .all(|b| b.num_rows() == 1 && b.num_columns() == 0)
        );

        let physical = PhysicalProjection::resolve(
            &pk_row_type(),
            Some(&[]),
            &BoundPredicate::AlwaysTrue,
            &[],
        )
        .unwrap();
        assert_eq!(physical.field_indexes, vec![0]);
    }

    /// Setup work must not run until the stream is polled, and its failure
    /// must arrive as the first stream item rather than from `execute`.
    #[test]
    fn lazy_stream_defers_setup_until_the_first_poll() {
        let setup_ran = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&setup_ran);
        let mut stream = lazy_stream(async move {
            flag.store(true, Ordering::SeqCst);
            Err(FlussLakeError::Internal("deferred failure".to_string()))
        });

        assert!(!setup_ran.load(Ordering::SeqCst));

        let first = futures::executor::block_on(stream.next());

        assert!(setup_ran.load(Ordering::SeqCst));
        assert!(matches!(first, Some(Err(FlussLakeError::Internal(_)))));
        assert!(futures::executor::block_on(stream.next()).is_none());
    }

    fn pk_row_type() -> RowType {
        RowType::new(vec![
            DataField::new("id", DataTypes::int(), None),
            DataField::new("region", DataTypes::string(), None),
            DataField::new("amount", DataTypes::bigint(), None),
        ])
    }

    #[test]
    fn physical_projection_keeps_predicate_columns_hidden_from_output() {
        let physical = PhysicalProjection::resolve(
            &pk_row_type(),
            Some(&[2]),
            &bound(col("id").eq(1_i32)),
            &[],
        )
        .unwrap();

        assert_eq!(physical.field_indexes, vec![2, 0]);
    }

    #[test]
    fn apply_filter_emits_only_nonempty_matching_batches() {
        for (threshold, expected) in [(0, vec![1, 2, 3, 4]), (2, vec![3, 4]), (5, vec![])] {
            let source = Box::pin(stream::iter([Ok(id_batch(vec![1, 2, 3, 4]))]));
            let filtered =
                apply_filter_and_projection(source, &bound(col("id").gt(threshold)), None);
            let expected = if expected.is_empty() {
                vec![]
            } else {
                vec![id_batch(expected)]
            };
            assert_eq!(
                collect_batches(filtered).unwrap(),
                expected,
                "id > {threshold}"
            );
        }
    }

    #[test]
    fn exact_filter_runs_before_hidden_columns_are_stripped() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["one", "two"])) as arrow::array::ArrayRef,
                Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef,
            ],
        )
        .unwrap();
        let source: RecordBatchStream =
            Box::pin(stream::iter(vec![Ok::<_, FlussLakeError>(batch)]));
        let filtered = apply_filter_and_projection(source, &bound(col("id").eq(2_i32)), Some(1));

        let batches = collect_batches(filtered).unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "name");
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "two");
    }

    #[test]
    fn exact_filter_never_passes_through_a_missing_column() {
        use arrow::array::StringArray;
        let batch = RecordBatch::try_from_iter(vec![(
            "name",
            Arc::new(StringArray::from(vec!["one"])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let source: RecordBatchStream =
            Box::pin(stream::iter(vec![Ok::<_, FlussLakeError>(batch)]));
        let filtered = apply_filter_and_projection(source, &bound(col("id").eq(1_i32)), None);

        assert!(matches!(
            collect_batches(filtered),
            Err(FlussLakeError::SchemaIncompatible(_))
        ));
    }

    #[test]
    fn source_batches_are_restated_under_the_frozen_physical_schema() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let expected = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef],
        )
        .unwrap();

        let normalized = normalize_batch_schema(source, expected.clone()).unwrap();

        assert_eq!(normalized.schema_ref(), &expected);
    }

    #[test]
    fn source_schema_type_drift_is_rejected_before_output() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let expected = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as arrow::array::ArrayRef],
        )
        .unwrap();

        assert!(matches!(
            normalize_batch_schema(source, expected),
            Err(FlussLakeError::SchemaIncompatible(_))
        ));
    }

    #[test]
    fn output_batch_size_slices_oversized_batches_after_filtering() {
        let source = Box::pin(stream::iter([Ok(id_batch(vec![1, 2, 3, 4, 5]))]));
        let resized = apply_output_processing(source, &bound(col("id").gt(1_i32)), None, Some(3));

        assert_eq!(
            collect_batches(resized).unwrap(),
            vec![id_batch(vec![2, 3, 4]), id_batch(vec![5])]
        );
    }

    #[test]
    fn output_batch_size_does_not_coalesce_small_batches() {
        let expected = vec![id_batch(vec![1, 2]), id_batch(vec![3])];
        let source = Box::pin(stream::iter(expected.clone().into_iter().map(Ok)));
        let resized = resize_batches(source, 3);

        assert_eq!(collect_batches(resized).unwrap(), expected);
    }
    #[test]
    fn pk_budget_is_released_on_baseline_error_and_pending_stream_drop() {
        use arrow::array::Int32Array;
        use fluss::client::LimitedReadMemoryBudget;
        use fluss::record::ChangeType;
        use futures::FutureExt;

        futures::executor::block_on(async {
            for fail in [false, true] {
                let budget = Arc::new(LimitedReadMemoryBudget::new(16_384));
                let batch = RecordBatch::try_from_iter(vec![(
                    "id",
                    Arc::new(Int32Array::from(vec![1])) as arrow::array::ArrayRef,
                )])
                .unwrap();
                let mut view = DeduplicateCurrentView::try_new_with_memory_budget(
                    batch.schema(),
                    vec![0],
                    budget.clone(),
                )
                .unwrap();
                view.fold_changelog_batch(batch, &[ChangeType::Insert])
                    .unwrap();
                let baseline: RecordBatchStream = if fail {
                    Box::pin(futures::stream::once(async {
                        Err(FlussLakeError::DataUnavailable("expired".into()))
                    }))
                } else {
                    Box::pin(futures::stream::pending())
                };
                let mut stream = reconciled_stream(view, baseline, 1);
                if fail {
                    assert!(matches!(
                        stream.next().await,
                        Some(Err(FlussLakeError::DataUnavailable(_)))
                    ));
                    assert_eq!(budget.used_bytes(), 0);
                } else {
                    assert!(stream.next().now_or_never().is_none());
                    assert!(budget.used_bytes() > 0);
                }
                drop(stream);
                assert_eq!(budget.used_bytes(), 0);
            }
            assert!(matches!(
                reconciliation_error(ClientError::BufferExhausted {
                    message: "limit".into()
                }),
                FlussLakeError::MemoryLimitExceeded(_)
            ));
        });
    }
}
