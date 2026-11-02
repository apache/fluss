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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-neutral reconciliation for a deduplicate primary-key current view.
//!
//! A caller first folds a bounded changelog tail in offset order, then streams
//! a baseline image through [`DeduplicateCurrentView::reconcile_baseline_batch`],
//! and finally calls [`DeduplicateCurrentView::finish`] to emit surviving tail
//! rows. The baseline may come from a lake snapshot, a Fluss KV snapshot, or
//! another source that returns at most one row per primary key.

use crate::error::{Error, Result};
use crate::record::ChangeType;
use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::{filter_record_batch, interleave_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, Rows, SortField};
use std::collections::HashMap;

/// Location of one surviving changelog row inside a retained Arrow batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChangelogRowRef {
    batch_index: usize,
    row_index: usize,
}

struct RetainedBatch {
    batch: RecordBatch,
    live_rows: usize,
}

/// Reconciles a deduplicate primary-key baseline with a bounded changelog tail.
///
/// The tail is the small side of a hash overlay:
///
/// - `INSERT` and `UPDATE_AFTER` retain the latest row for a key.
/// - `DELETE` and `UPDATE_BEFORE` retain a tombstone for a key.
/// - A baseline row is emitted only when its key is absent from the overlay.
/// - Surviving non-tombstone tail rows are emitted by [`finish`](Self::finish).
///
/// Calls to [`fold_changelog_batch`](Self::fold_changelog_batch) must follow
/// changelog offset order. The type intentionally does not impose a memory cap
/// or spill policy; those are outside the current reconciliation contract.
/// Fully superseded batches are released, but a partially live batch may still
/// retain buffers for obsolete rows.
pub struct DeduplicateCurrentView {
    entries: HashMap<Vec<u8>, Option<ChangelogRowRef>>,
    changelog_batches: HashMap<usize, RetainedBatch>,
    next_batch_index: usize,
    key_converter: RowConverter,
    primary_key_positions: Vec<usize>,
    schema: SchemaRef,
}

impl DeduplicateCurrentView {
    /// Creates an empty reconciliation state for one physical read schema.
    ///
    /// `primary_key_positions` are positions within `schema`, not positions in
    /// the table's unprojected schema. Callers may widen a requested projection
    /// with hidden key columns before constructing this value.
    pub fn try_new(
        schema: SchemaRef,
        primary_key_positions: Vec<usize>,
    ) -> Result<DeduplicateCurrentView> {
        if primary_key_positions.is_empty() {
            return Err(illegal_argument(
                "deduplicate current-view reconciliation requires at least one primary-key column",
            ));
        }

        let mut sort_fields = Vec::with_capacity(primary_key_positions.len());
        for position in &primary_key_positions {
            let field = schema.fields().get(*position).ok_or_else(|| {
                illegal_argument(format!(
                    "primary-key column position {position} exceeds the physical read width {}",
                    schema.fields().len()
                ))
            })?;
            sort_fields.push(SortField::new(field.data_type().clone()));
        }
        let key_converter = RowConverter::new(sort_fields)
            .map_err(|error| arrow_error("failed to create the primary-key row encoder", error))?;

        Ok(DeduplicateCurrentView {
            entries: HashMap::new(),
            changelog_batches: HashMap::new(),
            next_batch_index: 0,
            key_converter,
            primary_key_positions,
            schema,
        })
    }

    /// Folds one changelog batch into the current-view overlay.
    ///
    /// `change_types` must align one-to-one with the batch rows. Later calls,
    /// and later rows within one call, overwrite earlier values for the same
    /// primary key.
    pub fn fold_changelog_batch(
        &mut self,
        batch: RecordBatch,
        change_types: &[ChangeType],
    ) -> Result<()> {
        if change_types.len() != batch.num_rows() {
            return Err(illegal_argument(format!(
                "changelog batch has {} rows but {} change types",
                batch.num_rows(),
                change_types.len()
            )));
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // Reject the entire batch before changing state.
        if let Some(row_index) = change_types
            .iter()
            .position(|kind| *kind == ChangeType::AppendOnly)
        {
            return Err(illegal_argument(format!(
                "deduplicate current-view reconciliation received an append-only record at row {row_index}; a primary-key changelog is required"
            )));
        }

        let batch = self.normalize(&batch)?;
        let keys = self.encode_keys(&batch)?;
        let batch_index = self.next_batch_index;
        self.next_batch_index = batch_index
            .checked_add(1)
            .ok_or_else(|| illegal_argument("too many changelog batches"))?;
        let mut live_rows = 0;

        for (row_index, change_type) in change_types.iter().enumerate() {
            let row = match change_type {
                ChangeType::Insert | ChangeType::UpdateAfter => {
                    live_rows += 1;
                    Some(ChangelogRowRef {
                        batch_index,
                        row_index,
                    })
                }
                ChangeType::Delete | ChangeType::UpdateBefore => None,
                ChangeType::AppendOnly => unreachable!("validated before folding"),
            };
            if let Some(Some(previous)) = self
                .entries
                .insert(keys.row(row_index).as_ref().to_vec(), row)
            {
                if previous.batch_index == batch_index {
                    live_rows -= 1;
                } else {
                    release_row(&mut self.changelog_batches, previous.batch_index);
                }
            }
        }
        if live_rows > 0 {
            self.changelog_batches
                .insert(batch_index, RetainedBatch { batch, live_rows });
        }
        Ok(())
    }

    /// Suppresses baseline rows superseded or deleted by the folded tail.
    ///
    /// Returns `None` when no row in this batch remains current. The returned
    /// batch uses the schema supplied to [`try_new`](Self::try_new), even when
    /// the input differs only in field metadata or nullability.
    pub fn reconcile_baseline_batch(&self, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let batch = self.normalize(batch)?;
        let keys = self.encode_keys(&batch)?;
        let keep: BooleanArray = (0..batch.num_rows())
            .map(|row_index| Some(!self.entries.contains_key(keys.row(row_index).as_ref())))
            .collect();
        if keep.true_count() == 0 {
            return Ok(None);
        }
        if keep.true_count() == batch.num_rows() {
            return Ok(Some(batch));
        }

        filter_record_batch(&batch, &keep)
            .map(Some)
            .map_err(|error| arrow_error("failed to filter superseded baseline rows", error))
    }

    /// Emits the latest surviving non-tombstone rows from the changelog tail.
    ///
    /// This consumes the reconciliation state and must be called only after the
    /// complete baseline has been reconciled. Output order is deterministic for
    /// one folded tail but is not a primary-key ordering guarantee.
    pub fn finish(self) -> Result<Option<RecordBatch>> {
        self.finish_batches(usize::MAX)?.next().transpose()
    }

    /// Emits surviving tail rows in batches of at most `batch_size` rows.
    ///
    /// Call only after reconciling the complete baseline. This avoids building
    /// one tail-sized output batch; retained input batches are released after
    /// their last survivor is emitted. Keys and tombstones are dropped when
    /// this iterator is constructed. Survivor indexes still require memory
    /// proportional to the number of surviving rows, and there is no spill.
    pub fn finish_batches(
        self,
        batch_size: usize,
    ) -> Result<impl Iterator<Item = Result<RecordBatch>> + Send> {
        if batch_size == 0 {
            return Err(illegal_argument("output batch size must be positive"));
        }
        let mut indices: Vec<(usize, usize)> = self
            .entries
            .values()
            .filter_map(|value| value.map(|row| (row.batch_index, row.row_index)))
            .collect();
        indices.sort_unstable();
        let mut indices = indices.into_iter();
        let mut retained = self.changelog_batches;
        Ok(std::iter::from_fn(move || {
            let selected: Vec<_> = indices.by_ref().take(batch_size).collect();
            if selected.is_empty() {
                return None;
            }
            // Include only batches referenced by this output chunk, rather
            // than scanning all retained batches on every poll.
            let mut batches = Vec::new();
            let mut positions = HashMap::new();
            let local_indices: Vec<_> = selected
                .iter()
                .map(|(batch_id, row)| {
                    let position = *positions.entry(*batch_id).or_insert_with(|| {
                        let position = batches.len();
                        batches.push(&retained[batch_id].batch);
                        position
                    });
                    (position, *row)
                })
                .collect();
            let result = interleave_record_batch(&batches, &local_indices)
                .map_err(|error| arrow_error("failed to emit surviving changelog rows", error));
            for (batch_id, _) in selected {
                release_row(&mut retained, batch_id);
            }
            if result.is_err() {
                indices = Vec::new().into_iter();
                retained.clear();
            }
            Some(result)
        }))
    }

    fn encode_keys(&self, batch: &RecordBatch) -> Result<Rows> {
        let key_columns = self
            .primary_key_positions
            .iter()
            .map(|position| batch.column(*position).clone())
            .collect::<Vec<_>>();
        self.key_converter
            .convert_columns(&key_columns)
            .map_err(|error| arrow_error("failed to encode primary keys", error))
    }

    fn normalize(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if batch.schema_ref() == &self.schema {
            return Ok(batch.clone());
        }
        if batch.num_columns() != self.schema.fields().len() {
            return Err(illegal_argument(format!(
                "current-view input has {} columns but the physical read schema has {}",
                batch.num_columns(),
                self.schema.fields().len()
            )));
        }
        for (position, expected) in self.schema.fields().iter().enumerate() {
            let actual = batch.schema_ref().field(position);
            if actual.name() != expected.name() || actual.data_type() != expected.data_type() {
                return Err(illegal_argument(format!(
                    "current-view input column {position} is {}:{} but the physical read schema expects {}:{}",
                    actual.name(),
                    actual.data_type(),
                    expected.name(),
                    expected.data_type()
                )));
            }
        }

        RecordBatch::try_new(self.schema.clone(), batch.columns().to_vec()).map_err(|error| {
            arrow_error(
                "failed to restate a current-view input under the physical read schema",
                error,
            )
        })
    }
}

fn release_row(batches: &mut HashMap<usize, RetainedBatch>, batch_id: usize) {
    let retained = batches
        .get_mut(&batch_id)
        .expect("surviving row has a retained batch");
    retained.live_rows -= 1;
    if retained.live_rows == 0 {
        batches.remove(&batch_id);
    }
}

fn illegal_argument(message: impl Into<String>) -> Error {
    Error::IllegalArgument {
        message: message.into(),
    }
}

fn arrow_error(message: impl Into<String>, source: arrow::error::ArrowError) -> Error {
    Error::ArrowError {
        message: message.into(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
        ]))
    }

    fn batch(ids: Vec<i32>, names: Vec<&str>, amounts: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(Int64Array::from(amounts)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn current_view() -> DeduplicateCurrentView {
        DeduplicateCurrentView::try_new(schema(), vec![0]).unwrap()
    }

    fn rows_of(batch: &RecordBatch) -> Vec<(i32, String, i64)> {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amounts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| {
                (
                    ids.value(row),
                    names.value(row).to_string(),
                    amounts.value(row),
                )
            })
            .collect()
    }

    fn reconcile(
        current_view: DeduplicateCurrentView,
        baseline_batches: Vec<RecordBatch>,
    ) -> Vec<(i32, String, i64)> {
        let mut rows = Vec::new();
        for batch in baseline_batches {
            if let Some(batch) = current_view.reconcile_baseline_batch(&batch).unwrap() {
                rows.extend(rows_of(&batch));
            }
        }
        if let Some(batch) = current_view.finish().unwrap() {
            rows.extend(rows_of(&batch));
        }
        rows.sort();
        rows
    }

    #[test]
    fn tail_updates_supersede_baseline_rows() {
        let mut current_view = current_view();
        current_view
            .fold_changelog_batch(
                batch(vec![2], vec!["two-v2"], vec![22]),
                &[ChangeType::UpdateAfter],
            )
            .unwrap();

        let rows = reconcile(
            current_view,
            vec![batch(
                vec![1, 2, 3],
                vec!["one", "two", "three"],
                vec![10, 20, 30],
            )],
        );

        assert_eq!(
            rows,
            vec![
                (1, "one".to_string(), 10),
                (2, "two-v2".to_string(), 22),
                (3, "three".to_string(), 30),
            ]
        );
    }

    #[test]
    fn tombstones_remove_keys_from_the_current_view() {
        let mut current_view = current_view();
        current_view
            .fold_changelog_batch(batch(vec![2], vec!["two"], vec![20]), &[ChangeType::Delete])
            .unwrap();

        let rows = reconcile(
            current_view,
            vec![batch(vec![1, 2], vec!["one", "two"], vec![10, 20])],
        );

        assert_eq!(rows, vec![(1, "one".to_string(), 10)]);
    }

    #[test]
    fn update_before_then_after_keeps_the_later_value() {
        let mut current_view = current_view();
        current_view
            .fold_changelog_batch(
                batch(vec![1], vec!["one"], vec![10]),
                &[ChangeType::UpdateBefore],
            )
            .unwrap();
        current_view
            .fold_changelog_batch(
                batch(vec![1], vec!["one-v2"], vec![11]),
                &[ChangeType::UpdateAfter],
            )
            .unwrap();

        let rows = reconcile(current_view, vec![batch(vec![1], vec!["one"], vec![10])]);

        assert_eq!(rows, vec![(1, "one-v2".to_string(), 11)]);
    }

    #[test]
    fn last_writer_wins_within_one_batch() {
        let mut current_view = current_view();
        current_view
            .fold_changelog_batch(
                batch(vec![1, 1], vec!["first", "second"], vec![1, 2]),
                &[ChangeType::Insert, ChangeType::UpdateAfter],
            )
            .unwrap();

        let rows = reconcile(current_view, Vec::new());

        assert_eq!(rows, vec![(1, "second".to_string(), 2)]);
    }

    #[test]
    fn tail_inserts_appear_without_a_baseline() {
        let mut current_view = current_view();
        current_view
            .fold_changelog_batch(
                batch(vec![7, 8], vec!["seven", "eight"], vec![70, 80]),
                &[ChangeType::Insert, ChangeType::Insert],
            )
            .unwrap();

        let rows = reconcile(current_view, Vec::new());

        assert_eq!(
            rows,
            vec![(7, "seven".to_string(), 70), (8, "eight".to_string(), 80)]
        );
    }

    #[test]
    fn hidden_key_columns_remain_available_after_reconciliation() {
        let physical_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
            Field::new("id", DataType::Int32, false),
        ]));
        let physical_batch = |names: Vec<&str>, amounts: Vec<i64>, ids: Vec<i32>| {
            RecordBatch::try_new(
                physical_schema.clone(),
                vec![
                    Arc::new(StringArray::from(names)) as ArrayRef,
                    Arc::new(Int64Array::from(amounts)) as ArrayRef,
                    Arc::new(Int32Array::from(ids)) as ArrayRef,
                ],
            )
            .unwrap()
        };
        let mut current_view =
            DeduplicateCurrentView::try_new(physical_schema.clone(), vec![2]).unwrap();
        current_view
            .fold_changelog_batch(
                physical_batch(vec!["two-v2"], vec![22], vec![2]),
                &[ChangeType::UpdateAfter],
            )
            .unwrap();

        let baseline = current_view
            .reconcile_baseline_batch(&physical_batch(
                vec!["one", "two"],
                vec![10, 20],
                vec![1, 2],
            ))
            .unwrap()
            .unwrap();
        let survivors = current_view.finish().unwrap().unwrap();

        assert_eq!(baseline.schema_ref(), &physical_schema);
        assert_eq!(survivors.schema_ref(), &physical_schema);
        assert_eq!(baseline.num_columns(), 3);
        assert_eq!(survivors.num_columns(), 3);
    }

    #[test]
    fn append_only_records_are_rejected() {
        let mut current_view = current_view();

        let result = current_view.fold_changelog_batch(
            batch(vec![1], vec!["one"], vec![10]),
            &[ChangeType::AppendOnly],
        );

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    }

    #[test]
    fn superseded_and_delete_only_batches_do_not_retain_payloads() {
        let mut view = current_view();
        for amount in 0..100 {
            view.fold_changelog_batch(
                batch(vec![1], vec!["latest"], vec![amount]),
                &[ChangeType::UpdateAfter],
            )
            .unwrap();
            assert_eq!(view.entries.len(), 1);
            assert_eq!(view.changelog_batches.len(), 1);
        }
        view.fold_changelog_batch(
            batch(vec![1], vec!["deleted"], vec![0]),
            &[ChangeType::Delete],
        )
        .unwrap();
        assert!(view.changelog_batches.is_empty());
        assert_eq!(
            view.entries.len(),
            1,
            "tombstone must still suppress the baseline"
        );
        assert!(
            view.reconcile_baseline_batch(&batch(vec![1], vec!["baseline"], vec![1]),)
                .unwrap()
                .is_none()
        );
        assert!(view.finish().unwrap().is_none());
    }

    #[test]
    fn repeated_insert_delete_within_one_batch_keeps_only_last_survivor() {
        let mut view = current_view();
        view.fold_changelog_batch(
            batch(
                vec![1, 1, 1, 2, 2],
                vec!["a", "a", "b", "c", "c"],
                vec![1, 1, 2, 3, 3],
            ),
            &[
                ChangeType::Insert,
                ChangeType::Delete,
                ChangeType::Insert,
                ChangeType::Insert,
                ChangeType::Delete,
            ],
        )
        .unwrap();
        assert_eq!(view.changelog_batches.values().next().unwrap().live_rows, 1);
        assert_eq!(
            rows_of(&view.finish().unwrap().unwrap()),
            vec![(1, "b".to_string(), 2)]
        );
    }

    #[test]
    fn invalid_batch_does_not_partially_overwrite_the_overlay() {
        let mut view = current_view();
        view.fold_changelog_batch(
            batch(vec![1], vec!["original"], vec![1]),
            &[ChangeType::Insert],
        )
        .unwrap();
        assert!(
            view.fold_changelog_batch(
                batch(vec![1, 2], vec!["bad", "bad"], vec![2, 2]),
                &[ChangeType::Delete, ChangeType::AppendOnly],
            )
            .is_err()
        );
        assert_eq!(view.changelog_batches.len(), 1);
        assert_eq!(
            rows_of(&view.finish().unwrap().unwrap()),
            vec![(1, "original".to_string(), 1)]
        );
    }

    #[test]
    fn survivors_are_materialized_in_bounded_deterministic_batches() {
        for batch_size in [1, 2, 4, 100] {
            let mut view = current_view();
            for (ids, values) in [
                (vec![1, 2, 3], vec![1, 2, 3]),
                (vec![2, 4, 5], vec![22, 4, 5]),
            ] {
                view.fold_changelog_batch(
                    batch(ids, vec!["v"; 3], values),
                    &[ChangeType::Insert; 3],
                )
                .unwrap();
            }
            let batches = view
                .finish_batches(batch_size)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            assert!(batches.iter().all(|batch| batch.num_rows() <= batch_size));
            let rows: Vec<_> = batches.iter().flat_map(rows_of).collect();
            assert_eq!(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                vec![1, 3, 2, 4, 5]
            );
            assert_eq!(rows[2].2, 22);
        }
        assert!(current_view().finish_batches(0).is_err());
        assert_eq!(current_view().finish_batches(1).unwrap().count(), 0);
    }

    #[test]
    fn invalid_key_positions_are_rejected() {
        assert!(matches!(
            DeduplicateCurrentView::try_new(schema(), vec![9]),
            Err(Error::IllegalArgument { .. })
        ));
        assert!(matches!(
            DeduplicateCurrentView::try_new(schema(), Vec::new()),
            Err(Error::IllegalArgument { .. })
        ));
    }

    #[test]
    fn mismatched_change_type_count_is_rejected() {
        let mut current_view = current_view();

        let result = current_view.fold_changelog_batch(
            batch(vec![1, 2], vec!["one", "two"], vec![10, 20]),
            &[ChangeType::Insert],
        );

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    }

    #[test]
    fn inputs_that_disagree_on_columns_are_rejected() {
        let current_view = current_view();
        let narrow = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();

        assert!(matches!(
            current_view.reconcile_baseline_batch(&narrow),
            Err(Error::IllegalArgument { .. })
        ));
    }

    #[test]
    fn inputs_that_differ_only_in_nullability_are_normalized() {
        let current_view = current_view();
        let relaxed_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
        ]));
        let relaxed = RecordBatch::try_new(
            relaxed_schema,
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["one"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
            ],
        )
        .unwrap();

        let kept = current_view
            .reconcile_baseline_batch(&relaxed)
            .unwrap()
            .unwrap();

        assert_eq!(kept.schema_ref(), &schema());
        assert_eq!(rows_of(&kept), vec![(1, "one".to_string(), 10)]);
    }
}
