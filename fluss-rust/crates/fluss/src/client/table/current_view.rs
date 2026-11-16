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

use super::memory::{ReadMemoryBudget, Reservation, checked_bytes, hash_table_bytes};
use crate::error::{Error, Result};
use crate::record::ChangeType;
use arrow::array::{
    Array, ArrayData, ArrayRef, BooleanArray, BooleanBufferBuilder, RecordBatch, UInt64Array,
};
use arrow::compute::{filter_record_batch, interleave_record_batch, take_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, Rows, SortField};
use std::collections::HashMap;
use std::sync::Arc;

/// A surviving changelog row, by its original row index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChangelogRowRef {
    batch_index: usize,
    row_index: usize,
}

struct RetainedBatch {
    batch: RecordBatch,
    live_rows: usize,
    // Next compaction point.
    compact_at: usize,
    // Live bit per original row.
    live: BooleanBufferBuilder,
    // Original row of each position, sorted, once compacted.
    rows: Option<Vec<usize>>,
    // Drop the batch before releasing its charge.
    _memory: Reservation,
}

impl RetainedBatch {
    fn original_row(&self, position: usize) -> usize {
        self.rows.as_ref().map_or(position, |rows| rows[position])
    }

    fn position(&self, original_row: usize) -> usize {
        self.rows.as_ref().map_or(original_row, |rows| {
            rows.binary_search(&original_row)
                .expect("surviving row is retained")
        })
    }

    fn bytes(&self) -> Result<usize> {
        retained_bytes(&self.batch, &self.live, self.rows.as_deref())
    }
}

fn retained_bytes(
    batch: &RecordBatch,
    live: &BooleanBufferBuilder,
    rows: Option<&[usize]>,
) -> Result<usize> {
    checked_bytes(
        retained_batch_bytes(batch)?
            .checked_add(live.capacity().div_ceil(8))
            .and_then(|n| n.checked_add(rows.map_or(0, size_of_val))),
    )
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
/// changelog offset order. The default constructor is unlimited. Use
/// [`Self::try_new_with_memory_budget`] to bound retained state; there is no spill.
/// Fully superseded batches are released. Partially live batches are compacted
/// when half their rows become obsolete, if the budget allows the replacement.
pub struct DeduplicateCurrentView {
    entries: HashMap<Vec<u8>, Option<ChangelogRowRef>>,
    changelog_batches: HashMap<usize, RetainedBatch>,
    next_batch_index: usize,
    key_converter: RowConverter,
    primary_key_positions: Vec<usize>,
    schema: SchemaRef,
    key_bytes: usize,
    entry_capacity: usize,
    batch_capacity: usize,
    // Last field so owned state is destroyed before its reservation is released.
    memory: Reservation,
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
        Self::new(schema, primary_key_positions, None)
    }

    /// Creates an overlay charged to a shared query budget.
    ///
    /// Charges encoded keys, conservative hash-table capacity estimates, retained
    /// Arrow buffers and survivor indexes before retaining/growing that state.
    /// Shared Arrow allocations are conservatively charged per retained batch;
    /// even a small slice is charged for its backing buffers. Temporary encoding,
    /// source batches and output allocations are outside this retained-state budget.
    /// Rejection returns [`Error::BufferExhausted`]; discard the failed read attempt.
    pub fn try_new_with_memory_budget(
        schema: SchemaRef,
        primary_key_positions: Vec<usize>,
        budget: Arc<dyn ReadMemoryBudget>,
    ) -> Result<Self> {
        Self::new(schema, primary_key_positions, Some(budget))
    }

    fn new(
        schema: SchemaRef,
        primary_key_positions: Vec<usize>,
        budget: Option<Arc<dyn ReadMemoryBudget>>,
    ) -> Result<Self> {
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

        let mut memory = Reservation::new(budget);
        memory
            .resize(key_converter.size() + primary_key_positions.capacity() * size_of::<usize>())?;
        Ok(DeduplicateCurrentView {
            entries: HashMap::new(),
            changelog_batches: HashMap::new(),
            next_batch_index: 0,
            key_converter,
            primary_key_positions,
            schema,
            key_bytes: 0,
            entry_capacity: 0,
            batch_capacity: 0,
            memory,
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
        let mut live = BooleanBufferBuilder::new(batch.num_rows());
        live.append_n(batch.num_rows(), false);
        let mut batch_memory = Reservation::new(self.memory.budget());
        let budgeted = self.memory.budget().is_some();
        if budgeted {
            let retain_batch = usize::from(
                change_types
                    .iter()
                    .any(|kind| matches!(kind, ChangeType::Insert | ChangeType::UpdateAfter)),
            );
            if retain_batch != 0 {
                batch_memory.resize(retained_bytes(&batch, &live, None)?)?;
            }
            let mut new_keys = 0usize;
            let mut new_bytes = 0usize;
            for key in keys.iter() {
                if !self.entries.contains_key(key.as_ref()) {
                    new_keys += 1;
                    new_bytes = checked_bytes(new_bytes.checked_add(key.as_ref().len()))?;
                }
            }
            // Count duplicates conservatively during admission, then return the
            // unused estimate after folding. No per-row budget callbacks.
            let entries = checked_bytes(self.entries.len().checked_add(new_keys))?;
            let batches = checked_bytes(self.changelog_batches.len().checked_add(retain_batch))?;
            let bytes = checked_bytes(self.key_bytes.checked_add(new_bytes))?;
            // Retain the old table charge only while a larger allocation is
            // being created. Reserve before mutating any key or row reference.
            let entry_growth = entries > self.entries.capacity();
            let batch_growth = batches > self.changelog_batches.capacity();
            let overlap = checked_bytes(
                hash_table_bytes::<Vec<u8>, Option<ChangelogRowRef>>(if entry_growth {
                    self.entry_capacity
                } else {
                    0
                })?
                .checked_add(hash_table_bytes::<usize, RetainedBatch>(
                    if batch_growth { self.batch_capacity } else { 0 },
                )?),
            )?;
            self.memory.resize(checked_bytes(
                self.state_bytes(
                    bytes,
                    self.entry_capacity.max(entries),
                    self.batch_capacity.max(batches),
                )?
                .checked_add(overlap),
            )?)?;
            let allocation = self
                .entries
                .try_reserve(new_keys)
                .and_then(|()| self.changelog_batches.try_reserve(retain_batch));
            self.entry_capacity = self.entry_capacity.max(self.entries.capacity());
            self.batch_capacity = self.batch_capacity.max(self.changelog_batches.capacity());
            self.memory.resize(self.state_bytes(
                if allocation.is_ok() {
                    bytes
                } else {
                    self.key_bytes
                },
                self.entry_capacity,
                self.batch_capacity,
            )?)?;
            allocation.map_err(|error| Error::BufferExhausted {
                message: format!("failed to grow current-view state: {error}"),
            })?;
        }
        let batch_index = self.next_batch_index;
        self.next_batch_index = batch_index
            .checked_add(1)
            .ok_or_else(|| illegal_argument("too many changelog batches"))?;
        let mut live_rows = 0;
        let mut compact = Vec::new();

        for (row_index, change_type) in change_types.iter().enumerate() {
            let row = match change_type {
                ChangeType::Insert | ChangeType::UpdateAfter => {
                    live_rows += 1;
                    live.set_bit(row_index, true);
                    Some(ChangelogRowRef {
                        batch_index,
                        row_index,
                    })
                }
                ChangeType::Delete | ChangeType::UpdateBefore => None,
                ChangeType::AppendOnly => unreachable!("validated before folding"),
            };
            let key = keys.row(row_index);
            let previous = if let Some(value) = self.entries.get_mut(key.as_ref()) {
                std::mem::replace(value, row)
            } else {
                if budgeted {
                    self.key_bytes += key.as_ref().len();
                }
                self.entries.insert(key.as_ref().to_vec(), row);
                None
            };
            if let Some(previous) = previous {
                if previous.batch_index == batch_index {
                    live_rows -= 1;
                    live.set_bit(previous.row_index, false);
                } else if release_row(
                    &mut self.changelog_batches,
                    previous.batch_index,
                    previous.row_index,
                ) {
                    compact.push(previous.batch_index);
                }
            }
        }
        if live_rows > 0 {
            let compact_at = batch.num_rows() / 2;
            if live_rows <= compact_at {
                compact.push(batch_index);
            }
            self.changelog_batches.insert(
                batch_index,
                RetainedBatch {
                    batch,
                    live_rows,
                    compact_at,
                    live,
                    rows: None,
                    _memory: batch_memory,
                },
            );
        }
        if budgeted {
            self.entry_capacity = self.entry_capacity.max(self.entries.capacity());
            // Removed batches leave the map's allocation in place.
            self.batch_capacity = self.batch_capacity.max(self.changelog_batches.capacity());
            self.memory.resize(self.state_bytes(
                self.key_bytes,
                self.entry_capacity,
                self.batch_capacity,
            )?)?;
        }
        for batch_id in compact {
            self.compact_batch(batch_id)?;
        }
        Ok(())
    }

    fn compact_batch(&mut self, batch_id: usize) -> Result<()> {
        let Some(retained) = self.changelog_batches.get(&batch_id) else {
            return Ok(());
        };
        let mut positions = Vec::with_capacity(retained.live_rows);
        let mut rows = Vec::with_capacity(retained.live_rows);
        for position in 0..retained.batch.num_rows() {
            let original = retained.original_row(position);
            if retained.live.get_bit(original) {
                positions.push(position as u64);
                rows.push(original);
            }
        }
        debug_assert_eq!(rows.len(), retained.live_rows);
        let batch = take_record_batch(&retained.batch, &UInt64Array::from(positions))
            .map_err(|error| arrow_error("failed to compact surviving changelog rows", error))?;
        let mut memory = Reservation::new(self.memory.budget());
        let mut reserved = true;
        if self.memory.budget().is_some() {
            let bytes = retained_bytes(&batch, &retained.live, Some(&rows))?;
            reserved = bytes < retained.bytes()?
                && match memory.resize(bytes) {
                    Ok(()) => true,
                    Err(Error::BufferExhausted { .. }) => false,
                    Err(error) => return Err(error),
                };
        }
        let retained = self
            .changelog_batches
            .get_mut(&batch_id)
            .expect("compacted batch is retained");
        // A skipped compaction retries at the next halving.
        retained.compact_at = retained.live_rows / 2;
        if reserved {
            retained.batch = batch;
            retained.rows = Some(rows);
            retained._memory = memory;
        }
        Ok(())
    }

    fn state_bytes(&self, keys: usize, entries: usize, batches: usize) -> Result<usize> {
        let entry_table = hash_table_bytes::<Vec<u8>, Option<ChangelogRowRef>>(entries)?;
        let batch_table = hash_table_bytes::<usize, RetainedBatch>(batches)?;
        checked_bytes(
            keys.checked_add(entry_table)
                .and_then(|n| n.checked_add(batch_table))
                .and_then(|n| n.checked_add(self.key_converter.size()))
                .and_then(|n| {
                    n.checked_add(self.primary_key_positions.capacity() * size_of::<usize>())
                }),
        )
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
        if self.entries.is_empty() {
            return Ok(Some(batch));
        }
        let key_columns = self
            .primary_key_positions
            .iter()
            .map(|position| batch.column(*position).clone())
            .collect::<Vec<_>>();
        let keep = self.baseline_keep_mask(&key_columns)?;
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

    /// Returns one keep/drop decision per baseline row using only its key columns.
    ///
    /// Supply columns in the order of `primary_key_positions` passed to the
    /// constructor, with the corresponding types and equal lengths. The mask
    /// has no nulls; `false` means the tail replaced or deleted that key. Native
    /// readers can apply it to their own payload without building a full batch.
    pub fn baseline_keep_mask(&self, key_columns: &[ArrayRef]) -> Result<BooleanArray> {
        if key_columns.len() != self.primary_key_positions.len() {
            return Err(illegal_argument(
                "baseline key column count does not match the primary key",
            ));
        }
        let rows = key_columns[0].len();
        for (column, position) in key_columns.iter().zip(&self.primary_key_positions) {
            if column.data_type() != self.schema.field(*position).data_type()
                || column.len() != rows
            {
                return Err(illegal_argument(
                    "baseline key column types or lengths do not match",
                ));
            }
        }
        if self.entries.is_empty() {
            return Ok(BooleanArray::from(vec![true; rows]));
        }
        let keys = self
            .key_converter
            .convert_columns(key_columns)
            .map_err(|error| arrow_error("failed to encode baseline primary keys", error))?;
        Ok(keys
            .iter()
            .map(|key| Some(!self.entries.contains_key(key.as_ref())))
            .collect())
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
        let survivors = self.entries.values().filter(|row| row.is_some()).count();
        let mut memory = Reservation::new(self.memory.budget());
        if self.memory.budget().is_some() {
            let chunk = survivors.min(batch_size);
            let indices_bytes = checked_bytes(survivors.checked_mul(size_of::<(usize, usize)>()))?;
            let scratch = checked_bytes(chunk.checked_mul(3 * size_of::<(usize, usize)>()))?;
            let positions = hash_table_bytes::<usize, usize>(chunk)?;
            memory.resize(checked_bytes(
                indices_bytes
                    .checked_add(scratch)
                    .and_then(|n| n.checked_add(positions)),
            )?)?;
        }
        let mut indices = Vec::with_capacity(survivors);
        indices.extend(
            self.entries
                .values()
                .filter_map(|value| value.map(|row| (row.batch_index, row.row_index))),
        );
        indices.sort_unstable();
        drop(self.entries);
        drop(self.key_converter);
        drop(self.primary_key_positions);
        let mut retained_memory = self.memory;
        retained_memory.resize(hash_table_bytes::<usize, RetainedBatch>(
            self.batch_capacity,
        )?)?;
        // Tuple field order keeps the reservation alive until its allocations
        // are dropped, including when the consumer abandons the iterator.
        let mut state = (
            indices.into_iter(),
            self.changelog_batches,
            Some((memory, retained_memory)),
        );
        Ok(std::iter::from_fn(move || {
            let (indices, retained, memory) = &mut state;
            if indices.len() == 0 {
                *indices = Vec::new().into_iter();
                *retained = HashMap::new();
                memory.take();
                return None;
            }
            let mut selected = Vec::with_capacity(indices.len().min(batch_size));
            selected.extend(indices.by_ref().take(batch_size));
            // Include only batches referenced by this output chunk, rather
            // than scanning all retained batches on every poll.
            let batch_count = selected.len().min(retained.len());
            let mut batches = Vec::with_capacity(batch_count);
            let mut positions = HashMap::with_capacity(batch_count);
            let local_indices: Vec<_> = selected
                .iter()
                .map(|(batch_id, row)| {
                    let position = *positions.entry(*batch_id).or_insert_with(|| {
                        let position = batches.len();
                        batches.push(&retained[batch_id].batch);
                        position
                    });
                    (position, retained[batch_id].position(*row))
                })
                .collect();
            let result = interleave_record_batch(&batches, &local_indices)
                .map_err(|error| arrow_error("failed to emit surviving changelog rows", error));
            drop(batches);
            drop(positions);
            drop(local_indices);
            for (batch_id, row) in selected {
                release_row(retained, batch_id, row);
            }
            if result.is_err() || indices.len() == 0 {
                *indices = Vec::new().into_iter();
                *retained = HashMap::new();
                memory.take();
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

fn release_row(
    batches: &mut HashMap<usize, RetainedBatch>,
    batch_id: usize,
    original_row: usize,
) -> bool {
    let retained = batches
        .get_mut(&batch_id)
        .expect("surviving row has a retained batch");
    retained.live.set_bit(original_row, false);
    retained.live_rows -= 1;
    if retained.live_rows == 0 {
        batches.remove(&batch_id);
        return false;
    }
    // Trigger once per halving, so repeated updates do not repeatedly copy a batch.
    retained.live_rows == retained.compact_at
}

/// Count each backing allocation once within a batch, including slices, null
/// bitmaps and nested/dictionary children sharing the same IPC body.
fn retained_batch_bytes(batch: &RecordBatch) -> Result<usize> {
    fn duplicate_bytes(data: &ArrayData, allocations: &mut HashMap<usize, usize>) -> Result<usize> {
        let mut duplicate = 0usize;
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(|n| n.buffer()))
        {
            let bytes = allocations
                .entry(buffer.data_ptr().as_ptr() as usize)
                .or_default();
            duplicate = checked_bytes(duplicate.checked_add((*bytes).min(buffer.capacity())))?;
            *bytes = (*bytes).max(buffer.capacity());
        }
        for child in data.child_data() {
            duplicate = checked_bytes(duplicate.checked_add(duplicate_bytes(child, allocations)?))?;
        }
        Ok(duplicate)
    }
    let mut allocations = HashMap::new();
    let mut bytes = 0usize;
    for column in batch.columns() {
        let duplicate = duplicate_bytes(&column.to_data(), &mut allocations)?;
        let unique = checked_bytes(column.get_array_memory_size().checked_sub(duplicate))?;
        bytes = checked_bytes(bytes.checked_add(unique))?;
    }
    Ok(bytes)
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
    use crate::client::LimitedReadMemoryBudget;
    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
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
    fn native_baseline_needs_only_keys() {
        let mut view = DeduplicateCurrentView::try_new(schema(), vec![2, 0]).unwrap();
        let baseline = batch(vec![1, 2, 3], vec!["a", "b", "c"], vec![1, 2, 3]);
        let unchanged = view.reconcile_baseline_batch(&baseline).unwrap().unwrap();
        assert!(Arc::ptr_eq(unchanged.column(1), baseline.column(1)));
        assert_eq!(
            view.baseline_keep_mask(&[baseline.column(2).clone(), baseline.column(0).clone()])
                .unwrap(),
            BooleanArray::from(vec![true; 3])
        );
        view.fold_changelog_batch(
            batch(vec![1, 2], vec!["new", ""], vec![1, 2]),
            &[ChangeType::UpdateAfter, ChangeType::Delete],
        )
        .unwrap();
        let keep = view
            .baseline_keep_mask(&[baseline.column(2).clone(), baseline.column(0).clone()])
            .unwrap();
        assert_eq!(keep, BooleanArray::from(vec![false, false, true]));
        assert_eq!(
            batch(vec![3], vec!["c"], vec![3]),
            view.reconcile_baseline_batch(&baseline).unwrap().unwrap()
        );
        assert!(view.baseline_keep_mask(&[]).is_err());
        assert!(
            view.baseline_keep_mask(&[baseline.column(1).clone(), baseline.column(0).clone()])
                .is_err()
        );
        assert!(
            view.baseline_keep_mask(&[baseline.column(2).slice(0, 1), baseline.column(0).clone()])
                .is_err()
        );
        assert_eq!(
            view.finish().unwrap().unwrap(),
            batch(vec![1], vec!["new"], vec![1])
        );
    }

    #[test]
    fn shared_ipc_body_fits_the_retained_budget_once() {
        let input = batch(vec![1], vec![&"x".repeat(100_000)], vec![7]);
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, &schema()).unwrap();
        writer.write(&input).unwrap();
        writer.finish().unwrap();
        let decoded = StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        // IPC columns share one body. A per-column charge would reject this batch.
        let budget = Arc::new(LimitedReadMemoryBudget::new(150_000));
        let mut view =
            DeduplicateCurrentView::try_new_with_memory_budget(schema(), vec![0], budget.clone())
                .unwrap();
        view.fold_changelog_batch(decoded, &[ChangeType::Insert])
            .unwrap();
        assert!(budget.used_bytes() >= 100_000);
        assert_eq!(view.finish().unwrap().unwrap(), input);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn compaction_releases_obsolete_payloads_and_remaps_survivors() {
        for allow_compaction in [true, false] {
            let budget = Arc::new(LimitedReadMemoryBudget::new(1 << 20));
            let mut view = DeduplicateCurrentView::try_new_with_memory_budget(
                schema(),
                vec![0],
                budget.clone(),
            )
            .unwrap();
            let large = "x".repeat(20_000);
            view.fold_changelog_batch(
                batch(
                    (1..=8).collect(),
                    vec![
                        &large, &large, &large, &large, &large, &large, "seven", "eight",
                    ],
                    (1..=8).collect(),
                ),
                &[ChangeType::Insert; 8],
            )
            .unwrap();
            let before = budget.used_bytes();
            let held_elsewhere = if allow_compaction {
                0
            } else {
                (1 << 20) - before
            };
            budget.try_reserve(held_elsewhere).unwrap();
            view.fold_changelog_batch(
                batch((1..=6).collect(), vec![""; 6], vec![0; 6]),
                &[ChangeType::Delete; 6],
            )
            .unwrap();
            if allow_compaction {
                assert!(budget.used_bytes() < before - 100_000);
            } else {
                assert_eq!(budget.used_bytes(), 1 << 20);
            }
            budget.release(held_elsewhere);
            // A later update must follow the compacted row indexes.
            view.fold_changelog_batch(
                batch(vec![8], vec!["new"], vec![9]),
                &[ChangeType::UpdateAfter],
            )
            .unwrap();
            assert_eq!(
                reconcile(view, vec![]),
                vec![(7, "seven".into(), 7), (8, "new".into(), 9)]
            );
            assert_eq!(budget.used_bytes(), 0);
        }
    }

    #[test]
    fn tombstones_do_not_keep_growth_headroom_reserved() {
        let budget = Arc::new(LimitedReadMemoryBudget::new(192_000));
        let mut view =
            DeduplicateCurrentView::try_new_with_memory_budget(schema(), vec![0], budget.clone())
                .unwrap();
        for start in [0, 512] {
            view.fold_changelog_batch(
                batch((start..start + 512).collect(), vec![""; 512], vec![0; 512]),
                &[ChangeType::Delete; 512],
            )
            .unwrap();
        }
        // Allow simultaneous old/new tables while growing, then return headroom
        // to other readers. Updating existing keys requires no new table.
        assert!(budget.used_bytes() < 120_000);
        let spare = 192_000 - budget.used_bytes();
        budget.try_reserve(spare).unwrap();
        view.fold_changelog_batch(batch(vec![1], vec![""], vec![0]), &[ChangeType::Delete])
            .unwrap();
        budget.release(spare);
        drop(view);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn budget_charges_sliced_buffers_and_releases_superseded_batches() {
        let budget = Arc::new(LimitedReadMemoryBudget::new(1 << 20));
        let mut view =
            DeduplicateCurrentView::try_new_with_memory_budget(schema(), vec![0], budget.clone())
                .unwrap();
        let large = batch(vec![1, 2], vec!["small", &"x".repeat(100_000)], vec![1, 2]);
        let slice = large.slice(0, 1);
        let backing_bytes = slice.get_array_memory_size();
        view.fold_changelog_batch(slice, &[ChangeType::Insert])
            .unwrap();
        assert!(budget.used_bytes() >= backing_bytes);
        let before = budget.used_bytes();
        view.fold_changelog_batch(
            batch(vec![1], vec!["new"], vec![3]),
            &[ChangeType::UpdateAfter],
        )
        .unwrap();
        assert!(budget.used_bytes() < before - 90_000);
        let output = view.finish().unwrap().unwrap();
        assert_eq!(rows_of(&output), vec![(1, "new".into(), 3)]);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn shared_budget_rejects_growth_without_overwriting_existing_rows() {
        let budget = Arc::new(LimitedReadMemoryBudget::new(16_384));
        let mut first =
            DeduplicateCurrentView::try_new_with_memory_budget(schema(), vec![0], budget.clone())
                .unwrap();
        first
            .fold_changelog_batch(batch(vec![1], vec!["old"], vec![1]), &[ChangeType::Insert])
            .unwrap();
        let before = budget.used_bytes();
        let mut second =
            DeduplicateCurrentView::try_new_with_memory_budget(schema(), vec![0], budget.clone())
                .unwrap();
        assert!(budget.used_bytes() > before);
        assert!(matches!(
            second.fold_changelog_batch(
                batch(vec![2], vec![&"x".repeat(32_768)], vec![2]),
                &[ChangeType::Insert]
            ),
            Err(Error::BufferExhausted { .. })
        ));
        drop(second);
        assert_eq!(budget.used_bytes(), before);
        // Tombstones also consume key/table memory even without retained payloads.
        let ids: Vec<_> = (0..1000).collect();
        assert!(matches!(
            first.fold_changelog_batch(
                batch(ids, vec![""; 1000], vec![0; 1000]),
                &vec![ChangeType::Delete; 1000]
            ),
            Err(Error::BufferExhausted { .. })
        ));
        assert_eq!(
            rows_of(&first.finish().unwrap().unwrap()),
            vec![(1, "old".into(), 1)]
        );
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn survivor_iterator_keeps_reservations_until_consumed_or_dropped() {
        let budget = Arc::new(LimitedReadMemoryBudget::new(1 << 20));
        let mut view =
            DeduplicateCurrentView::try_new_with_memory_budget(schema(), vec![0], budget.clone())
                .unwrap();
        view.fold_changelog_batch(
            batch(vec![1, 2], vec!["a", "b"], vec![1, 2]),
            &[ChangeType::Insert; 2],
        )
        .unwrap();
        let mut survivors = view.finish_batches(1).unwrap();
        assert_eq!(survivors.next().unwrap().unwrap().num_rows(), 1);
        assert!(budget.used_bytes() > 0);
        drop(survivors);
        assert_eq!(budget.used_bytes(), 0);

        let mut view =
            DeduplicateCurrentView::try_new_with_memory_budget(schema(), vec![0], budget.clone())
                .unwrap();
        view.fold_changelog_batch(batch(vec![1], vec!["a"], vec![1]), &[ChangeType::Insert])
            .unwrap();
        let held_elsewhere = (1 << 20) - budget.used_bytes();
        budget.try_reserve(held_elsewhere).unwrap();
        assert!(matches!(
            view.finish_batches(1),
            Err(Error::BufferExhausted { .. })
        ));
        assert_eq!(budget.used_bytes(), held_elsewhere);
        budget.release(held_elsewhere);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn tail_changes_reconcile_with_baseline() {
        let mut view = current_view();
        view.fold_changelog_batch(
            batch(vec![2, 3], vec!["old", "deleted"], vec![20, 30]),
            &[ChangeType::UpdateBefore, ChangeType::Delete],
        )
        .unwrap();
        view.fold_changelog_batch(
            batch(vec![2, 4], vec!["updated", "inserted"], vec![22, 40]),
            &[ChangeType::UpdateAfter, ChangeType::Insert],
        )
        .unwrap();
        assert_eq!(
            reconcile(
                view,
                vec![batch(
                    vec![1, 2, 3],
                    vec!["one", "old", "deleted"],
                    vec![10, 20, 30]
                )]
            ),
            vec![
                (1, "one".into(), 10),
                (2, "updated".into(), 22),
                (4, "inserted".into(), 40)
            ],
        );
    }

    #[test]
    fn deleting_last_survivor_releases_payload_but_keeps_tombstone() {
        let mut view = current_view();
        let input = batch(vec![1], vec!["old"], vec![1]);
        let payload = Arc::downgrade(input.column(1));
        view.fold_changelog_batch(input, &[ChangeType::Insert])
            .unwrap();
        assert!(payload.upgrade().is_some());
        view.fold_changelog_batch(batch(vec![1], vec![""], vec![0]), &[ChangeType::Delete])
            .unwrap();
        assert!(payload.upgrade().is_none());
        assert!(
            view.reconcile_baseline_batch(&batch(vec![1], vec!["baseline"], vec![1]))
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
                vec![1, 1, 1, 1, 2, 2],
                vec!["a", "a", "b", "latest", "c", "c"],
                vec![1, 1, 2, 4, 3, 3],
            ),
            &[
                ChangeType::Insert,
                ChangeType::Delete,
                ChangeType::Insert,
                ChangeType::UpdateAfter,
                ChangeType::Insert,
                ChangeType::Delete,
            ],
        )
        .unwrap();
        assert_eq!(
            rows_of(&view.finish().unwrap().unwrap()),
            vec![(1, "latest".to_string(), 4)]
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
        assert!(matches!(
            view.fold_changelog_batch(
                batch(vec![1, 2], vec!["bad", "bad"], vec![2, 2]),
                &[ChangeType::Delete, ChangeType::AppendOnly],
            ),
            Err(Error::IllegalArgument { .. })
        ));
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
        let narrow = batch(vec![1], vec!["one"], vec![10]).project(&[0]).unwrap();

        assert!(matches!(
            current_view.reconcile_baseline_batch(&narrow),
            Err(Error::IllegalArgument { .. })
        ));
    }

    #[test]
    fn inputs_that_differ_only_in_nullability_are_normalized() {
        let current_view = current_view();
        let expected = batch(vec![1], vec!["one"], vec![10]);
        let mut fields = schema().fields().to_vec();
        fields[0] = Arc::new(fields[0].as_ref().clone().with_nullable(true));
        let relaxed =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), expected.columns().to_vec())
                .unwrap();

        let kept = current_view
            .reconcile_baseline_batch(&relaxed)
            .unwrap()
            .unwrap();

        assert_eq!(kept, expected);
    }

    #[test]
    fn skipped_compaction_retries_at_the_next_halving() {
        let budget = Arc::new(LimitedReadMemoryBudget::new(1 << 20));
        let mut view =
            DeduplicateCurrentView::try_new_with_memory_budget(schema(), vec![0], budget.clone())
                .unwrap();
        let large = "x".repeat(10_000);
        view.fold_changelog_batch(
            batch((1..=8).collect(), vec![&large; 8], (1..=8).collect()),
            &[ChangeType::Insert; 8],
        )
        .unwrap();
        let used = budget.used_bytes();
        let held_elsewhere = (1 << 20) - used;
        budget.try_reserve(held_elsewhere).unwrap();
        view.fold_changelog_batch(
            batch((1..=4).collect(), vec![""; 4], vec![0; 4]),
            &[ChangeType::Delete; 4],
        )
        .unwrap();
        assert_eq!(budget.used_bytes(), 1 << 20);

        budget.release(held_elsewhere);
        view.fold_changelog_batch(
            batch((5..=6).collect(), vec![""; 2], vec![0; 2]),
            &[ChangeType::Delete; 2],
        )
        .unwrap();
        assert!(budget.used_bytes() < used / 2);
        assert_eq!(
            reconcile(view, vec![]),
            vec![(7, large.clone(), 7), (8, large, 8)]
        );
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn random_changelogs_match_a_model() {
        let mut rng = StdRng::seed_from_u64(42);
        for case in 0..500 {
            let keys = rng.random_range(1..=40);
            let mut view = if rng.random_bool(0.5) {
                current_view()
            } else {
                DeduplicateCurrentView::try_new_with_memory_budget(
                    schema(),
                    vec![0],
                    Arc::new(LimitedReadMemoryBudget::new(usize::MAX)),
                )
                .unwrap()
            };
            let mut model = HashMap::new();
            let mut sequence = 0;
            for _ in 0..rng.random_range(1..=30) {
                let mut rows = Vec::new();
                let mut change_types = Vec::new();
                for _ in 0..rng.random_range(1..=25) {
                    sequence += 1;
                    let row = (rng.random_range(0..keys), format!("v{sequence}"), sequence);
                    let change_type = ChangeType::from_byte_value(rng.random_range(1..=4)).unwrap();
                    let live = matches!(change_type, ChangeType::Insert | ChangeType::UpdateAfter);
                    model.insert(row.0, live.then(|| row.clone()));
                    rows.push(row);
                    change_types.push(change_type);
                }
                view.fold_changelog_batch(batch_of(&rows), &change_types)
                    .unwrap();
            }

            let lake: Vec<_> = (0..keys).map(|id| (id, "lake".to_string(), -1)).collect();
            let mut actual = Vec::new();
            if let Some(kept) = view.reconcile_baseline_batch(&batch_of(&lake)).unwrap() {
                actual.extend(rows_of(&kept));
            }
            for output in view.finish_batches(rng.random_range(1..=5)).unwrap() {
                actual.extend(rows_of(&output.unwrap()));
            }
            actual.sort();
            let expected: Vec<_> = lake
                .into_iter()
                .filter_map(|row| model.remove(&row.0).unwrap_or(Some(row)))
                .collect();
            assert_eq!(actual, expected, "case {case}");
        }
    }

    fn batch_of(rows: &[(i32, String, i64)]) -> RecordBatch {
        batch(
            rows.iter().map(|row| row.0).collect(),
            rows.iter().map(|row| row.1.as_str()).collect(),
            rows.iter().map(|row| row.2).collect(),
        )
    }
}
