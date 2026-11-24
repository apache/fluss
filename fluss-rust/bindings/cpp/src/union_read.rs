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

use super::*;
use fluss_lake::{
    FlussLakeError, FlussLakeTable, UnionReadCheckpoint, UnionReadEvent, UnionReadStream,
};
use futures::{StreamExt, TryStreamExt};
use prost::Message;

pub struct UnionReader {
    stream: UnionReadStream,
}

fn lake_error(error: FlussLakeError) -> ffi::FfiResult {
    match error {
        FlussLakeError::Client { ref source, .. } => {
            err_result(err_from_core_error(source).error_code, error.to_string())
        }
        other => client_err(other.to_string()),
    }
}

impl Table {
    pub(super) fn capture_union_context(&self, bytes: &mut Vec<u8>) -> ffi::FfiResult {
        let table = FlussLakeTable::new(self.connection.clone(), &self.table_path);
        match RUNTIME
            .block_on(table.capture_read_context())
            .and_then(|ctx| ctx.to_protobuf())
        {
            Ok(pb) if pb.table_id != Some(self.table_info.table_id) => {
                client_err("Table was replaced before capturing the UnionRead context".into())
            }
            Ok(pb) => {
                *bytes = pb.encode_to_vec();
                ok_result()
            }
            Err(e) => lake_error(e),
        }
    }

    pub(super) fn open_union(
        &self,
        bytes: &[u8],
        options: ffi::FfiUnionOptions,
    ) -> ffi::FfiPtrResult {
        match RUNTIME.block_on(self.open_union_inner(bytes, options)) {
            Ok(reader) => ok_ptr(Box::into_raw(Box::new(reader)) as usize),
            Err(result) => ffi::FfiPtrResult { result, ptr: 0 },
        }
    }

    async fn open_union_inner(
        &self,
        bytes: &[u8],
        opts: ffi::FfiUnionOptions,
    ) -> Result<UnionReader, ffi::FfiResult> {
        let table = FlussLakeTable::new(self.connection.clone(), &self.table_path)
            .with_catalog_properties(
                opts.properties
                    .into_iter()
                    .map(|p| (p.key, p.value))
                    .collect(),
            );
        let pb = fcore::protocol::PbUnionReadContext::decode(bytes)
            .map_err(|e| client_err(format!("Invalid UnionRead context: {e}")))?;
        if pb.table_id != Some(self.table_info.table_id) {
            return Err(client_err(
                "UnionRead context does not match the open table".into(),
            ));
        }
        let ctx = table.import_read_context(&pb).await.map_err(lake_error)?;
        let schema = ctx.table_schema();
        let row = schema.row_type();
        let predicate = predicate_from_ffi_nodes(&opts.predicate, row).map_err(client_err)?;
        let budget = opts
            .has_budget
            .then(|| Arc::new(fcore::client::LimitedReadMemoryBudget::new(opts.budget)));
        let mut scan = table.new_scan();
        if !opts.names.is_empty() {
            scan = scan.with_projection_by_names(opts.names);
        } else if opts.has_projection {
            scan = scan.with_projection(opts.projection);
        }
        if let Some(filter) = predicate {
            scan = scan.with_filter(filter);
        }
        let stream = match opts.mode {
            ffi::FfiUnionMode::Bounded => {
                let plan = scan.plan_with_context(&ctx).await.map_err(lake_error)?;
                let mut reader = scan.new_reader();
                if let Some(budget) = budget {
                    reader = reader.with_memory_budget(budget);
                }
                reader
                    .read_splits(plan.splits())
                    .map_err(lake_error)?
                    .map_ok(UnionReadEvent::Snapshot)
                    .boxed()
            }
            ffi::FfiUnionMode::Streaming | ffi::FfiUnionMode::Resume => {
                let mut scan = scan.new_stream();
                if let Some(budget) = budget {
                    scan = scan.with_memory_budget(budget);
                }
                if opts.mode == ffi::FfiUnionMode::Resume {
                    let mut offsets = HashMap::new();
                    for o in opts.offsets {
                        let bucket = reader_bucket_to_core(
                            o.table_id,
                            o.has_partition_id,
                            o.partition_id,
                            o.bucket_id,
                        );
                        if offsets.insert(bucket, o.offset).is_some() {
                            return Err(client_err("Duplicate checkpoint bucket".into()));
                        }
                    }
                    scan.resume(
                        &ctx,
                        UnionReadCheckpoint {
                            view_digest: opts.view_digest,
                            next_offsets: offsets,
                        },
                    )
                    .await
                } else {
                    scan.start(&ctx).await
                }
                .map_err(lake_error)?
            }
            _ => return Err(client_err("Unknown UnionRead mode".into())),
        };
        Ok(UnionReader {
            stream: stream.fuse().boxed(),
        })
    }
}

impl UnionReader {
    pub(super) fn union_next(
        &mut self,
        timeout_ms: u64,
        array: usize,
        schema: usize,
    ) -> ffi::FfiUnionEvent {
        use ffi::FfiUnionKind as Kind;
        let mut out = ffi::FfiUnionEvent {
            result: ok_result(),
            kind: Kind::Finished,
            offsets: vec![],
            view_digest: [0; 32],
            changes: vec![],
        };
        let next = RUNTIME.block_on(async {
            tokio::time::timeout(Duration::from_millis(timeout_ms), self.stream.next()).await
        });
        match next {
            Err(_) => out.kind = Kind::TimedOut,
            Ok(None) => (),
            Ok(Some(Err(e))) => out.result = lake_error(e),
            Ok(Some(Ok(UnionReadEvent::Snapshot(batch)))) => {
                out.result = UnionArrowBatch(batch).export_union_batch(array, schema);
                out.kind = Kind::Snapshot;
            }
            Ok(Some(Ok(UnionReadEvent::SnapshotFinished(cp)))) => {
                out.kind = Kind::SnapshotFinished;
                out.view_digest = cp.view_digest;
                out.offsets = cp
                    .next_offsets
                    .into_iter()
                    .map(|(b, o)| offset(&b, o))
                    .collect();
            }
            Ok(Some(Ok(UnionReadEvent::Changes {
                batches,
                next_offsets,
            }))) => {
                out.kind = Kind::Changes;
                out.offsets = next_offsets
                    .into_iter()
                    .map(|(b, o)| offset(&b, o))
                    .collect();
                out.changes = batches
                    .into_iter()
                    .map(|c| ffi::FfiUnionBatch {
                        table_id: c.bucket.table_id(),
                        partition_id: c.bucket.partition_id().unwrap_or_default(),
                        has_partition_id: c.bucket.partition_id().is_some(),
                        bucket_id: c.bucket.bucket_id(),
                        changes: c
                            .change_types
                            .unwrap_or_default()
                            .into_iter()
                            .map(|k| k.to_byte_value())
                            .collect(),
                        data: Box::new(UnionArrowBatch(c.batch)),
                    })
                    .collect();
            }
            Ok(Some(Ok(UnionReadEvent::PartitionsDropped(offsets)))) => {
                out.kind = Kind::PartitionsDropped;
                out.offsets = offsets.into_iter().map(|(b, o)| offset(&b, o)).collect();
            }
        }
        out
    }
}

fn offset(b: &fcore::metadata::TableBucket, o: i64) -> ffi::FfiReaderStopOffset {
    ffi::FfiReaderStopOffset {
        table_id: b.table_id(),
        has_partition_id: b.partition_id().is_some(),
        partition_id: b.partition_id().unwrap_or_default(),
        bucket_id: b.bucket_id(),
        offset: o,
    }
}

pub struct UnionArrowBatch(arrow::record_batch::RecordBatch);
impl UnionArrowBatch {
    pub(super) fn export_union_batch(&self, array: usize, schema: usize) -> ffi::FfiResult {
        match types::export_record_batch(&self.0) {
            Ok((a, s)) => {
                // C++ supplies fresh C Data structs and transfers their release callbacks to Arrow.
                unsafe {
                    (array as *mut FFI_ArrowArray).write(a);
                    (schema as *mut FFI_ArrowSchema).write(s);
                }
                ok_result()
            }
            Err(e) => client_err(e),
        }
    }
}
