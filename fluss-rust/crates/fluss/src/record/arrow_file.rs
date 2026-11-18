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

use crate::error::Result;
use arrow::array::layout;
use arrow::datatypes::{DataType, Schema};
use arrow::error::ArrowError::ParseError;
use arrow::ipc::root_as_message;
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;

// Fluss schemas have fixed IPC buffer layouts, including nested lists, maps and
// structs. Fall back to the existing reader for other Arrow layouts.
fn select_buffers(data_type: &DataType, selected: bool, buffers: &mut Vec<bool>) -> Option<usize> {
    use DataType::*;
    if matches!(
        data_type,
        Union(..) | Dictionary(..) | RunEndEncoded(..) | Utf8View | BinaryView
    ) {
        return None;
    }
    let layout = layout(data_type);
    buffers.extend(std::iter::repeat_n(
        selected,
        layout.buffers.len() + usize::from(layout.can_contain_null_mask),
    ));
    let mut nodes = 1;
    match data_type {
        List(field)
        | LargeList(field)
        | FixedSizeList(field, _)
        | Map(field, _)
        | ListView(field)
        | LargeListView(field) => {
            nodes += select_buffers(field.data_type(), selected, buffers)?;
        }
        Struct(fields) => {
            for field in fields {
                nodes += select_buffers(field.data_type(), selected, buffers)?;
            }
        }
        _ => {}
    }
    Some(nodes)
}

#[derive(Clone)]
pub(super) struct FileReadPlan {
    selected: Vec<bool>,
    node_count: usize,
}

impl FileReadPlan {
    pub(super) fn new(schema: &Schema, columns: Option<&[usize]>) -> Option<Self> {
        let columns = columns?;
        if (0..schema.fields().len()).all(|index| columns.contains(&index)) {
            return None;
        }
        let mut selected = Vec::new();
        let mut node_count = 0;
        for (index, field) in schema.fields().iter().enumerate() {
            node_count +=
                select_buffers(field.data_type(), columns.contains(&index), &mut selected)?;
        }
        Some(Self {
            selected,
            node_count,
        })
    }

    pub(super) fn read(
        &self,
        length: usize,
        mut read: impl FnMut(usize, &mut [u8]) -> Result<()>,
    ) -> Result<Bytes> {
        let invalid = || ParseError("Invalid projected Arrow IPC metadata or buffer range".into());
        if length < 8 {
            return Err(invalid().into());
        }
        let mut prefix = [0; 8];
        read(0, &mut prefix)?;
        if LittleEndian::read_u32(&prefix[..4]) != u32::MAX {
            return Err(invalid().into());
        }
        let metadata_size = LittleEndian::read_u32(&prefix[4..]) as usize;
        let body_start = metadata_size
            .checked_add(7)
            .map(|size| size & !7)
            .and_then(|size| size.checked_add(8))
            .ok_or_else(invalid)?;
        if metadata_size == 0 || body_start > length {
            return Err(invalid().into());
        }
        let mut data = vec![0; body_start];
        data[..8].copy_from_slice(&prefix);
        read(8, &mut data[8..8 + metadata_size])?;
        let message = root_as_message(&data[8..8 + metadata_size])
            .map_err(|error| ParseError(error.to_string()))?;
        let batch = message.header_as_record_batch().ok_or_else(invalid)?;
        let buffers = batch.buffers().ok_or_else(invalid)?;
        let nodes = batch.nodes().ok_or_else(invalid)?;
        let body_length = usize::try_from(message.bodyLength()).map_err(|_| invalid())?;
        if body_length > length - body_start
            || batch.length() < 0
            || nodes.len() != self.node_count
            || buffers.len() != self.selected.len()
            || batch
                .variadicBufferCounts()
                .is_some_and(|counts| !counts.is_empty())
        {
            return Err(invalid().into());
        }
        // The verified FlatBuffer vector is inside `data`. Rebase its buffer
        // descriptors and body length in our private copy to decode a compact body with
        // the original nodes, compression and source-column indexes.
        let body_length_slot = message
            ._tab
            .vtable()
            .get(arrow::ipc::Message::VT_BODYLENGTH) as usize;
        let body_length_offset =
            (body_length_slot != 0).then(|| 8 + message._tab.loc() + body_length_slot);
        let descriptors_offset = buffers.bytes().as_ptr() as usize - data.as_ptr() as usize;
        let mut descriptors = Vec::with_capacity(buffers.len());
        let mut compact_size = 0usize;
        for (buffer, &keep) in buffers.iter().zip(&self.selected) {
            let offset = usize::try_from(buffer.offset()).map_err(|_| invalid())?;
            let size = usize::try_from(buffer.length()).map_err(|_| invalid())?;
            if offset > body_length || size > body_length - offset {
                return Err(invalid().into());
            }
            let target = compact_size;
            if keep {
                compact_size = compact_size
                    .checked_add(size)
                    .and_then(|n| n.checked_add(7))
                    .map(|n| n & !7)
                    .filter(|&n| n <= length)
                    .ok_or_else(invalid)?;
            }
            descriptors.push((offset, if keep { size } else { 0 }, target));
        }
        data.resize(body_start + compact_size, 0);
        if let Some(offset) = body_length_offset {
            LittleEndian::write_i64(&mut data[offset..offset + 8], compact_size as i64);
        }
        for (index, (offset, size, target)) in descriptors.into_iter().enumerate() {
            let buffer = arrow::ipc::Buffer::new(target as i64, size as i64);
            let start = descriptors_offset + index * std::mem::size_of::<arrow::ipc::Buffer>();
            data[start..start + buffer.0.len()].copy_from_slice(&buffer.0);
            if size != 0 {
                read(
                    body_start + offset,
                    &mut data[body_start + target..body_start + target + size],
                )?;
            }
        }
        Ok(Bytes::from(data))
    }
}
