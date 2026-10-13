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

//! Encoding rules shared by the writers of Fluss's 8-byte-slot binary formats.
//!
//! Java centralises these in `AbstractBinaryWriter` and leaves each subclass its
//! own field addressing, which is the split kept here: this module never learns
//! where a field lives, only how a value is packed once its slot is known.
//!
//! Fluss's row and array writers share one format and must move together, but
//! the Paimon writer also uses this module even though Paimon versions its
//! format independently. The two agree today because both descend from Flink's
//! `BinaryRowData`. If either side ever diverges, split the affected helper in
//! two rather than editing it in place, so that a change made for one format
//! cannot silently rewrite the other's output.

/// Longest payload that still fits inside an 8-byte slot alongside its length
/// marker.
pub(crate) const MAX_FIX_PART_DATA_SIZE: usize = 7;

/// Rounds up to a whole 8-byte word, how the variable-length part stays aligned.
pub(crate) fn round_to_nearest_word(num_bytes: usize) -> usize {
    let remainder = num_bytes & 0x07;
    if remainder == 0 {
        num_bytes
    } else {
        num_bytes + (8 - remainder)
    }
}

/// The slot value for a payload living in the variable-length part.
///
/// Timestamps pass the nano-of-millisecond as `size`, since their payload is
/// always 8 bytes and the low half would otherwise go to waste.
pub(crate) fn pack_offset_and_size(offset: usize, size: u64) -> i64 {
    ((offset as i64) << 32) | (size as i64)
}

/// The slot value for a payload of at most [`MAX_FIX_PART_DATA_SIZE`] bytes,
/// inlined with `len | 0x80` in the slot's high byte.
pub(crate) fn pack_inline_bytes(bytes: &[u8]) -> i64 {
    debug_assert!(bytes.len() <= MAX_FIX_PART_DATA_SIZE);
    let first_byte = (bytes.len() as u64) | 0x80;
    let mut seven_bytes = 0_u64;
    for (i, b) in bytes.iter().enumerate() {
        seven_bytes |= (*b as u64) << (i * 8);
    }
    ((first_byte << 56) | seven_bytes) as i64
}

/// Bytes Java reserves for a non-compact decimal, whatever its unscaled length.
const NON_COMPACT_DECIMAL_LEN: usize = 16;

/// Appends a non-compact decimal's unscaled bytes into a fixed 16-byte slot,
/// matching Java's `AbstractBinaryWriter.writeDecimal`.
///
/// The reserved length does not depend on the payload, so the stride stays the
/// same whether the unscaled value needs 3 bytes or 16.
pub(crate) fn append_non_compact_decimal(
    buffer: &mut Vec<u8>,
    cursor: &mut usize,
    unscaled: &[u8],
) -> i64 {
    debug_assert!(unscaled.len() <= NON_COMPACT_DECIMAL_LEN);
    let at = *cursor;
    let end = at + NON_COMPACT_DECIMAL_LEN;
    ensure_len(buffer, end);
    buffer[at..end].fill(0);
    buffer[at..at + unscaled.len()].copy_from_slice(unscaled);
    *cursor = end;
    pack_offset_and_size(at, unscaled.len() as u64)
}

/// Bytes a non-compact timestamp occupies in the variable-length part.
const NON_COMPACT_TIMESTAMP_LEN: usize = 8;

/// Appends the millisecond part of a non-compact timestamp and returns the slot
/// value, which carries `nano_of_millisecond` where a length would normally go.
///
/// The payload is always 8 bytes, so the low half of the slot is free for the
/// sub-millisecond part.
pub(crate) fn append_non_compact_timestamp(
    buffer: &mut Vec<u8>,
    cursor: &mut usize,
    millis: i64,
    nano_of_millisecond: i32,
) -> i64 {
    let at = *cursor;
    ensure_len(buffer, at + NON_COMPACT_TIMESTAMP_LEN);
    buffer[at..at + NON_COMPACT_TIMESTAMP_LEN].copy_from_slice(&millis.to_le_bytes());
    *cursor = at + NON_COMPACT_TIMESTAMP_LEN;
    pack_offset_and_size(at, nano_of_millisecond as u64)
}

/// Returns the slot value for `bytes`, inlining them when they fit and spilling
/// them to the variable-length part otherwise.
pub(crate) fn pack_or_append_bytes(buffer: &mut Vec<u8>, cursor: &mut usize, bytes: &[u8]) -> i64 {
    if bytes.len() <= MAX_FIX_PART_DATA_SIZE {
        pack_inline_bytes(bytes)
    } else {
        append_var_len(buffer, cursor, bytes)
    }
}

/// Extends `buffer` so that `len` bytes are addressable, leaving what is already
/// there untouched.
///
/// `Vec` grows its capacity geometrically underneath, so sizing to exactly what
/// is needed still costs amortised constant time per byte.
pub(crate) fn ensure_len(buffer: &mut Vec<u8>, len: usize) {
    if buffer.len() < len {
        buffer.resize(len, 0);
    }
}

/// Appends `bytes` word-aligned at `cursor` and returns the slot value pointing
/// at them, advancing `cursor` past the padding.
pub(crate) fn append_var_len(buffer: &mut Vec<u8>, cursor: &mut usize, bytes: &[u8]) -> i64 {
    let len = bytes.len();
    let rounded = round_to_nearest_word(len);
    let at = *cursor;
    ensure_len(buffer, at + rounded);

    // Zero the padding explicitly rather than trusting how the buffer grew.
    buffer[at + len..at + rounded].fill(0);
    buffer[at..at + len].copy_from_slice(bytes);

    *cursor = at + rounded;
    pack_offset_and_size(at, len as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_up_to_whole_words() {
        assert_eq!(round_to_nearest_word(0), 0);
        assert_eq!(round_to_nearest_word(1), 8);
        assert_eq!(round_to_nearest_word(8), 8);
        assert_eq!(round_to_nearest_word(9), 16);
    }

    #[test]
    fn packs_an_offset_and_size_into_the_two_halves() {
        let packed = pack_offset_and_size(32, 11);
        assert_eq!((packed >> 32) as usize, 32);
        assert_eq!((packed & 0xFFFF_FFFF) as usize, 11);
    }

    #[test]
    fn inlines_bytes_with_the_length_marker_on_top() {
        let packed = pack_inline_bytes(b"abc");
        let slot = packed.to_le_bytes();
        assert_eq!(&slot[..3], b"abc");
        assert_eq!(slot[7], 0x83);
    }

    /// Java reserves the same 16 bytes whatever the payload, so a short unscaled
    /// value must still advance the cursor by 16.
    #[test]
    fn a_non_compact_decimal_always_takes_sixteen_bytes() {
        let mut buffer = vec![0xFF; 8];
        let mut cursor = 8;
        let slot = append_non_compact_decimal(&mut buffer, &mut cursor, &[1, 2, 3]);

        assert_eq!((slot >> 32) as usize, 8);
        assert_eq!((slot & 0xFFFF_FFFF) as usize, 3);
        assert_eq!(cursor, 24);
        assert_eq!(&buffer[8..11], &[1, 2, 3]);
        // The rest of the reservation is zeroed, not left as it was found.
        assert_eq!(&buffer[11..24], &[0u8; 13]);
    }

    /// The append has to zero the padding itself, since a reused buffer can
    /// arrive with anything in it.
    #[test]
    fn append_zeroes_the_padding_of_a_partial_word() {
        let mut buffer = vec![0xFF; 8];
        let mut cursor = 8;
        let slot = append_var_len(&mut buffer, &mut cursor, b"abcdefghij");

        assert_eq!((slot >> 32) as usize, 8);
        assert_eq!((slot & 0xFFFF_FFFF) as usize, 10);
        assert_eq!(&buffer[8..18], b"abcdefghij");
        assert_eq!(&buffer[18..24], &[0u8; 6]);
        assert_eq!(cursor, 24);
    }

    #[test]
    fn ensure_len_only_grows() {
        let mut buffer = vec![1, 2, 3];
        ensure_len(&mut buffer, 2);
        assert_eq!(buffer, vec![1, 2, 3]);
        ensure_len(&mut buffer, 5);
        assert_eq!(buffer, vec![1, 2, 3, 0, 0]);
    }
}
