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

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */
use crate::error::Error::IllegalArgument;
use crate::error::Result;

pub const MURMUR3_DEFAULT_SEED: u32 = 0;
pub const FLINK_MURMUR3_DEFAULT_SEED: i32 = 42;

const C1: u32 = 0xCC9E_2D51;
const C2: u32 = 0x1B87_3593;
const R1: u32 = 15;
const R2: u32 = 13;
const M: u32 = 5;
const N: u32 = 0xE654_6B64;
const CHUNK_SIZE: usize = 4;

/// Hashes the data using 32-bit Murmur3 hash with 0 as seed
///
/// # Arguments
/// * `data` - byte array containing data to be hashed
///
/// # Returns
/// Returns hash value
pub fn hash_bytes(data: &[u8]) -> u32 {
    hash_bytes_with_seed(data, MURMUR3_DEFAULT_SEED)
}

#[inline(always)]
fn hash_bytes_with_seed(data: &[u8], seed: u32) -> u32 {
    let length = data.len();
    let chunks = length / CHUNK_SIZE;
    let length_aligned = chunks * CHUNK_SIZE;

    let mut h1 = hash_full_chunks(data, seed);
    let mut k1 = 0u32;

    for (shift, &b) in data[length_aligned..].iter().enumerate() {
        k1 |= (b as u32) << (8 * shift);
    }

    h1 ^= k1.wrapping_mul(C1).rotate_left(R1).wrapping_mul(C2);

    fmix(h1, length)
}

/// Hashes the data using Fluss'/Flink's variant of 32-bit Murmur hash with 42 as seed and tail bytes mixed into hash byte-by-byte
/// Maximum data array size supported is 2GB
///
/// # Arguments
/// * `data` - byte array containing data to be hashed
///
/// # Returns
/// * result of hashing, `Ok(hash_value)`
///
/// # Error
/// Returns `Err(IllegalArgument)` if byte array is larger than 2GB
pub fn fluss_hash_bytes(data: &[u8]) -> Result<i32> {
    fluss_hash_bytes_with_seed(data, FLINK_MURMUR3_DEFAULT_SEED)
}
#[inline(always)]
fn fluss_hash_bytes_with_seed(data: &[u8], seed: i32) -> Result<i32> {
    let length = data.len();

    if length >= i32::MAX as usize {
        return Err(IllegalArgument {
            message: "data array size {length} is bigger than supported".to_string(),
        });
    }

    let chunks = length / CHUNK_SIZE;
    let length_aligned = chunks * CHUNK_SIZE;

    let mut h1 = hash_full_chunks(data, seed as u32);

    for byte in data.iter().take(length).skip(length_aligned) {
        let k1 = mix_k1(*byte as u32);
        h1 = mix_h1(h1, k1);
    }

    Ok(fmix(h1, length) as i32)
}

#[inline(always)]
fn hash_full_chunks(data: &[u8], seed: u32) -> u32 {
    data.chunks_exact(CHUNK_SIZE).fold(seed, |h1, chunk| {
        let block = u32::from_le_bytes(chunk.try_into().unwrap());
        let k1 = mix_k1(block);
        mix_h1(h1, k1)
    })
}

#[inline(always)]
fn mix_k1(k1: u32) -> u32 {
    k1.wrapping_mul(C1).rotate_left(R1).wrapping_mul(C2)
}

#[inline(always)]
fn mix_h1(h1: u32, k1: u32) -> u32 {
    (h1 ^ k1).rotate_left(R2).wrapping_mul(M).wrapping_add(N)
}

// Finalization mix - force all bits of a hash block to avalanche
#[inline(always)]
fn fmix(mut h1: u32, length: usize) -> u32 {
    h1 ^= length as u32;
    bit_mix(h1)
}

/// Hashes an i32 using Fluss'/Flink's variant of Murmur
///
/// # Arguments
/// * `input` - i32 value to be hashed
///
/// # Returns
/// Returns hash value
pub fn fluss_hash_i32(input: i32) -> i32 {
    let mut input = input as u32;
    input = input.wrapping_mul(C1);
    input = input.rotate_left(R1);
    input = input.wrapping_mul(C2);
    input = input.rotate_left(R2);

    input = input.wrapping_mul(M).wrapping_add(N);
    input ^= CHUNK_SIZE as u32;
    let output = bit_mix(input) as i32;

    if output >= 0 {
        output
    } else if output != i32::MIN {
        -output
    } else {
        0
    }
}

const BIT_MIX_A: u32 = 0x85EB_CA6B;
const BIT_MIX_B: u32 = 0xC2B2_AE35;

#[inline(always)]
fn bit_mix(mut input: u32) -> u32 {
    input = input ^ (input >> 16);
    input = input.wrapping_mul(BIT_MIX_A);
    input = input ^ (input >> 13);
    input = input.wrapping_mul(BIT_MIX_B);
    input = input ^ (input >> 16);
    input
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_murmur3() {
        //
        let empty_data_hash = hash_bytes(&[]);
        assert_eq!(empty_data_hash, 0);

        let empty_data_hash = hash_bytes_with_seed(&[], 1);
        assert_eq!(0x514E_28B7, empty_data_hash);

        let empty_data_hash = hash_bytes_with_seed(&[], 0xFFFF_FFFF);
        assert_eq!(0x81F1_6F39, empty_data_hash);

        let hash = hash_bytes("The quick brown fox jumps over the lazy dog".as_bytes());
        assert_eq!(0x2E4F_F723, hash);

        let hash = hash_bytes_with_seed(
            "The quick brown fox jumps over the lazy dog".as_bytes(),
            0x9747_B28C,
        );
        assert_eq!(0x2FA8_26CD, hash);
    }

    #[test]
    fn test_flink_murmur() {
        let empty_data_hash = fluss_hash_bytes_with_seed(&[], 0).expect("Failed to hash");
        assert_eq!(empty_data_hash, 0);

        let empty_data_hash = fluss_hash_bytes(&[]).expect("Failed to hash");
        assert_eq!(0x087F_CD5C, empty_data_hash);

        let empty_data_hash =
            fluss_hash_bytes_with_seed(&[], 0xFFFF_FFFFu32 as i32).expect("Failed to hash");
        assert_eq!(0x81F1_6F39u32 as i32, empty_data_hash);

        let hash =
            fluss_hash_bytes_with_seed("The quick brown fox jumps over the lazy dog".as_bytes(), 0)
                .expect("Failed to hash");
        assert_eq!(0x5FD2_0A20, hash);

        let hash = fluss_hash_bytes("The quick brown fox jumps over the lazy dog".as_bytes())
            .expect("Failed to hash");
        assert_eq!(0x1BC6_F880, hash);

        let hash = fluss_hash_i32(0);
        assert_eq!(0x2362_F9DE, hash);

        let hash = fluss_hash_i32(42);
        assert_eq!(0x43A4_6E1D, hash);

        let hash = fluss_hash_i32(-77);
        assert_eq!(0x2EEB_27DE, hash);
    }
}
