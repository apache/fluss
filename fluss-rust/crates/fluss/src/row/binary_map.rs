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

//! Binary map format matching Java's `BinaryMap.java` layout.
//!
//! Binary layout:
//! ```text
//! [4 bytes: keyArraySizeInBytes] + [Key BinaryArray bytes] + [Value BinaryArray bytes]
//! ```

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::DataType;
use crate::row::binary_array::{FlussArray, FlussArrayWriter};
use crate::row::datum::{Datum, read_datum_from_fluss_array};
use bytes::Bytes;
use serde::Serialize;
use std::fmt;
use std::hash::{Hash, Hasher};

/// A Fluss binary map, wire-compatible with Java's `BinaryMap`.
///
/// Stores entries as two parallel binary arrays (keys and values) within a single
/// byte buffer.
#[derive(Clone)]
pub struct FlussMap {
    data: Bytes,
    key_array: FlussArray,
    value_array: FlussArray,
    key_type: DataType,
    value_type: DataType,
}

impl fmt::Debug for FlussMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussMap")
            .field("size", &self.size())
            .field("data_len", &self.data.len())
            .finish()
    }
}

impl fmt::Display for FlussMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlussMap[size={}]", self.size())
    }
}

impl PartialEq for FlussMap {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for FlussMap {}

impl PartialOrd for FlussMap {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FlussMap {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.data.cmp(&other.data)
    }
}

impl Hash for FlussMap {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl Serialize for FlussMap {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.data)
    }
}

fn check_no_null_keys(key_array: &FlussArray) -> Result<()> {
    for i in 0..key_array.size() {
        if key_array.is_null_at(i) {
            return Err(IllegalArgument {
                message: "FlussMap keys cannot be null".to_string(),
            });
        }
    }
    Ok(())
}

impl FlussMap {
    /// Validates the raw bytes and extracts the sub-arrays.
    fn validate(
        data: &[u8],
        key_type: &DataType,
        value_type: &DataType,
    ) -> Result<(FlussArray, FlussArray)> {
        if data.len() < 4 {
            return Err(IllegalArgument {
                message: format!(
                    "FlussMap data too short: need at least 4 bytes, got {}",
                    data.len()
                ),
            });
        }
        let raw_key_size = i32::from_le_bytes(data[0..4].try_into().unwrap());
        if raw_key_size < 0 {
            return Err(IllegalArgument {
                message: format!(
                    "FlussMap key array size must be non-negative, got {}",
                    raw_key_size
                ),
            });
        }
        let key_size = raw_key_size as usize;
        if 4 + key_size > data.len() {
            return Err(IllegalArgument {
                message: format!(
                    "FlussMap key array size {} exceeds remaining payload {}",
                    key_size,
                    data.len() - 4
                ),
            });
        }

        let key_bytes = &data[4..4 + key_size];
        let value_bytes = &data[4 + key_size..];

        let key_array = FlussArray::from_bytes(key_bytes).map_err(|e| IllegalArgument {
            message: format!("Invalid key array in FlussMap: {}", e),
        })?;

        let value_array = FlussArray::from_bytes(value_bytes).map_err(|e| IllegalArgument {
            message: format!("Invalid value array in FlussMap: {}", e),
        })?;

        if key_array.size() != value_array.size() {
            return Err(IllegalArgument {
                message: format!(
                    "FlussMap key array size ({}) does not match value array size ({})",
                    key_array.size(),
                    value_array.size()
                ),
            });
        }

        // Strict trailing byte check: ensure the total reach of key and value arrays
        // plus the 4-byte header matches the provided data length exactly.
        let key_extent = key_array.extent(key_type)?;
        let value_extent = value_array.extent(value_type)?;
        let expected_len = 4 + key_extent + value_extent;
        if expected_len != data.len() {
            return Err(IllegalArgument {
                message: format!(
                    "FlussMap binary validation failed: expected {expected_len} bytes, got {}",
                    data.len()
                ),
            });
        }

        check_no_null_keys(&key_array)?;

        Ok((key_array, value_array))
    }

    /// Creates a FlussMap from a byte slice (copies data).
    pub(crate) fn from_bytes(
        data: &[u8],
        key_type: &DataType,
        value_type: &DataType,
    ) -> Result<Self> {
        let (key_array, value_array) = Self::validate(data, key_type, value_type)?;
        Ok(FlussMap {
            data: Bytes::copy_from_slice(data),
            key_array,
            value_array,
            key_type: key_type.clone(),
            value_type: value_type.clone(),
        })
    }

    /// Creates a FlussMap from owned bytes without copying.
    pub(crate) fn from_owned_bytes(
        data: Bytes,
        key_type: &DataType,
        value_type: &DataType,
    ) -> Result<Self> {
        let (key_array, value_array) = Self::validate(&data, key_type, value_type)?;
        Ok(FlussMap {
            data,
            key_array,
            value_array,
            key_type: key_type.clone(),
            value_type: value_type.clone(),
        })
    }

    /// Creates a FlussMap by combining a key array and a value array.
    ///
    /// Copies both arrays into a new contiguous buffer.
    pub fn from_arrays(
        key_array: &FlussArray,
        value_array: &FlussArray,
        key_type: &DataType,
        value_type: &DataType,
    ) -> Result<Self> {
        if key_array.size() != value_array.size() {
            return Err(IllegalArgument {
                message: format!(
                    "FlussMap key array size ({}) does not match value array size ({})",
                    key_array.size(),
                    value_array.size()
                ),
            });
        }
        check_no_null_keys(key_array)?;

        let key_bytes = key_array.as_bytes();
        let value_bytes = value_array.as_bytes();

        let mut data = Vec::with_capacity(4 + key_bytes.len() + value_bytes.len());
        // Write the key array size (4 bytes)
        // Java's BinaryMap uses memory segment methods which write in LE
        data.extend_from_slice(&(key_bytes.len() as i32).to_le_bytes());
        // Write key array bytes
        data.extend_from_slice(key_bytes);
        // Write value array bytes
        data.extend_from_slice(value_bytes);

        let data = Bytes::from(data);
        Ok(FlussMap {
            data,
            key_array: key_array.clone(),
            value_array: value_array.clone(),
            key_type: key_type.clone(),
            value_type: value_type.clone(),
        })
    }

    /// Returns the number of entries in the map.
    pub fn size(&self) -> usize {
        self.key_array.size()
    }

    /// Returns the raw bytes of this map (the complete binary representation).
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Returns the key array.
    pub fn key_array(&self) -> &FlussArray {
        &self.key_array
    }

    /// Returns the value array.
    pub fn value_array(&self) -> &FlussArray {
        &self.value_array
    }

    pub fn key_type(&self) -> &DataType {
        &self.key_type
    }

    pub fn value_type(&self) -> &DataType {
        &self.value_type
    }

    pub fn entries(&self) -> Entries<'_> {
        Entries {
            map: self,
            index: 0,
        }
    }

    /// O(n) linear scan; the binary format carries no key index.
    pub fn get<'a>(&'a self, key: &Datum<'_>) -> Result<Option<Datum<'a>>> {
        for entry in self.entries() {
            let (k, v) = entry?;
            if &k == key {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
}

pub struct Entries<'a> {
    map: &'a FlussMap,
    index: usize,
}

impl<'a> Iterator for Entries<'a> {
    type Item = Result<(Datum<'a>, Datum<'a>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.map.size() {
            return None;
        }
        let i = self.index;
        self.index += 1;
        let key = read_datum_from_fluss_array(&self.map.key_array, i, &self.map.key_type);
        let value = read_datum_from_fluss_array(&self.map.value_array, i, &self.map.value_type);
        Some(key.and_then(|k| value.map(|v| (k, v))))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.map.size() - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for Entries<'_> {}

/// Writer for building a `FlussMap` entry by entry.
pub struct FlussMapWriter {
    key_writer: FlussArrayWriter,
    value_writer: FlussArrayWriter,
    key_type: DataType,
    value_type: DataType,
    current_index: usize,
}

impl FlussMapWriter {
    /// Creates a new writer for a map with the given capacity and key/value types.
    pub fn new(capacity: usize, key_type: &DataType, value_type: &DataType) -> Self {
        Self {
            key_writer: FlussArrayWriter::new(capacity, key_type),
            value_writer: FlussArrayWriter::new(capacity, value_type),
            key_type: key_type.clone(),
            value_type: value_type.clone(),
            current_index: 0,
        }
    }

    pub fn extend<'a, I, K, V>(&mut self, entries: I) -> Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Datum<'a>>,
        V: Into<Datum<'a>>,
    {
        for (k, v) in entries {
            self.write_entry(k.into(), v.into())?;
        }
        Ok(())
    }

    /// Writes a key-value entry into the map.
    ///
    /// # Errors
    /// Returns an error if the key is null or if there's a type mismatch.
    pub fn write_entry(&mut self, key: Datum, value: Datum) -> Result<()> {
        if key.is_null() {
            return Err(IllegalArgument {
                message: "FlussMap keys cannot be null".to_string(),
            });
        }

        Self::write_datum(
            &mut self.key_writer,
            self.current_index,
            key,
            &self.key_type,
        )?;
        Self::write_datum(
            &mut self.value_writer,
            self.current_index,
            value,
            &self.value_type,
        )?;
        self.current_index += 1;
        Ok(())
    }

    /// Finalizes the writer and returns the completed `FlussMap`.
    pub fn complete(self) -> Result<FlussMap> {
        let key_array = self.key_writer.complete()?;
        let value_array = self.value_writer.complete()?;
        FlussMap::from_arrays(&key_array, &value_array, &self.key_type, &self.value_type)
    }

    fn write_datum(
        writer: &mut FlussArrayWriter,
        pos: usize,
        datum: Datum,
        dt: &DataType,
    ) -> Result<()> {
        if datum.is_null() {
            writer.set_null_at(pos);
            return Ok(());
        }

        match (dt, &datum) {
            (DataType::Boolean(_), Datum::Bool(v)) => writer.write_boolean(pos, *v),
            (DataType::TinyInt(_), Datum::Int8(v)) => writer.write_byte(pos, *v),
            (DataType::SmallInt(_), Datum::Int16(v)) => writer.write_short(pos, *v),
            (DataType::Int(_), Datum::Int32(v)) => writer.write_int(pos, *v),
            (DataType::BigInt(_), Datum::Int64(v)) => writer.write_long(pos, *v),
            (DataType::Float(_), Datum::Float32(v)) => writer.write_float(pos, v.into_inner()),
            (DataType::Double(_), Datum::Float64(v)) => writer.write_double(pos, v.into_inner()),
            (DataType::Char(_), Datum::String(v)) => writer.write_string(pos, v),
            (DataType::String(_), Datum::String(v)) => writer.write_string(pos, v),
            (DataType::Binary(_), Datum::Blob(v)) => writer.write_binary_bytes(pos, v),
            (DataType::Bytes(_), Datum::Blob(v)) => writer.write_binary_bytes(pos, v),
            (DataType::Decimal(d), Datum::Decimal(v)) => {
                writer.write_decimal(pos, v, d.precision())
            }
            (DataType::Date(_), Datum::Date(v)) => writer.write_date(pos, *v),
            (DataType::Time(_), Datum::Time(v)) => writer.write_time(pos, *v),
            (DataType::Timestamp(t), Datum::TimestampNtz(v)) => {
                writer.write_timestamp_ntz(pos, v, t.precision())
            }
            (DataType::TimestampLTz(t), Datum::TimestampLtz(v)) => {
                writer.write_timestamp_ltz(pos, v, t.precision())
            }
            (DataType::Array(_), Datum::Array(v)) => writer.write_array(pos, v),
            (DataType::Map(_), Datum::Map(v)) => writer.write_map(pos, v),
            (DataType::Row(_), Datum::Row(v)) => writer.write_row(pos, v.as_ref())?,
            _ => {
                return Err(IllegalArgument {
                    message: format!("Type mismatch: expected {:?}, got {:?}", dt, datum),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;
    use crate::row::binary_array::FlussArrayWriter;

    #[test]
    fn test_round_trip_int_to_string_map() {
        let mut writer = FlussMapWriter::new(2, &DataTypes::int(), &DataTypes::string());
        writer.write_entry(1.into(), "a".into()).unwrap();
        writer.write_entry(2.into(), "b".into()).unwrap();
        let map = writer.complete().unwrap();
        assert_eq!(map.size(), 2);

        assert_eq!(
            map.as_bytes(),
            &[
                16, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,
                0, 97, 0, 0, 0, 0, 0, 0, 129, 98, 0, 0, 0, 0, 0, 0, 129
            ]
        );

        let bytes = map.as_bytes();
        let decoded = FlussMap::from_bytes(bytes, &DataTypes::int(), &DataTypes::string()).unwrap();

        assert_eq!(decoded.size(), 2);
        let decoded_keys = decoded.key_array();
        let decoded_values = decoded.value_array();

        assert_eq!(decoded_keys.get_int(0).unwrap(), 1);
        assert_eq!(decoded_keys.get_int(1).unwrap(), 2);
        assert_eq!(decoded_values.get_string(0).unwrap(), "a");
        assert_eq!(decoded_values.get_string(1).unwrap(), "b");
    }

    #[test]
    fn test_empty_map() {
        let writer = FlussMapWriter::new(0, &DataTypes::int(), &DataTypes::string());
        let map = writer.complete().unwrap();
        assert_eq!(map.size(), 0);

        let decoded =
            FlussMap::from_bytes(map.as_bytes(), &DataTypes::int(), &DataTypes::string()).unwrap();
        assert_eq!(decoded.size(), 0);
    }

    #[test]
    fn test_map_with_null_values() {
        let key_type = DataTypes::string();
        let value_type = DataTypes::int();
        let mut writer = FlussMapWriter::new(3, &key_type, &value_type);
        writer.write_entry("k1".into(), 10.into()).unwrap();
        writer.write_entry("k2".into(), Datum::Null).unwrap();
        writer.write_entry("k3".into(), 30.into()).unwrap();
        let map = writer.complete().unwrap();

        assert_eq!(
            map.as_bytes(),
            &[
                32, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 107, 49, 0, 0, 0, 0, 0, 130, 107, 50, 0, 0, 0,
                0, 0, 130, 107, 51, 0, 0, 0, 0, 0, 130, 3, 0, 0, 0, 2, 0, 0, 0, 10, 0, 0, 0, 0, 0,
                0, 0, 30, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        let decoded = FlussMap::from_bytes(map.as_bytes(), &key_type, &value_type).unwrap();

        let values = decoded.value_array();
        assert_eq!(values.size(), 3);
        assert!(!values.is_null_at(0));
        assert!(values.is_null_at(1));
        assert!(!values.is_null_at(2));
        assert_eq!(values.get_int(0).unwrap(), 10);
        assert_eq!(values.get_int(2).unwrap(), 30);
    }

    #[test]
    fn test_invalid_data() {
        // Too short
        let err =
            FlussMap::from_bytes(&[1, 2, 3], &DataTypes::int(), &DataTypes::int()).unwrap_err();
        assert!(err.to_string().contains("FlussMap data too short"));

        // Negative size
        let neg_size = (-1i32).to_le_bytes();
        let mut bad_data = vec![];
        bad_data.extend_from_slice(&neg_size);
        bad_data.extend_from_slice(&[0, 0, 0, 0]);
        let err2 =
            FlussMap::from_bytes(&bad_data, &DataTypes::int(), &DataTypes::int()).unwrap_err();
        assert!(
            err2.to_string()
                .contains("FlussMap key array size must be non-negative")
        );

        // Key array length exceeds payload
        let large_size = 100i32.to_le_bytes();
        let mut bad_data2 = vec![];
        bad_data2.extend_from_slice(&large_size);
        bad_data2.extend_from_slice(&[0, 0, 0, 0]);
        let err3 =
            FlussMap::from_bytes(&bad_data2, &DataTypes::int(), &DataTypes::int()).unwrap_err();
        assert!(
            err3.to_string()
                .contains("FlussMap key array size 100 exceeds remaining payload 4")
        );
    }

    #[test]
    fn test_mismatched_array_sizes() {
        let key_writer = FlussArrayWriter::new(1, &DataTypes::int());
        let key_array = key_writer.complete().unwrap();

        let value_writer = FlussArrayWriter::new(2, &DataTypes::string());
        let value_array = value_writer.complete().unwrap();

        let err = FlussMap::from_arrays(
            &key_array,
            &value_array,
            &DataTypes::int(),
            &DataTypes::string(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not match value array size"));
    }

    #[test]
    fn test_nested_map() {
        let map_type = DataTypes::map(DataTypes::int(), DataTypes::string());
        let mut inner_writer = FlussMapWriter::new(1, &DataTypes::int(), &DataTypes::string());
        inner_writer.write_entry(1.into(), "b".into()).unwrap();
        let inner_map = inner_writer.complete().unwrap();

        let mut writer = FlussMapWriter::new(1, &DataTypes::string(), &map_type);
        writer
            .write_entry("a".into(), Datum::Map(inner_map))
            .unwrap();
        let map = writer.complete().unwrap();

        let decoded =
            FlussMap::from_bytes(map.as_bytes(), &DataTypes::string(), &map_type).unwrap();
        let decoded_keys = decoded.key_array();
        let decoded_values = decoded.value_array();

        assert_eq!(decoded_keys.get_string(0).unwrap(), "a");
        let decoded_inner_map = decoded_values
            .get_map(0, &DataTypes::int(), &DataTypes::string())
            .unwrap();
        assert_eq!(decoded_inner_map.key_array().get_int(0).unwrap(), 1);
        assert_eq!(decoded_inner_map.value_array().get_string(0).unwrap(), "b");
    }

    #[test]
    fn test_trailing_garbage() {
        let mut key_writer = FlussArrayWriter::new(1, &DataTypes::int());
        key_writer.write_int(0, 1);
        let key_array = key_writer.complete().unwrap();

        let mut value_writer = FlussArrayWriter::new(1, &DataTypes::int());
        value_writer.write_int(0, 100);
        let value_array = value_writer.complete().unwrap();

        let map = FlussMap::from_arrays(
            &key_array,
            &value_array,
            &DataTypes::int(),
            &DataTypes::int(),
        )
        .unwrap();
        let bytes = map.as_bytes();

        // Valid bytes should pass
        assert!(FlussMap::from_bytes(bytes, &DataTypes::int(), &DataTypes::int()).is_ok());

        // Append trailing garbage
        let mut bad_bytes = bytes.to_vec();
        bad_bytes.push(0);
        let err =
            FlussMap::from_bytes(&bad_bytes, &DataTypes::int(), &DataTypes::int()).unwrap_err();
        assert!(err.to_string().contains("binary validation failed"));
        assert!(err.to_string().contains("expected"));
    }

    #[test]
    fn test_null_keys_fail_validation() {
        let mut key_writer = FlussArrayWriter::new(1, &DataTypes::int());
        key_writer.set_null_at(0);
        let key_array = key_writer.complete().unwrap();

        let mut value_writer = FlussArrayWriter::new(1, &DataTypes::int());
        value_writer.write_int(0, 100);
        let value_array = value_writer.complete().unwrap();

        let err = FlussMap::from_arrays(
            &key_array,
            &value_array,
            &DataTypes::int(),
            &DataTypes::int(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("keys cannot be null"));

        let key_bytes = key_array.as_bytes();
        let value_bytes = value_array.as_bytes();
        let mut data = vec![];
        data.extend_from_slice(&(key_bytes.len() as i32).to_le_bytes());
        data.extend_from_slice(key_bytes);
        data.extend_from_slice(value_bytes);

        let err = FlussMap::from_bytes(&data, &DataTypes::int(), &DataTypes::int()).unwrap_err();
        assert!(err.to_string().contains("keys cannot be null"));
    }

    #[test]
    fn entries_yields_typed_pairs_including_nulls() {
        let mut writer = FlussMapWriter::new(3, &DataTypes::string(), &DataTypes::int());
        writer.write_entry("a".into(), 1.into()).unwrap();
        writer.write_entry("b".into(), Datum::Null).unwrap();
        writer.write_entry("c".into(), 3.into()).unwrap();
        let map = writer.complete().unwrap();

        let collected: Vec<(Datum, Datum)> = map
            .entries()
            .collect::<Result<Vec<_>>>()
            .expect("entries should decode cleanly");

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0], (Datum::from("a"), Datum::from(1i32)));
        assert_eq!(collected[1].0, Datum::from("b"));
        assert_eq!(collected[1].1, Datum::Null);
        assert_eq!(collected[2], (Datum::from("c"), Datum::from(3i32)));
    }

    #[test]
    fn get_finds_present_key_and_returns_none_for_absent() {
        let mut writer = FlussMapWriter::new(2, &DataTypes::string(), &DataTypes::int());
        writer.write_entry("a".into(), 10.into()).unwrap();
        writer.write_entry("b".into(), 20.into()).unwrap();
        let map = writer.complete().unwrap();

        let v = map.get(&Datum::from("b")).unwrap();
        assert_eq!(v, Some(Datum::from(20i32)));

        let missing = map.get(&Datum::from("z")).unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn writer_extend_from_iterator_round_trips() {
        let src: Vec<(&str, i32)> = vec![("a", 1), ("b", 2), ("c", 3)];
        let mut writer = FlussMapWriter::new(src.len(), &DataTypes::string(), &DataTypes::int());
        writer.extend(src).unwrap();
        let map = writer.complete().unwrap();

        assert_eq!(map.size(), 3);
        assert_eq!(map.get(&Datum::from("b")).unwrap(), Some(Datum::from(2i32)));
    }
}
