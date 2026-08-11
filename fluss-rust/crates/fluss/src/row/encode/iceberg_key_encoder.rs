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

use bytes::Bytes;

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::RowType;
use crate::row::binary::{BinaryWriter, IcebergBinaryRowWriter, ValueWriter};
use crate::row::encode::KeyEncoder;
use crate::row::field_getter::FieldGetter;
use crate::row::{Datum, InternalRow};

/// Rust port of Java's `org.apache.fluss.row.encode.iceberg.IcebergKeyEncoder`.
///
/// Iceberg currently supports exactly one bucket key field. The field is
/// encoded with Iceberg's single-value binary representation before being
/// passed to the Iceberg bucketing function.
pub struct IcebergKeyEncoder {
    field_getter: FieldGetter,
    field_encoder: ValueWriter,
    writer: IcebergBinaryRowWriter,
}

impl IcebergKeyEncoder {
    /// Construct an Iceberg key encoder for the single key in `keys`.
    pub fn new(row_type: &RowType, keys: &[String]) -> Result<Self> {
        if keys.len() != 1 {
            return Err(IllegalArgument {
                message: format!(
                    "Key fields must have exactly one field for iceberg format, but got: {keys:?}"
                ),
            });
        }

        let key = &keys[0];
        let index = row_type
            .get_field_index(key)
            .ok_or_else(|| IllegalArgument {
                message: format!("Field {key:?} not found in input row type {row_type:?}"),
            })?;
        let data_type = row_type.fields()[index].data_type();

        Ok(Self {
            field_getter: FieldGetter::create(data_type, index),
            field_encoder: IcebergBinaryRowWriter::create_value_writer(data_type)?,
            writer: IcebergBinaryRowWriter::new(),
        })
    }
}

impl KeyEncoder for IcebergKeyEncoder {
    fn encode_key(&mut self, row: &dyn InternalRow) -> Result<Bytes> {
        self.writer.reset();

        let datum = self.field_getter.get_field(row)?;
        if datum == Datum::Null {
            return Err(IllegalArgument {
                message: "Iceberg key columns do not support null values".to_string(),
            });
        }
        self.field_encoder
            .write_value(&mut self.writer, 0, &datum)?;

        Ok(self.writer.to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucketing::BucketingFunction;
    use crate::metadata::{DataLakeFormat, DataType, DataTypes};
    use crate::row::{Date, Decimal, GenericRow, Time, TimestampNtz};

    fn encode(data_type: DataType, datum: Datum<'static>) -> Bytes {
        let row_type = RowType::with_data_types_and_field_names(vec![data_type], vec!["key"]);
        let mut encoder = IcebergKeyEncoder::new(&row_type, &["key".to_string()]).unwrap();
        encoder
            .encode_key(&GenericRow::from_data(vec![datum]))
            .unwrap()
    }

    fn assert_iceberg_hash(data_type: DataType, datum: Datum<'static>, expected_hash: i32) {
        let encoded = encode(data_type, datum);
        let bucketing = <dyn BucketingFunction>::of(Some(&DataLakeFormat::Iceberg));
        // With i32::MAX buckets, the bucket id is the raw hash with its sign bit cleared.
        let actual = bucketing.bucketing(&encoded, i32::MAX).unwrap();

        assert_eq!(actual, expected_hash & i32::MAX);
    }

    #[test]
    fn rejects_key_counts_other_than_one() {
        let row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataTypes::string()],
            vec!["id", "name"],
        );

        for keys in [Vec::new(), vec!["id".to_string(), "name".to_string()]] {
            let error = match IcebergKeyEncoder::new(&row_type, &keys) {
                Ok(_) => panic!("expected IllegalArgument"),
                Err(error) => error,
            };
            assert!(
                error
                    .to_string()
                    .contains("Key fields must have exactly one field for iceberg format")
            );
        }
    }

    #[test]
    fn rejects_missing_key_field() {
        let row_type = RowType::with_data_types_and_field_names(vec![DataTypes::int()], vec!["id"]);
        let error = match IcebergKeyEncoder::new(&row_type, &["missing".to_string()]) {
            Ok(_) => panic!("expected IllegalArgument"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("not found in input row type"));
    }

    #[test]
    fn hashes_iceberg_appendix_b_vectors() {
        // https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
        assert_iceberg_hash(DataTypes::int(), Datum::from(34_i32), 2_017_239_379);
        assert_iceberg_hash(DataTypes::bigint(), Datum::from(34_i64), 2_017_239_379);
        assert_iceberg_hash(
            DataTypes::decimal(4, 2),
            Datum::Decimal(Decimal::from_unscaled_long(1_420, 4, 2).unwrap()),
            -500_754_589,
        );
        assert_iceberg_hash(
            DataTypes::date(),
            // 2017-11-16, as days from the Unix epoch.
            Datum::Date(Date::new(17_486)),
            -653_330_422,
        );
        assert_iceberg_hash(
            DataTypes::time(),
            // 22:31:08, as milliseconds from midnight.
            Datum::Time(Time::new(81_068_000)),
            -662_762_989,
        );
        assert_iceberg_hash(
            DataTypes::timestamp_with_precision(6),
            // 2017-11-16T22:31:08.
            Datum::TimestampNtz(TimestampNtz::new(1_510_871_468_000)),
            -2_047_944_441,
        );
        assert_iceberg_hash(
            DataTypes::timestamp_with_precision(6),
            // 2017-11-16T22:31:08.000001.
            Datum::TimestampNtz(TimestampNtz::from_millis_nanos(1_510_871_468_000, 1_000).unwrap()),
            -1_207_196_810,
        );
        assert_iceberg_hash(DataTypes::string(), Datum::from("iceberg"), 1_210_000_089);
        assert_iceberg_hash(
            DataTypes::binary(4),
            Datum::from(vec![0_u8, 1, 2, 3]),
            -188_683_207,
        );
        assert_iceberg_hash(
            DataTypes::bytes(),
            Datum::from(vec![0_u8, 1, 2, 3]),
            -188_683_207,
        );
    }

    #[test]
    fn rejects_null_key_without_panicking() {
        let row_type = RowType::with_data_types_and_field_names(vec![DataTypes::int()], vec!["id"]);
        let mut encoder = IcebergKeyEncoder::new(&row_type, &["id".to_string()]).unwrap();
        let error = encoder
            .encode_key(&GenericRow::from_data(vec![Datum::Null]))
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Iceberg key columns do not support null values")
        );
    }

    #[test]
    fn reuses_writer_across_rows() {
        let row_type =
            RowType::with_data_types_and_field_names(vec![DataTypes::string()], vec!["id"]);
        let mut encoder = IcebergKeyEncoder::new(&row_type, &["id".to_string()]).unwrap();

        let first = encoder
            .encode_key(&GenericRow::from_data(vec![Datum::from(
                "a longer first key",
            )]))
            .unwrap();
        let second = encoder
            .encode_key(&GenericRow::from_data(vec![Datum::from("short")]))
            .unwrap();

        assert_eq!(first.as_ref(), b"a longer first key");
        assert_eq!(second.as_ref(), b"short");
    }
}
