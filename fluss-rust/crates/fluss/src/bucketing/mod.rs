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

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::DataLakeFormat;
use crate::util::murmur_hash;

pub trait BucketingFunction: Sync + Send {
    fn bucketing(&self, bucket_key: &[u8], num_buckets: i32) -> Result<i32>;
}

impl dyn BucketingFunction {
    /// Provides the bucketing function for a given [DataLakeFormat]
    ///
    /// # Arguments
    /// * `lake_format` - Data lake format or none
    ///
    /// # Returns
    /// * BucketingFunction
    pub fn of(lake_format: Option<&DataLakeFormat>) -> Box<dyn BucketingFunction> {
        match lake_format {
            None => Box::new(FlussBucketingFunction),
            Some(DataLakeFormat::Paimon) => Box::new(PaimonBucketingFunction),
            Some(DataLakeFormat::Lance) => Box::new(FlussBucketingFunction),
            Some(DataLakeFormat::Iceberg) => Box::new(IcebergBucketingFunction),
        }
    }
}

struct FlussBucketingFunction;
impl BucketingFunction for FlussBucketingFunction {
    fn bucketing(&self, bucket_key: &[u8], num_buckets: i32) -> Result<i32> {
        if bucket_key.is_empty() {
            return Err(IllegalArgument {
                message: "bucket_key must not be empty!".to_string(),
            });
        }

        if num_buckets <= 0 {
            return Err(IllegalArgument {
                message: "num_buckets must be positive!".to_string(),
            });
        }

        let key_hash = murmur_hash::fluss_hash_bytes(bucket_key)?;

        Ok(murmur_hash::fluss_hash_i32(key_hash) % num_buckets)
    }
}

struct PaimonBucketingFunction;
impl BucketingFunction for PaimonBucketingFunction {
    fn bucketing(&self, bucket_key: &[u8], num_buckets: i32) -> Result<i32> {
        if bucket_key.is_empty() {
            return Err(IllegalArgument {
                message: "bucket_key must not be empty!".to_string(),
            });
        }

        if num_buckets <= 0 {
            return Err(IllegalArgument {
                message: "num_buckets must be positive!".to_string(),
            });
        }

        let key_hash = murmur_hash::fluss_hash_bytes(bucket_key)?;

        Ok((key_hash % num_buckets).abs())
    }
}

struct IcebergBucketingFunction;
impl BucketingFunction for IcebergBucketingFunction {
    fn bucketing(&self, bucket_key: &[u8], num_buckets: i32) -> Result<i32> {
        if bucket_key.is_empty() {
            return Err(IllegalArgument {
                message: "bucket_key must not be empty!".to_string(),
            });
        }

        if num_buckets <= 0 {
            return Err(IllegalArgument {
                message: "num_buckets must be positive!".to_string(),
            });
        };

        Ok((murmur_hash::hash_bytes(bucket_key) as i32 & i32::MAX) % num_buckets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_bucketing() {
        let default_bucketing = <dyn BucketingFunction>::of(None);

        let expected = 1;
        let actual = default_bucketing.bucketing(&[00u8, 10u8], 7).unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 0;
        let actual = default_bucketing
            .bucketing(&[00u8, 10u8, 10u8, 10u8], 12)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 6;
        let actual = default_bucketing
            .bucketing("2bb87d68-baf9-4e64-90f9-f80910419fa6".as_bytes(), 16)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 6;
        let actual = default_bucketing
            .bucketing("The quick brown fox jumps over the lazy dog".as_bytes(), 8)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );
    }

    #[test]
    fn test_paimon_bucketing() {
        let paimon_bucketing = <dyn BucketingFunction>::of(Some(&DataLakeFormat::Paimon));

        let expected = 1;
        let actual = paimon_bucketing.bucketing(&[00u8, 10u8], 7).unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 11;
        let actual = paimon_bucketing
            .bucketing(&[00u8, 10u8, 10u8, 10u8], 12)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 12;
        let actual = paimon_bucketing
            .bucketing("2bb87d68-baf9-4e64-90f9-f80910419fa6".as_bytes(), 16)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 0;
        let actual = paimon_bucketing
            .bucketing("The quick brown fox jumps over the lazy dog".as_bytes(), 8)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );
    }

    #[test]
    fn test_lance_bucketing() {
        let lance_bucketing = <dyn BucketingFunction>::of(Some(&DataLakeFormat::Lance));

        let expected = 1;
        let actual = lance_bucketing.bucketing(&[00u8, 10u8], 7).unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 0;
        let actual = lance_bucketing
            .bucketing(&[00u8, 10u8, 10u8, 10u8], 12)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 6;
        let actual = lance_bucketing
            .bucketing("2bb87d68-baf9-4e64-90f9-f80910419fa6".as_bytes(), 16)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 6;
        let actual = lance_bucketing
            .bucketing("The quick brown fox jumps over the lazy dog".as_bytes(), 8)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );
    }

    #[test]
    fn test_iceberg_bucketing() {
        let iceberg_bucketing = <dyn BucketingFunction>::of(Some(&DataLakeFormat::Iceberg));

        let expected = 3;
        let actual = iceberg_bucketing.bucketing(&[00u8, 10u8], 7).unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 4;
        let actual = iceberg_bucketing
            .bucketing(&[00u8, 10u8, 10u8, 10u8], 12)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 12;
        let actual = iceberg_bucketing
            .bucketing("2bb87d68-baf9-4e64-90f9-f80910419fa6".as_bytes(), 16)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );

        let expected = 3;
        let actual = iceberg_bucketing
            .bucketing("The quick brown fox jumps over the lazy dog".as_bytes(), 8)
            .unwrap();
        assert_eq!(
            expected, actual,
            "Expecting bucket to be {expected} but got {actual}"
        );
    }
}
