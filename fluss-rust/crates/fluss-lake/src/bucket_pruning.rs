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

//! Conservative bucket-key pruning over the engine-supplied filter predicate.
//!
//! Buckets are pruned only when the filter provides equality constraints on
//! every bucket-key column, so the bucket hash can be recomputed exactly. Any
//! missing or non-equality condition on a bucket key keeps all buckets.

use fluss::BucketingFunction;
use fluss::metadata::{DataLakeFormat, RowType};
use fluss::predicate::{BoundPredicate, CompoundFunction, LeafFunction, Literal};
use fluss::row::GenericRow;
use fluss::row::encode::KeyEncoderFactory;
use std::collections::{HashMap, HashSet};

// Pruning is optional: bound both candidate storage and the number of key
// values hashed, falling back to the exact output filter for larger predicates.
const MAX_BUCKET_PRUNING_WORK: usize = 4096;

/// Decides which buckets may contain rows that satisfy the scan filter.
pub(crate) struct BucketPruner {
    matching_buckets: Option<HashSet<i32>>,
}

impl BucketPruner {
    /// Builds a pruner for one planning pass.
    ///
    /// Returns a pruner that keeps every bucket when the filter does not
    /// provably constrain all bucket-key columns to equality values.
    pub(crate) fn new(
        row_type: &RowType,
        bucket_keys: &[String],
        num_buckets: i32,
        data_lake_format: &Option<DataLakeFormat>,
        filter: &BoundPredicate,
    ) -> Self {
        if bucket_keys.is_empty() || num_buckets <= 0 {
            return Self {
                matching_buckets: None,
            };
        }

        let constraints = match extract_bucket_constraints(filter, bucket_keys) {
            Some(constraints) => constraints,
            None => {
                return Self {
                    matching_buckets: None,
                };
            }
        };
        let Some(combination_count) = constraints.combination_count() else {
            return Self {
                matching_buckets: None,
            };
        };

        match compute_matching_buckets(
            row_type,
            bucket_keys,
            num_buckets,
            data_lake_format,
            &constraints,
            combination_count,
        ) {
            Ok(buckets) => Self {
                matching_buckets: Some(buckets),
            },
            Err(_) => Self {
                matching_buckets: None,
            },
        }
    }

    /// Returns whether a bucket may contain matching rows.
    pub(crate) fn bucket_may_match(&self, bucket_id: i32) -> bool {
        match &self.matching_buckets {
            Some(buckets) => buckets.contains(&bucket_id),
            None => true,
        }
    }
}

/// Equality values extracted for each bucket-key column.
#[derive(Debug, Clone)]
struct BucketConstraints<'a> {
    values: Vec<Vec<&'a Literal>>,
}

impl BucketConstraints<'_> {
    fn combination_count(&self) -> Option<usize> {
        let count = self.values.iter().try_fold(1_usize, |count, values| {
            count
                .checked_mul(values.len())
                .filter(|count| *count <= MAX_BUCKET_PRUNING_WORK)
        })?;
        (count.checked_mul(self.values.len())? <= MAX_BUCKET_PRUNING_WORK).then_some(count)
    }
}

fn extract_bucket_constraints<'a>(
    filter: &'a BoundPredicate,
    bucket_keys: &[String],
) -> Option<BucketConstraints<'a>> {
    if bucket_keys.len() > MAX_BUCKET_PRUNING_WORK {
        return None;
    }
    let mut per_column: HashMap<&str, Vec<&Literal>> = HashMap::new();
    let mut value_count = 0_usize;

    // Collect equality constraints from the top-level AND structure.
    let mut worklist: Vec<&BoundPredicate> = vec![filter];
    while let Some(predicate) = worklist.pop() {
        match predicate {
            BoundPredicate::Compound {
                function: CompoundFunction::And,
                children,
            } => worklist.extend(children),
            BoundPredicate::Leaf {
                field_name,
                function: LeafFunction::Equal | LeafFunction::In,
                literals,
                ..
            } if bucket_keys.contains(field_name) => {
                value_count = value_count.checked_add(literals.len())?;
                if value_count > MAX_BUCKET_PRUNING_WORK {
                    return None;
                }
                per_column
                    .entry(field_name.as_str())
                    .or_default()
                    .extend(literals);
            }
            _ => {}
        }
    }

    let values: Vec<Vec<&Literal>> = bucket_keys
        .iter()
        .map(|key| per_column.remove(key.as_str()).unwrap_or_default())
        .collect();

    if values.iter().any(Vec::is_empty) {
        return None;
    }
    Some(BucketConstraints { values })
}

fn compute_matching_buckets(
    row_type: &RowType,
    bucket_keys: &[String],
    num_buckets: i32,
    data_lake_format: &Option<DataLakeFormat>,
    constraints: &BucketConstraints<'_>,
    combination_count: usize,
) -> crate::Result<HashSet<i32>> {
    let mut encoder =
        KeyEncoderFactory::of_bucket_key_encoder(row_type, bucket_keys, data_lake_format).map_err(
            |error| {
                crate::FlussLakeError::PlanningFailed(format!(
                    "failed to create bucket-key encoder for pruning: {error}"
                ))
            },
        )?;
    let bucketing = <dyn BucketingFunction>::of(data_lake_format.as_ref());
    let key_positions: Vec<usize> = bucket_keys
        .iter()
        .map(|key| {
            row_type
                .get_field_index(key)
                .expect("bucket keys were validated against the row type")
        })
        .collect();

    let mut buckets = HashSet::new();
    // Enumerate one mixed-radix candidate at a time, never materializing the
    // Cartesian product. The caller has already bounded total hashing work.
    for mut index in 0..combination_count {
        let mut row = GenericRow::new(row_type.fields().len());
        for (position, values) in key_positions.iter().zip(&constraints.values) {
            row.set_field(*position, values[index % values.len()].to_datum());
            index /= values.len();
        }
        let key_bytes = encoder.encode_key(&row).map_err(|error| {
            crate::FlussLakeError::PlanningFailed(format!(
                "failed to encode bucket-key value for pruning: {error}"
            ))
        })?;
        let bucket_id = bucketing
            .bucketing(&key_bytes, num_buckets)
            .map_err(|error| {
                crate::FlussLakeError::PlanningFailed(format!(
                    "failed to compute bucket for pruning: {error}"
                ))
            })?;
        buckets.insert(bucket_id);
        if buckets.len() == num_buckets as usize {
            break;
        }
    }
    Ok(buckets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::{DataField, DataTypes};
    use fluss::predicate::{Predicate, col};

    fn row_type() -> RowType {
        RowType::new(vec![
            DataField::new("id", DataTypes::int(), None),
            DataField::new("region", DataTypes::string(), None),
        ])
    }

    fn bound(predicate: Predicate) -> BoundPredicate {
        BoundPredicate::bind(Some(&predicate), &row_type()).unwrap()
    }

    #[test]
    fn no_bucket_keys_keeps_every_bucket() {
        let pruner = BucketPruner::new(&row_type(), &[], 4, &None, &bound(col("id").eq(1_i32)));
        assert!(pruner.bucket_may_match(0));
        assert!(pruner.bucket_may_match(3));
    }

    #[test]
    fn equality_on_single_bucket_key_prunes_others() {
        let pruner = BucketPruner::new(
            &row_type(),
            &["id".to_string()],
            4,
            &None,
            &bound(col("id").eq(1_i32)),
        );
        // Hash of 1 with Fluss bucketing lands in one specific bucket.
        let matching: Vec<i32> = (0..4).filter(|id| pruner.bucket_may_match(*id)).collect();
        assert_eq!(matching.len(), 1);
    }

    #[test]
    fn pruning_recomputes_the_hash_modulus_for_each_partition_layout() {
        let format = Some(DataLakeFormat::Paimon);
        let keys = vec!["id".to_string()];
        let ty = row_type();
        let mut differs = false;
        for key in 0..32_i32 {
            let filter = bound(col("id").eq(key));
            let small = BucketPruner::new(&ty, &keys, 2, &format, &filter);
            let large = BucketPruner::new(&ty, &keys, 8, &format, &filter);
            let small_ids = (0..2)
                .filter(|id| small.bucket_may_match(*id))
                .collect::<Vec<_>>();
            let large_ids = (0..8)
                .filter(|id| large.bucket_may_match(*id))
                .collect::<Vec<_>>();
            assert_eq!(small_ids.len(), 1);
            assert_eq!(large_ids.len(), 1);
            assert_eq!(large_ids[0] % 2, small_ids[0]);
            differs |= small_ids[0] != large_ids[0];
        }
        assert!(
            differs,
            "the fixture must catch reuse of the table-default pruner"
        );
    }

    #[test]
    fn missing_bucket_key_constraint_keeps_every_bucket() {
        let pruner = BucketPruner::new(
            &row_type(),
            &["id".to_string()],
            4,
            &None,
            &bound(col("region").eq("US")),
        );
        assert!(pruner.bucket_may_match(0));
        assert!(pruner.bucket_may_match(3));
    }

    #[test]
    fn in_list_on_bucket_key_prunes_to_matching_buckets() {
        let pruner = BucketPruner::new(
            &row_type(),
            &["id".to_string()],
            4,
            &None,
            &bound(col("id").is_in([1_i32, 2_i32])),
        );
        let matching: Vec<i32> = (0..4).filter(|id| pruner.bucket_may_match(*id)).collect();
        assert!(!matching.is_empty());
        assert!(matching.len() <= 2);
    }

    #[test]
    fn large_cartesian_product_keeps_all_buckets() {
        let filter = bound(
            col("id")
                .is_in(0..100_i32)
                .and(col("region").is_in((0..100).map(|i| i.to_string()))),
        );
        let keys = vec!["id".to_string(), "region".to_string()];
        let constraints = extract_bucket_constraints(&filter, &keys).unwrap();
        assert_eq!(constraints.combination_count(), None);
        let pruner = BucketPruner::new(&row_type(), &keys, 128, &None, &filter);
        assert!(pruner.matching_buckets.is_none());
        assert!((0..128).all(|bucket| pruner.bucket_may_match(bucket)));
    }

    #[test]
    fn oversized_single_in_list_skips_candidate_collection() {
        let filter = bound(col("id").is_in(0..=MAX_BUCKET_PRUNING_WORK as i32));
        let keys = vec!["id".to_string()];
        assert!(extract_bucket_constraints(&filter, &keys).is_none());
        let pruner = BucketPruner::new(&row_type(), &keys, 4, &None, &filter);
        assert!(pruner.matching_buckets.is_none());
    }

    #[test]
    fn combination_budget_includes_key_width_and_cannot_overflow() {
        let filter = bound(col("id").eq(1_i32));
        let keys = vec!["id".to_string()];
        let constraints = extract_bucket_constraints(&filter, &keys).unwrap();
        let value = constraints.values[0][0];
        let at_limit = BucketConstraints {
            values: vec![vec![value; 64], vec![value; 32]],
        };
        assert_eq!(at_limit.combination_count(), Some(2048));
        let above_limit = BucketConstraints {
            values: vec![vec![value; 64], vec![value; 33]],
        };
        assert_eq!(above_limit.combination_count(), None);
        let overflow = BucketConstraints {
            values: vec![vec![value; 2]; usize::BITS as usize],
        };
        assert_eq!(overflow.combination_count(), None);
    }

    #[test]
    fn lazy_combinations_match_individually_pruned_keys() {
        let keys = vec!["id".to_string(), "region".to_string()];
        for format in [None, Some(DataLakeFormat::Paimon)] {
            let filter = bound(
                col("id")
                    .is_in([1_i32, 2, 3])
                    .and(col("region").is_in(["US", "EU"])),
            );
            let pruner = BucketPruner::new(&row_type(), &keys, 128, &format, &filter);
            let mut expected = HashSet::new();
            for id in [1_i32, 2, 3] {
                for region in ["US", "EU"] {
                    let single = bound(col("id").eq(id).and(col("region").eq(region)));
                    let single = BucketPruner::new(&row_type(), &keys, 128, &format, &single);
                    expected.extend(single.matching_buckets.unwrap());
                }
            }
            assert_eq!(pruner.matching_buckets, Some(expected));
        }
    }
}
