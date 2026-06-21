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

use crate::bucketing::BucketingFunction;
use crate::cluster::Cluster;
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::PhysicalTablePath;
use bytes::Bytes;
use rand::Rng;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

pub trait BucketAssigner: Sync + Send {
    fn abort_if_batch_full(&self) -> bool;

    fn on_new_batch(&self, cluster: &Cluster, prev_bucket_id: i32);

    fn assign_bucket(&self, bucket_key: Option<&Bytes>, cluster: &Cluster) -> Result<i32>;
}

#[derive(Debug)]
pub struct StickyBucketAssigner {
    table_path: Arc<PhysicalTablePath>,
    current_bucket_id: AtomicI32,
}

impl StickyBucketAssigner {
    pub fn new(table_path: Arc<PhysicalTablePath>) -> Self {
        Self {
            table_path,
            current_bucket_id: AtomicI32::new(-1),
        }
    }

    fn next_bucket(&self, cluster: &Cluster, prev_bucket_id: i32) -> i32 {
        let old_bucket = self.current_bucket_id.load(Ordering::Relaxed);
        let mut new_bucket = old_bucket;
        if old_bucket < 0 || old_bucket == prev_bucket_id {
            let available_buckets = cluster.get_available_buckets_for_table_path(&self.table_path);
            if available_buckets.is_empty() {
                let mut rng = rand::rng();
                let mut random: i32 = rng.random();
                random &= i32::MAX;
                new_bucket = random % cluster.get_bucket_count(self.table_path.get_table_path());
            } else if available_buckets.len() == 1 {
                new_bucket = available_buckets[0].table_bucket.bucket_id();
            } else {
                let mut rng = rand::rng();
                while new_bucket < 0 || new_bucket == old_bucket {
                    let mut random: i32 = rng.random();
                    random &= i32::MAX;
                    new_bucket = available_buckets
                        [(random % available_buckets.len() as i32) as usize]
                        .bucket_id();
                }
            }
        }

        if old_bucket < 0 {
            self.current_bucket_id.store(new_bucket, Ordering::Relaxed);
        } else {
            self.current_bucket_id
                .compare_exchange(
                    prev_bucket_id,
                    new_bucket,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .ok();
        }
        self.current_bucket_id.load(Ordering::Relaxed)
    }
}

impl BucketAssigner for StickyBucketAssigner {
    fn abort_if_batch_full(&self) -> bool {
        true
    }

    fn on_new_batch(&self, cluster: &Cluster, prev_bucket_id: i32) {
        self.next_bucket(cluster, prev_bucket_id);
    }

    fn assign_bucket(&self, _bucket_key: Option<&Bytes>, cluster: &Cluster) -> Result<i32> {
        let bucket_id = self.current_bucket_id.load(Ordering::Relaxed);
        if bucket_id < 0 {
            Ok(self.next_bucket(cluster, bucket_id))
        } else {
            Ok(bucket_id)
        }
    }
}

/// Unlike [StickyBucketAssigner], each record is assigned to the next bucket
/// in a rotating sequence, providing even data distribution across all buckets.
pub struct RoundRobinBucketAssigner {
    table_path: Arc<PhysicalTablePath>,
    num_buckets: i32,
    counter: AtomicI32,
}

impl RoundRobinBucketAssigner {
    pub fn new(table_path: Arc<PhysicalTablePath>, num_buckets: i32) -> Self {
        let mut rng = rand::rng();
        Self {
            table_path,
            num_buckets,
            counter: AtomicI32::new(rng.random()),
        }
    }
}

impl BucketAssigner for RoundRobinBucketAssigner {
    fn abort_if_batch_full(&self) -> bool {
        false
    }

    fn on_new_batch(&self, _cluster: &Cluster, _prev_bucket_id: i32) {}

    fn assign_bucket(&self, _bucket_key: Option<&Bytes>, cluster: &Cluster) -> Result<i32> {
        let next_value = self.counter.fetch_add(1, Ordering::Relaxed);
        let available_buckets = cluster.get_available_buckets_for_table_path(&self.table_path);
        if available_buckets.is_empty() {
            Ok((next_value & i32::MAX) % self.num_buckets)
        } else {
            let idx = (next_value & i32::MAX) % available_buckets.len() as i32;
            Ok(available_buckets[idx as usize].bucket_id())
        }
    }
}

/// A [BucketAssigner] which assigns based on a modulo hashing function
pub struct HashBucketAssigner {
    num_buckets: i32,
    bucketing_function: Box<dyn BucketingFunction>,
}

#[allow(dead_code)]
impl HashBucketAssigner {
    /// Creates a new [HashBucketAssigner] based on the given [BucketingFunction].
    /// See [BucketingFunction.of(Option<&DataLakeFormat>)] for bucketing functions.
    ///
    ///
    /// # Arguments
    /// * `num_buckets` - The number of buckets
    /// * `bucketing_function` - The bucketing function
    ///
    /// # Returns
    /// * [HashBucketAssigner] - The hash bucket assigner
    pub fn new(num_buckets: i32, bucketing_function: Box<dyn BucketingFunction>) -> Self {
        HashBucketAssigner {
            num_buckets,
            bucketing_function,
        }
    }
}

impl BucketAssigner for HashBucketAssigner {
    fn abort_if_batch_full(&self) -> bool {
        false
    }

    fn on_new_batch(&self, _: &Cluster, _: i32) {
        // do nothing
    }

    fn assign_bucket(&self, bucket_key: Option<&Bytes>, _: &Cluster) -> Result<i32> {
        let key = bucket_key.ok_or_else(|| IllegalArgument {
            message: "no bucket key provided".to_string(),
        })?;
        self.bucketing_function.bucketing(key, self.num_buckets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucketing::BucketingFunction;
    use crate::cluster::Cluster;
    use crate::metadata::TablePath;
    use crate::test_utils::build_cluster;
    use std::sync::Arc;

    #[test]
    fn sticky_bucket_assigner_picks_available_bucket() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster(&table_path, 1, 2);
        let assigner = StickyBucketAssigner::new(Arc::new(PhysicalTablePath::of(Arc::new(
            table_path.clone(),
        ))));
        let bucket = assigner.assign_bucket(None, &cluster).expect("bucket");
        assert!((0..2).contains(&bucket));

        assigner.on_new_batch(&cluster, bucket);
        let next_bucket = assigner.assign_bucket(None, &cluster).expect("bucket");
        assert!((0..2).contains(&next_bucket));
    }

    #[test]
    fn round_robin_assigner_cycles_through_buckets() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let num_buckets = 3;
        let cluster = build_cluster(&table_path, 1, num_buckets);
        let physical = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));
        let assigner = RoundRobinBucketAssigner::new(physical, num_buckets);

        let mut seen = Vec::new();
        for _ in 0..(num_buckets * 2) {
            let bucket = assigner.assign_bucket(None, &cluster).expect("bucket");
            assert!((0..num_buckets).contains(&bucket));
            seen.push(bucket);
        }

        assert_eq!(seen[0], seen[3]);
        assert_eq!(seen[1], seen[4]);
        assert_eq!(seen[2], seen[5]);
    }

    #[test]
    fn round_robin_assigner_does_not_abort_on_batch_full() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let physical = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));
        let assigner = RoundRobinBucketAssigner::new(physical, 3);
        assert!(!assigner.abort_if_batch_full());
    }

    #[test]
    fn hash_bucket_assigner_requires_key() {
        let assigner = HashBucketAssigner::new(3, <dyn BucketingFunction>::of(None));
        let cluster = Cluster::default();
        let err = assigner.assign_bucket(None, &cluster).unwrap_err();
        assert!(matches!(err, IllegalArgument { .. }));
    }

    #[test]
    fn hash_bucket_assigner_hashes_key() {
        let assigner = HashBucketAssigner::new(4, <dyn BucketingFunction>::of(None));
        let cluster = Cluster::default();
        let bucket = assigner
            .assign_bucket(Some(&Bytes::from_static(b"key")), &cluster)
            .expect("bucket");
        assert!((0..4).contains(&bucket));
    }
}
