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
use crate::BucketId;
use crate::bucketing::BucketingFunction;
use crate::cluster::Cluster;
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::PhysicalTablePath;
use bytes::Bytes;
use rand::Rng;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

/// `bucket_count` can differ from the table's `num_buckets` after `bucket.num` changes.
pub trait BucketAssigner: Sync + Send {
    fn abort_if_batch_full(&self) -> bool;

    fn on_new_batch(&self, cluster: &Cluster, bucket_count: i32, prev_bucket_id: BucketId);

    fn assign_bucket(
        &self,
        bucket_key: Option<&Bytes>,
        cluster: &Cluster,
        bucket_count: i32,
    ) -> Result<BucketId>;
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

    fn next_bucket(
        &self,
        cluster: &Cluster,
        bucket_count: i32,
        prev_bucket_id: BucketId,
    ) -> BucketId {
        let old_bucket = self.current_bucket_id.load(Ordering::Relaxed);
        if old_bucket < 0 || old_bucket >= bucket_count || old_bucket == prev_bucket_id {
            let available_buckets = cluster.get_available_buckets_for_table_path(&self.table_path);
            let random = rand::rng().random::<i32>() & i32::MAX;
            let start = match available_buckets.len() {
                0 => 0,
                len => random as usize % len,
            };
            let (before, from_start) = available_buckets.split_at(start);
            // Available buckets may lie outside a provisional count.
            let candidate = from_start
                .iter()
                .chain(before)
                .map(|bucket| bucket.bucket_id())
                .find(|&bucket| bucket < bucket_count && bucket != old_bucket);
            let new_bucket = match candidate {
                Some(bucket) => bucket,
                None if !available_buckets.is_empty()
                    && (0..bucket_count).contains(&old_bucket) =>
                {
                    old_bucket
                }
                None => random % bucket_count,
            };

            if old_bucket < 0 {
                self.current_bucket_id.store(new_bucket, Ordering::Relaxed);
            } else {
                self.current_bucket_id
                    .compare_exchange(old_bucket, new_bucket, Ordering::Relaxed, Ordering::Relaxed)
                    .ok();
            }
            // Another producer may have stored a bucket chosen for a larger count.
            let current = self.current_bucket_id.load(Ordering::Relaxed);
            return if (0..bucket_count).contains(&current) {
                current
            } else {
                new_bucket
            };
        }
        self.current_bucket_id.load(Ordering::Relaxed)
    }
}

impl BucketAssigner for StickyBucketAssigner {
    fn abort_if_batch_full(&self) -> bool {
        true
    }

    fn on_new_batch(&self, cluster: &Cluster, bucket_count: i32, prev_bucket_id: BucketId) {
        self.next_bucket(cluster, bucket_count, prev_bucket_id);
    }

    fn assign_bucket(
        &self,
        _bucket_key: Option<&Bytes>,
        cluster: &Cluster,
        bucket_count: i32,
    ) -> Result<BucketId> {
        let bucket_id = self.current_bucket_id.load(Ordering::Relaxed);
        if bucket_id < 0 || bucket_id >= bucket_count {
            Ok(self.next_bucket(cluster, bucket_count, bucket_id))
        } else {
            Ok(bucket_id)
        }
    }
}

/// Unlike [StickyBucketAssigner], each record is assigned to the next bucket
/// in a rotating sequence, providing even data distribution across all buckets.
pub struct RoundRobinBucketAssigner {
    table_path: Arc<PhysicalTablePath>,
    counter: AtomicI32,
}

impl RoundRobinBucketAssigner {
    pub fn new(table_path: Arc<PhysicalTablePath>) -> Self {
        let mut rng = rand::rng();
        Self {
            table_path,
            counter: AtomicI32::new(rng.random()),
        }
    }
}

impl BucketAssigner for RoundRobinBucketAssigner {
    fn abort_if_batch_full(&self) -> bool {
        false
    }

    fn on_new_batch(&self, _cluster: &Cluster, _bucket_count: i32, _prev_bucket_id: BucketId) {}

    fn assign_bucket(
        &self,
        _bucket_key: Option<&Bytes>,
        cluster: &Cluster,
        bucket_count: i32,
    ) -> Result<BucketId> {
        let next_value = self.counter.fetch_add(1, Ordering::Relaxed) & i32::MAX;
        let available_buckets = cluster.get_available_buckets_for_table_path(&self.table_path);
        if available_buckets.is_empty() {
            return Ok(next_value % bucket_count);
        }
        let idx = next_value % available_buckets.len() as i32;
        let bucket_id = available_buckets[idx as usize].bucket_id();
        // Metadata may already list the final layout while the count is still provisional.
        Ok(if bucket_id < bucket_count {
            bucket_id
        } else {
            next_value % bucket_count
        })
    }
}

/// A [BucketAssigner] which assigns based on a modulo hashing function
pub struct HashBucketAssigner {
    bucketing_function: Box<dyn BucketingFunction>,
}

impl HashBucketAssigner {
    /// Creates a new [HashBucketAssigner] based on the given [BucketingFunction].
    /// See [BucketingFunction.of(Option<&DataLakeFormat>)] for bucketing functions.
    pub fn new(bucketing_function: Box<dyn BucketingFunction>) -> Self {
        HashBucketAssigner { bucketing_function }
    }
}

impl BucketAssigner for HashBucketAssigner {
    fn abort_if_batch_full(&self) -> bool {
        false
    }

    fn on_new_batch(&self, _: &Cluster, _: i32, _: BucketId) {
        // do nothing
    }

    fn assign_bucket(
        &self,
        bucket_key: Option<&Bytes>,
        _: &Cluster,
        bucket_count: i32,
    ) -> Result<BucketId> {
        let key = bucket_key.ok_or_else(|| IllegalArgument {
            message: "no bucket key provided".to_string(),
        })?;
        self.bucketing_function.bucketing(key, bucket_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucketing::BucketingFunction;
    use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
    use crate::metadata::{TableBucket, TablePath};
    use crate::test_utils::build_cluster;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn sticky_bucket_assigner_picks_available_bucket() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster(&table_path, 1, 2);
        let assigner = StickyBucketAssigner::new(Arc::new(PhysicalTablePath::of(Arc::new(
            table_path.clone(),
        ))));
        let bucket = assigner.assign_bucket(None, &cluster, 2).expect("bucket");
        assert!((0..2).contains(&bucket));

        assigner.on_new_batch(&cluster, 2, bucket);
        let next_bucket = assigner.assign_bucket(None, &cluster, 2).expect("bucket");
        assert!((0..2).contains(&next_bucket));
    }

    #[test]
    fn round_robin_assigner_cycles_through_buckets() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let num_buckets = 3;
        let cluster = build_cluster(&table_path, 1, num_buckets);
        let physical = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));
        let assigner = RoundRobinBucketAssigner::new(physical);

        let mut seen = Vec::new();
        for _ in 0..(num_buckets * 2) {
            let bucket = assigner
                .assign_bucket(None, &cluster, num_buckets)
                .expect("bucket");
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
        let assigner = RoundRobinBucketAssigner::new(physical);
        assert!(!assigner.abort_if_batch_full());
    }

    #[test]
    fn hash_bucket_assigner_requires_key() {
        let assigner = HashBucketAssigner::new(<dyn BucketingFunction>::of(None));
        let cluster = Cluster::default();
        let err = assigner.assign_bucket(None, &cluster, 3).unwrap_err();
        assert!(matches!(err, IllegalArgument { .. }));
    }

    #[test]
    fn hash_bucket_assigner_hashes_key() {
        let assigner = HashBucketAssigner::new(<dyn BucketingFunction>::of(None));
        let cluster = Cluster::default();
        let bucket = assigner
            .assign_bucket(Some(&Bytes::from_static(b"key")), &cluster, 4)
            .expect("bucket");
        assert!((0..4).contains(&bucket));
    }

    #[test]
    fn keyless_assigners_stay_within_a_smaller_routing_count() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        // Metadata lists 8 buckets while the writer routes with 4.
        let cluster = build_cluster(&table_path, 1, 8);
        let physical = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        let round_robin = RoundRobinBucketAssigner::new(Arc::clone(&physical));
        let sticky = StickyBucketAssigner::new(physical);
        for _ in 0..32 {
            let bucket = round_robin
                .assign_bucket(None, &cluster, 4)
                .expect("bucket");
            assert!((0..4).contains(&bucket));
            let bucket = sticky.assign_bucket(None, &cluster, 4).expect("bucket");
            assert!((0..4).contains(&bucket));
            sticky.on_new_batch(&cluster, 4, bucket);
        }
    }

    #[test]
    fn sticky_assigner_moves_off_a_bucket_outside_the_routing_count() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster(&table_path, 1, 8);
        let sticky =
            StickyBucketAssigner::new(Arc::new(PhysicalTablePath::of(Arc::new(table_path))));
        sticky.current_bucket_id.store(6, Ordering::Relaxed);

        let bucket = sticky.assign_bucket(None, &cluster, 4).expect("bucket");
        assert!((0..4).contains(&bucket));
    }

    #[test]
    fn sticky_assigner_keeps_the_only_bucket_within_the_routing_count() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let physical = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));
        let server = ServerNode::new(1, "127.0.0.1".to_string(), 9000, ServerType::TabletServer);
        // Bucket 0 has no leader, so bucket 1 is the only available one below 2.
        let locations: Vec<BucketLocation> = (1..8)
            .map(|bucket| {
                BucketLocation::new(
                    TableBucket::new(1, bucket),
                    Some(server.clone()),
                    Arc::clone(&physical),
                )
            })
            .collect();
        let cluster = Cluster::new(
            None,
            HashMap::from([(1, server)]),
            HashMap::from([(Arc::clone(&physical), locations.clone())]),
            locations
                .into_iter()
                .map(|location| (location.table_bucket.clone(), location))
                .collect(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );
        let sticky = StickyBucketAssigner::new(physical);
        for _ in 0..16 {
            sticky.current_bucket_id.store(1, Ordering::Relaxed);
            sticky.on_new_batch(&cluster, 2, 1);
            assert_eq!(sticky.assign_bucket(None, &cluster, 2).expect("bucket"), 1);
        }
    }

    #[test]
    fn sticky_assigner_without_available_buckets_stays_within_the_routing_count() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let sticky =
            StickyBucketAssigner::new(Arc::new(PhysicalTablePath::of(Arc::new(table_path))));
        let cluster = Cluster::default();
        for _ in 0..16 {
            let bucket = sticky.assign_bucket(None, &cluster, 4).expect("bucket");
            assert!((0..4).contains(&bucket));
            sticky.on_new_batch(&cluster, 4, bucket);
        }
    }

    #[test]
    fn sticky_assigner_stays_within_the_count_while_another_producer_uses_a_larger_one() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = Arc::new(build_cluster(&table_path, 1, 8));
        let sticky = Arc::new(StickyBucketAssigner::new(Arc::new(PhysicalTablePath::of(
            Arc::new(table_path),
        ))));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let provisional = {
            let (cluster, sticky, stop) =
                (Arc::clone(&cluster), Arc::clone(&sticky), Arc::clone(&stop));
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let bucket = sticky.assign_bucket(None, &cluster, 8).expect("bucket");
                    sticky.on_new_batch(&cluster, 8, bucket);
                }
            })
        };
        let mut out_of_range = 0;
        for _ in 0..200_000 {
            let bucket = sticky.assign_bucket(None, &cluster, 2).expect("bucket");
            if !(0..2).contains(&bucket) {
                out_of_range += 1;
            }
            sticky.on_new_batch(&cluster, 2, bucket);
        }
        stop.store(true, Ordering::Relaxed);
        provisional.join().expect("provisional producer");
        assert_eq!(out_of_range, 0);
    }
}
