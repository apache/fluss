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

pub mod murmur_hash;
pub mod partition;
pub mod varint;

use crate::TableId;
use crate::metadata::TableBucket;
use linked_hash_map::LinkedHashMap;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_millis() as i64
}

// Removed: delete_file() is no longer used.
// File cleanup is now handled via RAII with FileCleanupGuard in arrow.rs
// which uses Rust's drop order to ensure files are closed before deletion.

pub struct FairBucketStatusMap<S> {
    map: LinkedHashMap<TableBucket, Arc<S>>,
    size: usize,
}

#[allow(dead_code)]
impl<S> FairBucketStatusMap<S> {
    pub fn new() -> Self {
        Self {
            map: LinkedHashMap::new(),
            size: 0,
        }
    }

    /// Moves the bucket to the end of the iteration order
    pub fn move_to_end(&mut self, table_bucket: TableBucket)
    where
        TableBucket: Eq + Hash,
    {
        if let Some(status) = self.map.remove(&table_bucket) {
            self.map.insert(table_bucket, status);
        }
    }

    /// Updates the status and moves the bucket to the end
    pub fn update_and_move_to_end(&mut self, table_bucket: TableBucket, status: S)
    where
        TableBucket: Eq + Hash,
    {
        self.map.remove(&table_bucket);
        self.map.insert(table_bucket, Arc::new(status));
        self.update_size();
    }

    /// Updates the status without changing the order
    pub fn update(&mut self, table_bucket: TableBucket, status: Arc<S>)
    where
        TableBucket: Eq + Hash,
    {
        self.map.insert(table_bucket, status);
        self.update_size();
    }

    /// Removes a bucket
    pub fn remove(&mut self, table_bucket: &TableBucket)
    where
        TableBucket: Eq + Hash,
    {
        self.map.remove(table_bucket);
        self.update_size();
    }

    /// Returns an immutable view of all buckets
    pub fn bucket_set(&self) -> HashSet<&TableBucket>
    where
        TableBucket: Eq + Hash,
    {
        self.map.keys().collect()
    }

    /// Clears all buckets
    pub fn clear(&mut self) {
        self.map.clear();
        self.update_size();
    }

    /// Checks if a bucket exists
    pub fn contains(&self, table_bucket: &TableBucket) -> bool
    where
        TableBucket: Eq + Hash,
    {
        self.map.contains_key(table_bucket)
    }

    /// Returns an immutable view of the bucket-status map
    pub fn bucket_status_map(&self) -> &LinkedHashMap<TableBucket, Arc<S>> {
        &self.map
    }

    /// Returns status values in current order
    pub fn bucket_status_values(&self) -> Vec<&Arc<S>> {
        self.map.values().collect()
    }

    /// Gets the status for a bucket
    pub fn status_value(&self, table_bucket: &TableBucket) -> Option<&Arc<S>>
    where
        TableBucket: Eq + Hash,
    {
        self.map.get(table_bucket)
    }

    /// Applies a function to each bucket-status pair
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&TableBucket, &S),
    {
        for (bucket, status) in &self.map {
            f(bucket, status);
        }
    }

    /// Gets the current bucket count (thread-safe)
    pub fn size(&self) -> usize {
        self.size
    }

    pub fn set(&mut self, bucket_to_status: HashMap<TableBucket, Arc<S>>)
    where
        TableBucket: Eq + Hash + Clone,
        S: Clone,
    {
        self.map.clear();

        // Group buckets by table ID
        let mut table_to_buckets: LinkedHashMap<TableId, Vec<TableBucket>> = LinkedHashMap::new();
        for bucket in bucket_to_status.keys() {
            table_to_buckets
                .entry(bucket.table_id())
                .or_default()
                .push(bucket.clone());
        }

        // Insert buckets grouped by table
        for (_, buckets) in table_to_buckets {
            for bucket in buckets {
                if let Some(status) = bucket_to_status.get(&bucket) {
                    self.map.insert(bucket, status.clone());
                }
            }
        }

        self.update_size();
    }

    fn update_size(&mut self) {
        self.size = self.map.len()
    }
}

impl<S> Default for FairBucketStatusMap<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn fair_bucket_status_map_tracks_order_and_size() {
        let bucket0 = TableBucket::new(1, 0);
        let bucket1 = TableBucket::new(1, 1);

        let mut map = FairBucketStatusMap::new();
        map.update_and_move_to_end(bucket0.clone(), 10);
        map.update_and_move_to_end(bucket1.clone(), 20);
        assert_eq!(map.size(), 2);

        let values: Vec<i32> = map
            .bucket_status_values()
            .into_iter()
            .map(|value| **value)
            .collect();
        assert_eq!(values, vec![10, 20]);

        map.move_to_end(bucket0.clone());
        let values: Vec<i32> = map
            .bucket_status_values()
            .into_iter()
            .map(|value| **value)
            .collect();
        assert_eq!(values, vec![20, 10]);
    }

    #[test]
    fn fair_bucket_status_map_mutations() {
        let bucket0 = TableBucket::new(1, 0);
        let bucket1 = TableBucket::new(2, 1);

        let mut map = FairBucketStatusMap::new();
        let mut input = HashMap::new();
        input.insert(bucket0.clone(), Arc::new(1));
        input.insert(bucket1.clone(), Arc::new(2));
        map.set(input);

        assert!(map.contains(&bucket0));
        assert!(map.contains(&bucket1));
        assert_eq!(map.bucket_set().len(), 2);

        map.remove(&bucket1);
        assert_eq!(map.size(), 1);

        map.clear();
        assert_eq!(map.size(), 0);
    }
}
