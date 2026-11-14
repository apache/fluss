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

//! Per-table batch size estimator. Mirrors Java's `DynamicWriteBatchSizeEstimator`:
//! grow 10% above 80% fill, shrink 5% below 50%, clamped to `[min, max]`.

use std::sync::atomic::{AtomicUsize, Ordering};

const GROW_THRESHOLD: f64 = 0.8;
const SHRINK_THRESHOLD: f64 = 0.5;
const GROW_FACTOR: f64 = 1.1;
const SHRINK_FACTOR: f64 = 0.95;

#[derive(Debug)]
pub(crate) struct DynamicWriteBatchSizeEstimator {
    current: AtomicUsize,
    min: usize,
    max: usize,
}

impl DynamicWriteBatchSizeEstimator {
    pub fn new(min_size: usize, max_size: usize) -> Self {
        Self {
            current: AtomicUsize::new(max_size),
            min: min_size.min(max_size),
            max: max_size,
        }
    }

    pub fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Last-write-wins on races, matching Java's `ConcurrentHashMap.put`.
    pub fn update(&self, actual: usize) -> usize {
        let prev = self.current.load(Ordering::Relaxed);
        let cur = prev as f64;
        let actual = actual as f64;
        let next = if actual > cur * GROW_THRESHOLD {
            cur * GROW_FACTOR
        } else if actual < cur * SHRINK_THRESHOLD {
            cur * SHRINK_FACTOR
        } else {
            cur
        };
        let clamped = (next as usize).clamp(self.min, self.max);
        if clamped != prev {
            self.current.store(clamped, Ordering::Relaxed);
        }
        clamped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: usize = 256 * 1024;
    const MAX: usize = 2 * 1024 * 1024;
    /// ~41 shrink steps, ~22 grow steps; 50 covers both with margin.
    const CONVERGENCE_STEPS: usize = 50;

    #[test]
    fn starts_at_max() {
        let est = DynamicWriteBatchSizeEstimator::new(MIN, MAX);
        assert_eq!(est.current(), MAX);
    }

    #[test]
    fn min_clamped_to_max_when_misconfigured() {
        let est = DynamicWriteBatchSizeEstimator::new(MAX * 2, MAX);
        assert_eq!(est.current(), MAX);
        assert_eq!(est.update(0), MAX);
    }

    #[test]
    fn grows_when_above_grow_threshold() {
        let est = DynamicWriteBatchSizeEstimator::new(MIN, MAX);
        for _ in 0..CONVERGENCE_STEPS {
            est.update(0);
        }
        assert_eq!(est.current(), MIN);

        // 0.9 sits safely past the 0.8 threshold and avoids f64 boundary noise.
        let next = est.update((MIN as f64 * 0.9) as usize);
        assert_eq!(next, ((MIN as f64) * GROW_FACTOR) as usize);
    }

    #[test]
    fn shrinks_when_below_shrink_threshold() {
        let est = DynamicWriteBatchSizeEstimator::new(MIN, MAX);
        // 0.4 sits safely below the strict 0.5 threshold.
        let next = est.update((MAX as f64 * 0.4) as usize);
        assert_eq!(next, ((MAX as f64) * SHRINK_FACTOR) as usize);
    }

    #[test]
    fn shrink_clamps_to_min() {
        let est = DynamicWriteBatchSizeEstimator::new(MIN, MAX);
        for _ in 0..CONVERGENCE_STEPS {
            est.update(0);
        }
        assert_eq!(est.current(), MIN);
    }

    #[test]
    fn grow_clamps_to_max() {
        let est = DynamicWriteBatchSizeEstimator::new(MIN, MAX);
        for _ in 0..CONVERGENCE_STEPS {
            est.update(0);
        }
        for _ in 0..CONVERGENCE_STEPS {
            est.update(est.current());
        }
        assert_eq!(est.current(), MAX);
    }

    #[test]
    fn oversized_actual_clamps_at_max() {
        let est = DynamicWriteBatchSizeEstimator::new(MIN, MAX);
        assert_eq!(est.update(MAX * 4), MAX);
    }

    #[test]
    fn dead_zone_is_a_fixed_point() {
        let est = DynamicWriteBatchSizeEstimator::new(MIN, MAX);
        let initial = est.current();
        for _ in 0..20 {
            est.update((est.current() as f64 * 0.65) as usize);
        }
        assert_eq!(est.current(), initial);
    }
}
