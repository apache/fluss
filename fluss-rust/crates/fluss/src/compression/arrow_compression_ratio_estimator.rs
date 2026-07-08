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

use std::sync::atomic::{AtomicU32, Ordering};

/// Adaptive estimator for Arrow compression ratios.
///
/// Tracks the ratio between compressed and uncompressed Arrow body sizes.
/// The estimate adjusts asymmetrically: it increases quickly when compression
/// worsens (to avoid underestimating batch sizes) and decreases slowly when
/// compression improves (conservative).
///
/// Thread-safe: uses atomic f32 (stored as u32 bits) matching Java's `volatile float`.
///
/// Matching Java's `ArrowCompressionRatioEstimator`.
pub struct ArrowCompressionRatioEstimator {
    /// Stored as `f32::to_bits()` for atomic access.
    ratio_bits: AtomicU32,
}

const COMPRESSION_RATIO_IMPROVING_STEP: f32 = 0.005;
const COMPRESSION_RATIO_DETERIORATE_STEP: f32 = 0.05;
const DEFAULT_COMPRESSION_RATIO: f32 = 1.0;

impl ArrowCompressionRatioEstimator {
    pub fn new() -> Self {
        Self {
            ratio_bits: AtomicU32::new(DEFAULT_COMPRESSION_RATIO.to_bits()),
        }
    }

    pub fn estimation(&self) -> f32 {
        f32::from_bits(self.ratio_bits.load(Ordering::Relaxed))
    }

    pub fn update_estimation(&self, observed_ratio: f32) {
        let current = self.estimation();
        let new_ratio = if observed_ratio > current {
            (current + COMPRESSION_RATIO_DETERIORATE_STEP).max(observed_ratio)
        } else if observed_ratio < current {
            (current - COMPRESSION_RATIO_IMPROVING_STEP).max(observed_ratio)
        } else {
            return;
        };
        self.ratio_bits
            .store(new_ratio.to_bits(), Ordering::Relaxed);
    }
}

impl Default for ArrowCompressionRatioEstimator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_ratio_is_one() {
        let e = ArrowCompressionRatioEstimator::new();
        assert_eq!(e.estimation(), 1.0);
    }

    #[test]
    fn test_deterioration_jumps_quickly() {
        let e = ArrowCompressionRatioEstimator::new();
        // Observed ratio worse than estimate: jump by at least DETERIORATE_STEP
        e.update_estimation(1.1);
        assert!(e.estimation() >= 1.05);
    }

    #[test]
    fn test_improvement_moves_slowly() {
        let e = ArrowCompressionRatioEstimator::new();
        // Observed ratio better than estimate: move down by at most IMPROVING_STEP
        e.update_estimation(0.5);
        assert!((e.estimation() - 0.995).abs() < 0.001);
    }

    #[test]
    fn test_converges_to_observed() {
        let e = ArrowCompressionRatioEstimator::new();
        // After many updates with same ratio, should converge
        for _ in 0..1000 {
            e.update_estimation(0.7);
        }
        assert!((e.estimation() - 0.7).abs() < 0.01);
    }
}
