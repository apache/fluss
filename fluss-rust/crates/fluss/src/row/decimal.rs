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

use crate::error::{Error, Result};
use bigdecimal::num_bigint::BigInt;
use bigdecimal::num_traits::Zero;
use bigdecimal::{BigDecimal, RoundingMode};
use std::fmt;

#[cfg(test)]
use std::str::FromStr;

/// Maximum decimal precision that can be stored compactly as a single i64.
/// Values with precision > MAX_COMPACT_PRECISION require byte array storage.
pub const MAX_COMPACT_PRECISION: u32 = 18;

/// An internal data structure representing a decimal value with fixed precision and scale.
///
/// This data structure is immutable and stores decimal values in a compact representation
/// (as a long value) if values are small enough (precision ≤ 18).
///
/// Matches Java's org.apache.fluss.row.Decimal class.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Decimal {
    precision: u32,
    scale: u32,
    // If precision <= MAX_COMPACT_PRECISION, this holds the unscaled value
    long_val: Option<i64>,
    // BigDecimal representation (may be cached)
    decimal_val: Option<BigDecimal>,
}

impl Decimal {
    /// Returns the precision of this Decimal.
    ///
    /// The precision is the number of digits in the unscaled value.
    pub fn precision(&self) -> u32 {
        self.precision
    }

    /// Returns the scale of this Decimal.
    pub fn scale(&self) -> u32 {
        self.scale
    }

    /// Returns whether the decimal value is small enough to be stored in a long.
    pub fn is_compact(&self) -> bool {
        self.precision <= MAX_COMPACT_PRECISION
    }

    /// Returns whether a given precision can be stored compactly.
    pub fn is_compact_precision(precision: u32) -> bool {
        precision <= MAX_COMPACT_PRECISION
    }

    /// Converts this Decimal into a BigDecimal.
    pub fn to_big_decimal(&self) -> BigDecimal {
        if let Some(bd) = &self.decimal_val {
            bd.clone()
        } else if let Some(long_val) = self.long_val {
            BigDecimal::new(BigInt::from(long_val), self.scale as i64)
        } else {
            // Should never happen - we always have one representation
            BigDecimal::new(BigInt::from(0), self.scale as i64)
        }
    }

    /// Returns a long describing the unscaled value of this Decimal.
    pub fn to_unscaled_long(&self) -> Result<i64> {
        if let Some(long_val) = self.long_val {
            Ok(long_val)
        } else {
            // Extract unscaled value from BigDecimal
            let bd = self.to_big_decimal();
            let (unscaled, _) = bd.as_bigint_and_exponent();
            unscaled.try_into().map_err(|_| Error::IllegalArgument {
                message: format!(
                    "Decimal unscaled value does not fit in i64: precision={}",
                    self.precision
                ),
            })
        }
    }

    /// Returns a byte array describing the unscaled value of this Decimal.
    pub fn to_unscaled_bytes(&self) -> Vec<u8> {
        let bd = self.to_big_decimal();
        let (unscaled, _) = bd.as_bigint_and_exponent();
        unscaled.to_signed_bytes_be()
    }

    /// Creates a Decimal from Arrow's Decimal128 representation.
    // TODO: For compact decimals with matching scale we may call from_unscaled_long
    pub fn from_arrow_decimal128(
        i128_val: i128,
        arrow_scale: i64,
        precision: u32,
        scale: u32,
    ) -> Result<Self> {
        let bd = BigDecimal::new(BigInt::from(i128_val), arrow_scale);
        Self::from_big_decimal(bd, precision, scale)
    }

    /// Creates an instance of Decimal from a BigDecimal with the given precision and scale.
    ///
    /// The returned decimal value may be rounded to have the desired scale. The precision
    /// will be checked. If the precision overflows, an error is returned.
    pub fn from_big_decimal(bd: BigDecimal, precision: u32, scale: u32) -> Result<Self> {
        // Rescale to the target scale with HALF_UP rounding (matches Java)
        let scaled = bd.with_scale_round(scale as i64, RoundingMode::HalfUp);

        // Extract unscaled value
        let (unscaled, exp) = scaled.as_bigint_and_exponent();

        // Sanity check that scale matches
        debug_assert_eq!(
            exp, scale as i64,
            "Scaled decimal exponent ({exp}) != expected scale ({scale})"
        );

        let actual_precision = Self::compute_precision(&unscaled);
        if actual_precision > precision as usize {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Decimal precision overflow: value has {actual_precision} digits but precision is {precision} (value: {scaled})"
                ),
            });
        }

        // Compute compact representation if possible
        let long_val = if precision <= MAX_COMPACT_PRECISION {
            Some(i64::try_from(&unscaled).map_err(|_| Error::IllegalArgument {
                message: format!(
                    "Decimal mantissa exceeds i64 range for compact precision {precision}: unscaled={unscaled} (value={scaled})"
                ),
            })?)
        } else {
            None
        };

        Ok(Decimal {
            precision,
            scale,
            long_val,
            decimal_val: Some(scaled),
        })
    }

    /// Creates an instance of Decimal from an unscaled long value with the given precision and scale.
    pub fn from_unscaled_long(unscaled_long: i64, precision: u32, scale: u32) -> Result<Self> {
        if precision > MAX_COMPACT_PRECISION {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Precision {precision} exceeds MAX_COMPACT_PRECISION ({MAX_COMPACT_PRECISION})"
                ),
            });
        }

        let actual_precision = Self::compute_precision(&BigInt::from(unscaled_long));
        if actual_precision > precision as usize {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Decimal precision overflow: unscaled value has {actual_precision} digits but precision is {precision}"
                ),
            });
        }

        Ok(Decimal {
            precision,
            scale,
            long_val: Some(unscaled_long),
            decimal_val: None,
        })
    }

    /// Creates an instance of Decimal from an unscaled byte array with the given precision and scale.
    pub fn from_unscaled_bytes(unscaled_bytes: &[u8], precision: u32, scale: u32) -> Result<Self> {
        let unscaled = BigInt::from_signed_bytes_be(unscaled_bytes);
        let bd = BigDecimal::new(unscaled, scale as i64);
        Self::from_big_decimal(bd, precision, scale)
    }

    /// Computes the precision of a decimal's unscaled value, matching Java's BigDecimal.precision().
    pub fn compute_precision(unscaled: &BigInt) -> usize {
        if unscaled.is_zero() {
            return 1;
        }

        // Count ALL digits in the unscaled value (matches Java's BigDecimal.precision())
        // For bounded precision (≤ 38 digits), string conversion is cheap and simple.
        unscaled.magnitude().to_str_radix(10).len()
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_big_decimal())
    }
}

// Manual implementations of comparison traits to ignore cached fields
impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        // Use numeric equality like Java's Decimal.equals() which delegates to compareTo.
        // This means 1.0 (scale=1) equals 1.00 (scale=2).
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for Decimal {}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // If both are compact and have the same scale, compare directly
        if self.is_compact() && other.is_compact() && self.scale == other.scale {
            self.long_val.cmp(&other.long_val)
        } else {
            // Otherwise, compare as BigDecimal
            self.to_big_decimal().cmp(&other.to_big_decimal())
        }
    }
}

impl std::hash::Hash for Decimal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the BigDecimal representation.
        //
        // IMPORTANT: Unlike Java's BigDecimal, Rust's bigdecimal crate normalizes
        // before hashing, so hash(1.0) == hash(1.00). Combined with our numeric
        // equality (1.0 == 1.00), this CORRECTLY satisfies the hash/equals contract.
        //
        // This is BETTER than Java's implementation which has a hash/equals violation:
        // - Java: equals(1.0, 1.00) = true, but hashCode(1.0) != hashCode(1.00)
        // - Rust: equals(1.0, 1.00) = true, and hash(1.0) == hash(1.00) ✓
        //
        // Result: HashMap/HashSet will work correctly even if you create Decimals
        // with different scales for the same numeric value (though this is rare in
        // practice since decimals are schema-driven with fixed precision/scale).
        self.to_big_decimal().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_precision_calculation() {
        // Zero is special case
        assert_eq!(Decimal::compute_precision(&BigInt::from(0)), 1);

        // Must count ALL digits including trailing zeros (matches Java BigDecimal.precision())
        assert_eq!(Decimal::compute_precision(&BigInt::from(10)), 2);
        assert_eq!(Decimal::compute_precision(&BigInt::from(100)), 3);
        assert_eq!(Decimal::compute_precision(&BigInt::from(12300)), 5);
        assert_eq!(
            Decimal::compute_precision(&BigInt::from(10000000000i64)),
            11
        );

        // Test the case: value=1, scale=10 → unscaled=10000000000 (11 digits)
        let bd = BigDecimal::new(BigInt::from(1), 0);
        assert!(
            Decimal::from_big_decimal(bd.clone(), 1, 10).is_err(),
            "Should reject: unscaled 10000000000 has 11 digits, precision=1 is too small"
        );
        assert!(
            Decimal::from_big_decimal(bd, 11, 10).is_ok(),
            "Should accept with correct precision=11"
        );
    }

    /// Test precision validation boundaries
    #[test]
    fn test_precision_validation() {
        let test_cases = vec![
            (10i64, 1, 2),            // 1.0 → unscaled: 10 (2 digits)
            (100i64, 2, 3),           // 1.00 → unscaled: 100 (3 digits)
            (10000000000i64, 10, 11), // 1.0000000000 → unscaled: 10000000000 (11 digits)
        ];

        for (unscaled, scale, min_precision) in test_cases {
            let bd = BigDecimal::new(BigInt::from(unscaled), scale as i64);

            // Reject if precision too small
            assert!(Decimal::from_big_decimal(bd.clone(), min_precision - 1, scale).is_err());
            // Accept with correct precision
            assert!(Decimal::from_big_decimal(bd, min_precision, scale).is_ok());
        }

        // i64::MAX has 19 digits, should reject with precision=5
        let bd = BigDecimal::new(BigInt::from(i64::MAX), 0);
        assert!(Decimal::from_big_decimal(bd, 5, 0).is_err());
    }

    /// Test creation and basic operations for both compact and non-compact decimals
    #[test]
    fn test_creation_and_representation() {
        // Compact (precision ≤ 18): from unscaled long
        let compact = Decimal::from_unscaled_long(12345, 10, 2).unwrap();
        assert_eq!(compact.precision(), 10);
        assert_eq!(compact.scale(), 2);
        assert!(compact.is_compact());
        assert_eq!(compact.to_unscaled_long().unwrap(), 12345);
        assert_eq!(compact.to_big_decimal().to_string(), "123.45");

        // Non-compact (precision > 18): from BigDecimal
        let bd = BigDecimal::new(BigInt::from(12345), 0);
        let non_compact = Decimal::from_big_decimal(bd, 28, 0).unwrap();
        assert_eq!(non_compact.precision(), 28);
        assert!(!non_compact.is_compact());
        assert_eq!(
            non_compact.to_unscaled_bytes(),
            BigInt::from(12345).to_signed_bytes_be()
        );

        // Test compact boundary
        assert!(Decimal::is_compact_precision(18));
        assert!(!Decimal::is_compact_precision(19));

        // Test rounding during creation
        let bd = BigDecimal::new(BigInt::from(12345), 3); // 12.345
        let rounded = Decimal::from_big_decimal(bd, 10, 2).unwrap();
        assert_eq!(rounded.to_unscaled_long().unwrap(), 1235); // 12.35
    }

    /// Test serialization round-trip (unscaled bytes)
    #[test]
    fn test_serialization_roundtrip() {
        // Compact decimal
        let bd1 = BigDecimal::new(BigInt::from(1314567890123i64), 5); // 13145678.90123
        let decimal1 = Decimal::from_big_decimal(bd1.clone(), 15, 5).unwrap();
        let (unscaled1, _) = bd1.as_bigint_and_exponent();
        let from_bytes1 =
            Decimal::from_unscaled_bytes(&unscaled1.to_signed_bytes_be(), 15, 5).unwrap();
        assert_eq!(from_bytes1, decimal1);
        assert_eq!(
            from_bytes1.to_unscaled_bytes(),
            unscaled1.to_signed_bytes_be()
        );

        // Non-compact decimal
        let bd2 = BigDecimal::new(BigInt::from(12345678900987654321i128), 10);
        let decimal2 = Decimal::from_big_decimal(bd2.clone(), 23, 10).unwrap();
        let (unscaled2, _) = bd2.as_bigint_and_exponent();
        let from_bytes2 =
            Decimal::from_unscaled_bytes(&unscaled2.to_signed_bytes_be(), 23, 10).unwrap();
        assert_eq!(from_bytes2, decimal2);
        assert_eq!(
            from_bytes2.to_unscaled_bytes(),
            unscaled2.to_signed_bytes_be()
        );
    }

    /// Test numeric equality and ordering (matches Java semantics)
    #[test]
    fn test_equality_and_ordering() {
        // Same value, different precision/scale → should be equal (numeric equality)
        let d1 = Decimal::from_big_decimal(BigDecimal::new(BigInt::from(10), 1), 2, 1).unwrap(); // 1.0
        let d2 = Decimal::from_big_decimal(BigDecimal::new(BigInt::from(100), 2), 3, 2).unwrap(); // 1.00
        assert_eq!(d1, d2, "Numeric equality: 1.0 == 1.00");
        assert_eq!(d1.cmp(&d2), std::cmp::Ordering::Equal);

        // Test ordering with positive values
        let small = Decimal::from_unscaled_long(10, 5, 0).unwrap();
        let large = Decimal::from_unscaled_long(15, 5, 0).unwrap();
        assert!(small < large);
        assert_eq!(small.cmp(&large), std::cmp::Ordering::Less);

        // Test ordering with negative values
        let negative_large = Decimal::from_unscaled_long(-10, 5, 0).unwrap(); // -10
        let negative_small = Decimal::from_unscaled_long(-15, 5, 0).unwrap(); // -15
        assert!(negative_small < negative_large); // -15 < -10
        assert_eq!(
            negative_small.cmp(&negative_large),
            std::cmp::Ordering::Less
        );

        // Test ordering with mixed positive and negative
        let positive = Decimal::from_unscaled_long(5, 5, 0).unwrap();
        let negative = Decimal::from_unscaled_long(-5, 5, 0).unwrap();
        assert!(negative < positive);
        assert_eq!(negative.cmp(&positive), std::cmp::Ordering::Less);

        // Test clone and round-trip equality
        let original = Decimal::from_unscaled_long(10, 5, 0).unwrap();
        assert_eq!(original.clone(), original);
        assert_eq!(
            Decimal::from_unscaled_long(original.to_unscaled_long().unwrap(), 5, 0).unwrap(),
            original
        );
    }

    /// Test hash/equals contract (Rust implementation is correct, unlike Java)
    #[test]
    fn test_hash_equals_contract() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let d1 = Decimal::from_big_decimal(BigDecimal::new(BigInt::from(10), 1), 2, 1).unwrap(); // 1.0
        let d2 = Decimal::from_big_decimal(BigDecimal::new(BigInt::from(100), 2), 3, 2).unwrap(); // 1.00

        // Numeric equality
        assert_eq!(d1, d2);

        // Hash contract: if a == b, then hash(a) == hash(b)
        let mut hasher1 = DefaultHasher::new();
        d1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        d2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2, "Equal decimals must have equal hashes");

        // Verify HashMap works correctly (this would fail in Java due to their hash/equals bug)
        let mut map = std::collections::HashMap::new();
        map.insert(d1.clone(), "value");
        assert_eq!(map.get(&d2), Some(&"value"));
    }

    /// Test edge cases: zeros, large numbers, rescaling
    #[test]
    fn test_edge_cases() {
        // Zero handling (compact and non-compact)
        let zero_compact = Decimal::from_unscaled_long(0, 5, 2).unwrap();
        assert_eq!(
            zero_compact.to_big_decimal(),
            BigDecimal::new(BigInt::from(0), 2)
        );

        let zero_non_compact =
            Decimal::from_big_decimal(BigDecimal::new(BigInt::from(0), 2), 20, 2).unwrap();
        assert_eq!(
            zero_non_compact.to_big_decimal(),
            BigDecimal::new(BigInt::from(0), 2)
        );

        // Large number (39 digits)
        let large_bd = BigDecimal::from_str("123456789012345678901234567890123456789").unwrap();
        let large = Decimal::from_big_decimal(large_bd, 39, 0).unwrap();
        let double_val = large.to_big_decimal().to_string().parse::<f64>().unwrap();
        assert!((double_val - 1.2345678901234568E38).abs() < 0.01);

        // Rescaling: 5.0 (scale=1) → 5.00 (scale=2)
        let d1 = Decimal::from_big_decimal(BigDecimal::new(BigInt::from(50), 1), 10, 1).unwrap();
        let d2 = Decimal::from_big_decimal(d1.to_big_decimal(), 10, 2).unwrap();
        assert_eq!(d2.to_big_decimal().to_string(), "5.00");
        assert_eq!(d2.scale(), 2);
    }
}
