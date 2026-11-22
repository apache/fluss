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

/// Mirrors Java `org.apache.fluss.client.admin.RegisterResult`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterProducerResult {
    /// Snapshot was newly created (first-startup scenario; no undo recovery needed).
    Created,
    /// Snapshot already existed (failover scenario; caller should perform undo recovery).
    AlreadyExists,
}

impl RegisterProducerResult {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::Created => 0,
            Self::AlreadyExists => 1,
        }
    }

    pub fn try_from_i32(value: i32) -> Result<Self> {
        match value {
            0 => Ok(Self::Created),
            1 => Ok(Self::AlreadyExists),
            _ => Err(Error::IllegalArgument {
                message: format!("Unsupported RegisterProducerResult: {value}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_producer_result_roundtrip() {
        for r in [
            RegisterProducerResult::Created,
            RegisterProducerResult::AlreadyExists,
        ] {
            assert_eq!(RegisterProducerResult::try_from_i32(r.to_i32()).unwrap(), r);
        }
    }

    #[test]
    fn test_register_producer_result_unknown() {
        assert!(RegisterProducerResult::try_from_i32(99).is_err());
    }
}
