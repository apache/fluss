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

/// Mirrors Java `org.apache.fluss.cluster.rebalance.GoalType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalType {
    ReplicaDistribution,
    LeaderDistribution,
    RackAware,
}

impl GoalType {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::ReplicaDistribution => 0,
            Self::LeaderDistribution => 1,
            Self::RackAware => 2,
        }
    }

    pub fn try_from_i32(value: i32) -> Result<Self> {
        match value {
            0 => Ok(Self::ReplicaDistribution),
            1 => Ok(Self::LeaderDistribution),
            2 => Ok(Self::RackAware),
            _ => Err(Error::IllegalArgument {
                message: format!("Unsupported GoalType: {value}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_goal_type_roundtrip() {
        for goal in [
            GoalType::ReplicaDistribution,
            GoalType::LeaderDistribution,
            GoalType::RackAware,
        ] {
            assert_eq!(GoalType::try_from_i32(goal.to_i32()).unwrap(), goal);
        }
    }

    #[test]
    fn test_goal_type_unknown() {
        assert!(GoalType::try_from_i32(99).is_err());
    }
}
