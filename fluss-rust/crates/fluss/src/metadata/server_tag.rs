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

/// Mirrors Java `org.apache.fluss.cluster.rebalance.ServerTag`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerTag {
    PermanentOffline,
    TemporaryOffline,
}

impl ServerTag {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::PermanentOffline => 0,
            Self::TemporaryOffline => 1,
        }
    }

    pub fn try_from_i32(value: i32) -> Result<Self> {
        match value {
            0 => Ok(Self::PermanentOffline),
            1 => Ok(Self::TemporaryOffline),
            _ => Err(Error::IllegalArgument {
                message: format!("Unsupported ServerTag: {value}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_tag_roundtrip() {
        for tag in [ServerTag::PermanentOffline, ServerTag::TemporaryOffline] {
            assert_eq!(ServerTag::try_from_i32(tag.to_i32()).unwrap(), tag);
        }
    }

    #[test]
    fn test_server_tag_unknown() {
        assert!(ServerTag::try_from_i32(99).is_err());
    }
}
