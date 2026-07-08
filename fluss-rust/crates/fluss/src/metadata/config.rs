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
use crate::proto::{PbAlterConfig, PbDescribeConfig};

/// Mirrors Java `org.apache.fluss.config.cluster.AlterConfigOpType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterConfigOpType {
    Set,
    Delete,
    Append,
    Subtract,
}

impl AlterConfigOpType {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::Set => 0,
            Self::Delete => 1,
            Self::Append => 2,
            Self::Subtract => 3,
        }
    }

    pub fn try_from_i32(value: i32) -> Result<Self> {
        match value {
            0 => Ok(Self::Set),
            1 => Ok(Self::Delete),
            2 => Ok(Self::Append),
            3 => Ok(Self::Subtract),
            _ => Err(Error::IllegalArgument {
                message: format!("Unsupported AlterConfigOpType: {value}"),
            }),
        }
    }
}

/// Mirrors Java `org.apache.fluss.config.cluster.AlterConfig`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfig {
    pub config_key: String,
    pub config_value: Option<String>,
    pub op_type: AlterConfigOpType,
}

impl AlterConfig {
    pub fn new<K: Into<String>>(
        config_key: K,
        config_value: Option<String>,
        op_type: AlterConfigOpType,
    ) -> Self {
        Self {
            config_key: config_key.into(),
            config_value,
            op_type,
        }
    }

    pub fn to_pb(&self) -> PbAlterConfig {
        PbAlterConfig {
            config_key: self.config_key.clone(),
            config_value: self.config_value.clone(),
            op_type: self.op_type.to_i32(),
        }
    }

    pub fn from_pb(pb: &PbAlterConfig) -> Result<Self> {
        Ok(Self {
            config_key: pb.config_key.clone(),
            config_value: pb.config_value.clone(),
            op_type: AlterConfigOpType::try_from_i32(pb.op_type)?,
        })
    }
}

/// One entry in the response of `describe_cluster_configs`. Mirrors Java's
/// `org.apache.fluss.config.cluster.DescribeConfig`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfig {
    pub config_key: String,
    pub config_value: Option<String>,
    pub config_source: String,
}

impl DescribeConfig {
    pub fn from_pb(pb: &PbDescribeConfig) -> Self {
        Self {
            config_key: pb.config_key.clone(),
            config_value: pb.config_value.clone(),
            config_source: pb.config_source.clone(),
        }
    }

    pub fn to_pb(&self) -> PbDescribeConfig {
        PbDescribeConfig {
            config_key: self.config_key.clone(),
            config_value: self.config_value.clone(),
            config_source: self.config_source.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alter_config_op_type_roundtrip() {
        for op in [
            AlterConfigOpType::Set,
            AlterConfigOpType::Delete,
            AlterConfigOpType::Append,
            AlterConfigOpType::Subtract,
        ] {
            assert_eq!(AlterConfigOpType::try_from_i32(op.to_i32()).unwrap(), op);
        }
    }

    #[test]
    fn test_alter_config_op_type_unknown() {
        assert!(AlterConfigOpType::try_from_i32(99).is_err());
    }

    #[test]
    fn test_alter_config_pb_roundtrip() {
        let cases = [
            AlterConfig::new("foo", Some("bar".to_string()), AlterConfigOpType::Set),
            AlterConfig::new("baz", None, AlterConfigOpType::Delete),
        ];
        for original in cases {
            let pb = original.to_pb();
            let restored = AlterConfig::from_pb(&pb).unwrap();
            assert_eq!(original, restored);
        }
    }
}
