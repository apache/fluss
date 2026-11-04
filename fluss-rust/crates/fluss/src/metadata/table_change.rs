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

use super::AlterConfig;
use crate::error::{Error, Result};
use crate::proto::{PbAddColumn, PbDropColumn, PbModifyColumn, PbRenameColumn};

/// Mirrors Java `org.apache.fluss.config.cluster.ColumnPositionType`.
///
/// The Java server today only handles `Last`; `First` and `After` are reserved by
/// the proto schema (`column_position_type = 0=LAST, 1=FIRST, 3=AFTER`) for future
/// use. Sending other variants will be rejected by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnPositionType {
    Last,
}

impl ColumnPositionType {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::Last => 0,
        }
    }

    pub fn try_from_i32(value: i32) -> Result<Self> {
        match value {
            0 => Ok(Self::Last),
            _ => Err(Error::IllegalArgument {
                message: format!("Unsupported ColumnPositionType: {value}"),
            }),
        }
    }
}

/// Add a column. Mirrors the `AddColumn` variant of Java `TableChange`.
///
/// `data_type_json` carries the column's `DataType` already serialized to its JSON
/// representation (matching the Java client, which transports `DataType` as JSON over
/// the wire). Callers can produce it via `DataType::serialize_json` + `serde_json::to_vec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddColumn {
    pub column_name: String,
    pub data_type_json: Vec<u8>,
    pub comment: Option<String>,
    pub position: ColumnPositionType,
}

impl AddColumn {
    pub fn to_pb(&self) -> PbAddColumn {
        PbAddColumn {
            column_name: self.column_name.clone(),
            data_type_json: self.data_type_json.clone(),
            comment: self.comment.clone(),
            column_position_type: self.position.to_i32(),
            // aggregation columns (added server-side in #3459) aren't modeled by the
            // client AddColumn API yet; a plain ADD COLUMN carries no aggregation.
            agg_function_type: None,
            agg_function_params: Vec::new(),
        }
    }

    pub fn from_pb(pb: &PbAddColumn) -> Result<Self> {
        Ok(Self {
            column_name: pb.column_name.clone(),
            data_type_json: pb.data_type_json.clone(),
            comment: pb.comment.clone(),
            position: ColumnPositionType::try_from_i32(pb.column_position_type)?,
        })
    }
}

/// Drop a column. Mirrors the `DropColumn` variant of Java `TableChange`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropColumn {
    pub column_name: String,
}

impl DropColumn {
    pub fn to_pb(&self) -> PbDropColumn {
        PbDropColumn {
            column_name: self.column_name.clone(),
        }
    }

    pub fn from_pb(pb: &PbDropColumn) -> Self {
        Self {
            column_name: pb.column_name.clone(),
        }
    }
}

/// Rename a column. Mirrors the `RenameColumn` variant of Java `TableChange`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameColumn {
    pub old_column_name: String,
    pub new_column_name: String,
}

impl RenameColumn {
    pub fn to_pb(&self) -> PbRenameColumn {
        PbRenameColumn {
            old_column_name: self.old_column_name.clone(),
            new_column_name: self.new_column_name.clone(),
        }
    }

    pub fn from_pb(pb: &PbRenameColumn) -> Self {
        Self {
            old_column_name: pb.old_column_name.clone(),
            new_column_name: pb.new_column_name.clone(),
        }
    }
}

/// Bundle of changes for a single `alter_table` call. Empty `Vec`s and `None`
/// mean "no change of that kind"; pass `Default::default()` to send only
/// config changes.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AlterTableChanges {
    pub config_changes: Vec<AlterConfig>,
    pub add_columns: Vec<AddColumn>,
    pub drop_columns: Vec<DropColumn>,
    pub rename_columns: Vec<RenameColumn>,
    pub modify_columns: Vec<ModifyColumn>,
    /// A new default bucket count for a partitioned table. Existing partitions keep their
    /// bucket counts; partitions created afterwards use this one.
    pub new_bucket_count: Option<i32>,
}

impl AlterTableChanges {
    /// Rejects changes the server would reject or silently ignore, before sending them.
    pub fn validate(&self) -> Result<()> {
        // The server stores it as a custom property and keeps the bucket count.
        if self
            .config_changes
            .iter()
            .any(|change| change.config_key == "bucket.num")
        {
            return Err(Error::IllegalArgument {
                message: "'bucket.num' is not a table property; set new_bucket_count to change \
                          the bucket count."
                    .to_string(),
            });
        }
        if let Some(bucket_count) = self.new_bucket_count {
            if bucket_count <= 0 {
                return Err(Error::IllegalArgument {
                    message: format!("Bucket count must be positive, but was {bucket_count}."),
                });
            }
        }
        Ok(())
    }
}

/// Modify a column's type/comment/position. Mirrors the `ModifyColumn` variant of
/// Java `TableChange`. All fields except `column_name` are optional — only the
/// non-`None` ones are applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyColumn {
    pub column_name: String,
    pub data_type_json: Option<Vec<u8>>,
    pub comment: Option<String>,
    pub position: Option<ColumnPositionType>,
}

impl ModifyColumn {
    pub fn to_pb(&self) -> PbModifyColumn {
        PbModifyColumn {
            column_name: self.column_name.clone(),
            data_type_json: self.data_type_json.clone(),
            comment: self.comment.clone(),
            column_position_type: self.position.map(|p| p.to_i32()),
        }
    }

    pub fn from_pb(pb: &PbModifyColumn) -> Result<Self> {
        Ok(Self {
            column_name: pb.column_name.clone(),
            data_type_json: pb.data_type_json.clone(),
            comment: pb.comment.clone(),
            position: pb
                .column_position_type
                .map(ColumnPositionType::try_from_i32)
                .transpose()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::AlterConfigOpType;

    #[test]
    fn bucket_num_is_not_a_config_change() {
        for op_type in [AlterConfigOpType::Set, AlterConfigOpType::Delete] {
            let changes = AlterTableChanges {
                config_changes: vec![AlterConfig::new(
                    "bucket.num",
                    Some("8".to_string()),
                    op_type,
                )],
                ..Default::default()
            };
            assert!(matches!(
                changes.validate(),
                Err(Error::IllegalArgument { message }) if message.contains("new_bucket_count")
            ));
        }
    }

    #[test]
    fn a_bucket_count_change_must_be_positive() {
        let changes = |new_bucket_count| AlterTableChanges {
            new_bucket_count,
            ..Default::default()
        };
        assert!(changes(Some(4)).validate().is_ok());
        assert!(changes(None).validate().is_ok());
        for bucket_count in [0, -1] {
            assert!(matches!(
                changes(Some(bucket_count)).validate(),
                Err(Error::IllegalArgument { .. })
            ));
        }
    }

    #[test]
    fn test_column_position_type_roundtrip() {
        assert_eq!(
            ColumnPositionType::try_from_i32(ColumnPositionType::Last.to_i32()).unwrap(),
            ColumnPositionType::Last
        );
        assert!(ColumnPositionType::try_from_i32(1).is_err());
        assert!(ColumnPositionType::try_from_i32(3).is_err());
    }

    #[test]
    fn test_add_column_pb_roundtrip() {
        let original = AddColumn {
            column_name: "amount".to_string(),
            data_type_json: b"{\"type\":\"BIGINT\"}".to_vec(),
            comment: Some("the amount".to_string()),
            position: ColumnPositionType::Last,
        };
        let pb = original.to_pb();
        assert_eq!(AddColumn::from_pb(&pb).unwrap(), original);
    }

    #[test]
    fn test_drop_column_pb_roundtrip() {
        let original = DropColumn {
            column_name: "obsolete".to_string(),
        };
        let pb = original.to_pb();
        assert_eq!(DropColumn::from_pb(&pb), original);
    }

    #[test]
    fn test_rename_column_pb_roundtrip() {
        let original = RenameColumn {
            old_column_name: "old_name".to_string(),
            new_column_name: "new_name".to_string(),
        };
        let pb = original.to_pb();
        assert_eq!(RenameColumn::from_pb(&pb), original);
    }

    #[test]
    fn test_modify_column_pb_roundtrip_full() {
        let original = ModifyColumn {
            column_name: "x".to_string(),
            data_type_json: Some(b"{\"type\":\"DOUBLE\"}".to_vec()),
            comment: Some("changed".to_string()),
            position: Some(ColumnPositionType::Last),
        };
        let pb = original.to_pb();
        assert_eq!(ModifyColumn::from_pb(&pb).unwrap(), original);
    }

    #[test]
    fn test_modify_column_pb_roundtrip_empty() {
        let original = ModifyColumn {
            column_name: "x".to_string(),
            data_type_json: None,
            comment: None,
            position: None,
        };
        let pb = original.to_pb();
        assert_eq!(ModifyColumn::from_pb(&pb).unwrap(), original);
    }
}
