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

use crate::error::Error;
use crate::metadata::{
    AddColumn, AlterConfig, AlterTableChanges, DropColumn, ModifyColumn, RenameColumn, TablePath,
};
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::convert::to_table_path;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct AlterTableRequest {
    pub(crate) inner_request: proto::AlterTableRequest,
}

impl AlterTableRequest {
    pub fn new(
        table_path: &TablePath,
        ignore_if_not_exists: bool,
        changes: AlterTableChanges,
    ) -> Self {
        AlterTableRequest {
            inner_request: proto::AlterTableRequest {
                table_path: to_table_path(table_path),
                ignore_if_not_exists,
                config_changes: changes
                    .config_changes
                    .iter()
                    .map(AlterConfig::to_pb)
                    .collect(),
                add_columns: changes.add_columns.iter().map(AddColumn::to_pb).collect(),
                drop_columns: changes.drop_columns.iter().map(DropColumn::to_pb).collect(),
                rename_columns: changes
                    .rename_columns
                    .iter()
                    .map(RenameColumn::to_pb)
                    .collect(),
                modify_columns: changes
                    .modify_columns
                    .iter()
                    .map(ModifyColumn::to_pb)
                    .collect(),
                modify_bucket_count: changes
                    .new_bucket_count
                    .map(|new_bucket_count| proto::PbModifyBucketCount { new_bucket_count }),
            },
        }
    }
}

/// Older servers ignore `modify_bucket_count` and still report success.
const MODIFY_BUCKET_COUNT_MIN_VERSION: ApiVersion = ApiVersion(1);

impl RequestBody for AlterTableRequest {
    type ResponseBody = proto::AlterTableResponse;
    const API_KEY: ApiKey = ApiKey::AlterTable;

    fn check_version(&self, version: ApiVersion) -> Result<(), Error> {
        if self.inner_request.modify_bucket_count.is_some()
            && version < MODIFY_BUCKET_COUNT_MIN_VERSION
        {
            return Err(Error::UnsupportedVersion {
                message: format!(
                    "Modifying the bucket count requires ALTER_TABLE version {} or newer, but the server negotiated version {}.",
                    MODIFY_BUCKET_COUNT_MIN_VERSION.0, version.0
                ),
            });
        }
        Ok(())
    }
}

impl_write_type!(AlterTableRequest);
impl_read_type!(proto::AlterTableResponse);

#[cfg(test)]
mod tests {
    use super::*;

    fn request(new_bucket_count: Option<i32>) -> AlterTableRequest {
        AlterTableRequest::new(
            &TablePath::new("db", "tbl"),
            false,
            AlterTableChanges {
                new_bucket_count,
                ..Default::default()
            },
        )
    }

    #[test]
    fn bucket_count_change_needs_alter_table_v1() {
        let request = request(Some(4));
        assert!(matches!(
            request.check_version(ApiVersion(0)),
            Err(Error::UnsupportedVersion { .. })
        ));
        assert!(request.check_version(ApiVersion(1)).is_ok());
    }

    #[test]
    fn other_changes_still_work_on_alter_table_v0() {
        assert!(request(None).check_version(ApiVersion(0)).is_ok());
    }
}
