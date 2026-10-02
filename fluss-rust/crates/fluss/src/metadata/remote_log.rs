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

use crate::metadata::TableBucket;
use crate::proto::PbRemoteLogManifestEntry;

/// One bucket's remote-log manifest pointer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteLogManifestEntry {
    pub table_bucket: TableBucket,
    pub remote_log_manifest_path: String,
    pub remote_log_end_offset: i64,
}

impl RemoteLogManifestEntry {
    pub fn from_pb(pb: &PbRemoteLogManifestEntry) -> Self {
        Self {
            table_bucket: TableBucket::from_pb(&pb.table_bucket),
            remote_log_manifest_path: pb.remote_log_manifest_path.clone(),
            remote_log_end_offset: pb.remote_log_end_offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::PbTableBucket;

    #[test]
    fn test_remote_log_manifest_entry_from_pb() {
        let pb = PbRemoteLogManifestEntry {
            table_bucket: PbTableBucket {
                table_id: 1,
                partition_id: None,
                bucket_id: 2,
            },
            remote_log_manifest_path: "s3://bucket/manifest.json".to_string(),
            remote_log_end_offset: 999,
        };
        let m = RemoteLogManifestEntry::from_pb(&pb);
        assert_eq!(m.remote_log_end_offset, 999);
        assert_eq!(m.table_bucket.bucket_id(), 2);
    }
}
