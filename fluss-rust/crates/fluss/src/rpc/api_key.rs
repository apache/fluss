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

use crate::rpc::api_key::ApiKey::Unknown;
use crate::rpc::api_version::{ApiVersion, ApiVersionRange};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum ApiKey {
    ApiVersion,                 // 1000
    CreateDatabase,             // 1001
    DropDatabase,               // 1002
    ListDatabases,              // 1003
    DatabaseExists,             // 1004
    CreateTable,                // 1005
    DropTable,                  // 1006
    GetTable,                   // 1007
    ListTables,                 // 1008
    ListPartitionInfos,         // 1009
    TableExists,                // 1010
    GetTableSchema,             // 1011
    MetaData,                   // 1012
    ProduceLog,                 // 1014
    FetchLog,                   // 1015
    PutKv,                      // 1016
    Lookup,                     // 1017
    ListOffsets,                // 1021
    GetFileSystemSecurityToken, // 1025
    InitWriter,                 // 1026
    GetLatestLakeSnapshot,      // 1032
    LimitScan,                  // 1033
    PrefixLookup,               // 1034
    GetDatabaseInfo,            // 1035
    CreatePartition,            // 1036
    DropPartition,              // 1037
    Authenticate,               // 1038
    Unknown(i16),
}

impl ApiKey {
    /// Returns the range of versions supported by the client for this API key.
    pub fn supported_versions(&self) -> Option<ApiVersionRange> {
        match self {
            // Most APIs only support v0.
            ApiKey::ApiVersion
            | ApiKey::CreateDatabase
            | ApiKey::DropDatabase
            | ApiKey::ListDatabases
            | ApiKey::DatabaseExists
            | ApiKey::CreateTable
            | ApiKey::DropTable
            | ApiKey::GetTable
            | ApiKey::ListTables
            | ApiKey::ListPartitionInfos
            | ApiKey::TableExists
            | ApiKey::GetTableSchema
            | ApiKey::MetaData
            | ApiKey::ProduceLog
            | ApiKey::FetchLog
            | ApiKey::ListOffsets
            | ApiKey::GetFileSystemSecurityToken
            | ApiKey::InitWriter
            | ApiKey::GetLatestLakeSnapshot
            | ApiKey::LimitScan
            | ApiKey::GetDatabaseInfo
            | ApiKey::CreatePartition
            | ApiKey::DropPartition
            | ApiKey::Authenticate
            // TODO(key-encoding-v1): The Java server supports v0..v1 for these
            // APIs, but the Rust client has not yet implemented the v1 key
            // encoding format. Pinned to v0 until that is done.
            | ApiKey::PutKv | ApiKey::Lookup | ApiKey::PrefixLookup => {
                Some(ApiVersionRange::new(ApiVersion(0), ApiVersion(0)))
            }
            Unknown(_) => None,
        }
    }
}

impl From<i16> for ApiKey {
    fn from(key: i16) -> Self {
        match key {
            1000 => ApiKey::ApiVersion,
            1001 => ApiKey::CreateDatabase,
            1002 => ApiKey::DropDatabase,
            1003 => ApiKey::ListDatabases,
            1004 => ApiKey::DatabaseExists,
            1005 => ApiKey::CreateTable,
            1006 => ApiKey::DropTable,
            1007 => ApiKey::GetTable,
            1008 => ApiKey::ListTables,
            1009 => ApiKey::ListPartitionInfos,
            1010 => ApiKey::TableExists,
            1011 => ApiKey::GetTableSchema,
            1012 => ApiKey::MetaData,
            1014 => ApiKey::ProduceLog,
            1015 => ApiKey::FetchLog,
            1016 => ApiKey::PutKv,
            1017 => ApiKey::Lookup,
            1021 => ApiKey::ListOffsets,
            1025 => ApiKey::GetFileSystemSecurityToken,
            1026 => ApiKey::InitWriter,
            1032 => ApiKey::GetLatestLakeSnapshot,
            1033 => ApiKey::LimitScan,
            1034 => ApiKey::PrefixLookup,
            1035 => ApiKey::GetDatabaseInfo,
            1036 => ApiKey::CreatePartition,
            1037 => ApiKey::DropPartition,
            1038 => ApiKey::Authenticate,

            _ => Unknown(key),
        }
    }
}

impl From<ApiKey> for i16 {
    fn from(key: ApiKey) -> Self {
        match key {
            ApiKey::ApiVersion => 1000,
            ApiKey::CreateDatabase => 1001,
            ApiKey::DropDatabase => 1002,
            ApiKey::ListDatabases => 1003,
            ApiKey::DatabaseExists => 1004,
            ApiKey::CreateTable => 1005,
            ApiKey::DropTable => 1006,
            ApiKey::GetTable => 1007,
            ApiKey::ListTables => 1008,
            ApiKey::ListPartitionInfos => 1009,
            ApiKey::TableExists => 1010,
            ApiKey::GetTableSchema => 1011,
            ApiKey::MetaData => 1012,
            ApiKey::ProduceLog => 1014,
            ApiKey::FetchLog => 1015,
            ApiKey::PutKv => 1016,
            ApiKey::Lookup => 1017,
            ApiKey::ListOffsets => 1021,
            ApiKey::GetFileSystemSecurityToken => 1025,
            ApiKey::InitWriter => 1026,
            ApiKey::GetLatestLakeSnapshot => 1032,
            ApiKey::LimitScan => 1033,
            ApiKey::PrefixLookup => 1034,
            ApiKey::GetDatabaseInfo => 1035,
            ApiKey::CreatePartition => 1036,
            ApiKey::DropPartition => 1037,
            ApiKey::Authenticate => 1038,
            Unknown(x) => x,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_round_trip() {
        let cases = [
            (1000, ApiKey::ApiVersion),
            (1001, ApiKey::CreateDatabase),
            (1002, ApiKey::DropDatabase),
            (1003, ApiKey::ListDatabases),
            (1004, ApiKey::DatabaseExists),
            (1005, ApiKey::CreateTable),
            (1006, ApiKey::DropTable),
            (1007, ApiKey::GetTable),
            (1008, ApiKey::ListTables),
            (1009, ApiKey::ListPartitionInfos),
            (1010, ApiKey::TableExists),
            (1011, ApiKey::GetTableSchema),
            (1012, ApiKey::MetaData),
            (1014, ApiKey::ProduceLog),
            (1015, ApiKey::FetchLog),
            (1016, ApiKey::PutKv),
            (1017, ApiKey::Lookup),
            (1021, ApiKey::ListOffsets),
            (1025, ApiKey::GetFileSystemSecurityToken),
            (1026, ApiKey::InitWriter),
            (1032, ApiKey::GetLatestLakeSnapshot),
            (1033, ApiKey::LimitScan),
            (1034, ApiKey::PrefixLookup),
            (1035, ApiKey::GetDatabaseInfo),
            (1036, ApiKey::CreatePartition),
            (1037, ApiKey::DropPartition),
            (1038, ApiKey::Authenticate),
        ];

        for (raw, key) in cases {
            assert_eq!(ApiKey::from(raw), key);
            let mapped: i16 = key.into();
            assert_eq!(mapped, raw);
        }

        let unknown = ApiKey::from(9999);
        assert_eq!(unknown, ApiKey::Unknown(9999));
        let mapped: i16 = unknown.into();
        assert_eq!(mapped, 9999);
    }
}
