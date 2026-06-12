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

use crate::BucketId;
use crate::metadata::{PhysicalTablePath, TableBucket};
use std::fmt;
use std::sync::Arc;

#[allow(clippy::module_inception)]
mod cluster;

pub use cluster::Cluster;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServerNode {
    id: i32,
    uid: String,
    host: String,
    port: u32,
    server_type: ServerType,
}

impl ServerNode {
    pub fn new(id: i32, host: String, port: u32, server_type: ServerType) -> ServerNode {
        ServerNode {
            id,
            uid: match server_type {
                ServerType::CoordinatorServer => format!("cs-{id}"),
                ServerType::TabletServer => format!("ts-{id}"),
            },
            host,
            port,
            server_type,
        }
    }

    pub fn uid(&self) -> &str {
        &self.uid
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u32 {
        self.port
    }

    pub fn server_type(&self) -> &ServerType {
        &self.server_type
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServerType {
    TabletServer,
    CoordinatorServer,
}

impl ServerType {
    pub fn to_type_id(&self) -> i32 {
        match self {
            ServerType::CoordinatorServer => 1,
            ServerType::TabletServer => 2,
        }
    }

    pub fn from_type_id(type_id: i32) -> Option<ServerType> {
        match type_id {
            1 => Some(ServerType::CoordinatorServer),
            2 => Some(ServerType::TabletServer),
            _ => None,
        }
    }
}

impl fmt::Display for ServerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerType::TabletServer => write!(f, "TabletServer"),
            ServerType::CoordinatorServer => write!(f, "CoordinatorServer"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BucketLocation {
    pub table_bucket: TableBucket,
    leader: Option<ServerNode>,
    physical_table_path: Arc<PhysicalTablePath>,
}

impl BucketLocation {
    pub fn new(
        table_bucket: TableBucket,
        leader: Option<ServerNode>,
        physical_table_path: Arc<PhysicalTablePath>,
    ) -> BucketLocation {
        BucketLocation {
            table_bucket,
            leader,
            physical_table_path,
        }
    }

    pub fn leader(&self) -> &Option<ServerNode> {
        &self.leader
    }

    pub fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    pub fn bucket_id(&self) -> BucketId {
        self.table_bucket.bucket_id()
    }

    pub fn physical_table_path(&self) -> &Arc<PhysicalTablePath> {
        &self.physical_table_path
    }
}
