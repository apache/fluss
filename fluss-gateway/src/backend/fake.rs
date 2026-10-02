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

//! Fixed catalog fixtures, recorded mutations, and operation-scoped failures for protocol tests.

use crate::backend::context::RequestContext;
use crate::backend::types::ClusterId;
use crate::backend::{FlussBackend, RowWriteError, WriteRequest, WriteResult, unknown_cluster};
use crate::error::{GatewayError, GatewayResult, Resource};
use async_trait::async_trait;
use fluss::metadata::{
    AlterTableChanges, DataType, PartitionInfo, PartitionSpec, Schema, TableDescriptor, TableInfo,
    TablePath,
};
use std::collections::{BTreeMap, HashMap, btree_map::Entry};
use std::sync::{Mutex, MutexGuard, PoisonError};

const FIXTURE_TIME: i64 = 1_700_000_000_000;

struct FakeTable {
    info: TableInfo,
    partitions: Vec<PartitionInfo>,
}

#[derive(Default)]
struct FakeState {
    databases: BTreeMap<String, BTreeMap<String, FakeTable>>,
    calls: Vec<FakeCall>,
    failures: HashMap<Operation, GatewayError>,
    writes: Vec<Option<Vec<String>>>,
    write_failures: Vec<(usize, GatewayError)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    ListDatabases,
    CreateDatabase,
    DropDatabase,
    ListTables,
    DescribeTable,
    CreateTable,
    AlterTable,
    DropTable,
    ListPartitions,
    CreatePartition,
    DropPartition,
    Write,
}

#[derive(Debug, Clone)]
pub enum FakeCall {
    CreateDatabase(String),
    DropDatabase(String),
    CreateTable(TablePath, TableDescriptor),
    AlterTable(TablePath, AlterTableChanges),
    DropTable(TablePath),
    CreatePartition(TablePath, PartitionSpec),
    DropPartition(TablePath, PartitionSpec),
}

pub struct FakeFlussBackend {
    clusters: Vec<ClusterId>,
    state: Mutex<FakeState>,
}

impl Default for FakeFlussBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeFlussBackend {
    pub fn new() -> Self {
        Self::with_catalog(&[])
    }

    pub fn with_catalog(databases: &[(&str, &[&str])]) -> Self {
        let backend = Self {
            clusters: vec![cluster_id("default")],
            state: Mutex::default(),
        };
        for (database, tables) in databases {
            backend.define_database(database);
            for table in *tables {
                backend.define_table(fixture_table(TablePath::new(*database, *table)));
            }
        }
        backend
    }

    pub fn with_clusters(ids: &[&str]) -> Self {
        let mut clusters: Vec<ClusterId> = ids.iter().map(|id| cluster_id(id)).collect();
        clusters.sort();
        Self {
            clusters,
            ..Self::new()
        }
    }

    pub fn define_database(&self, name: &str) {
        self.state().databases.entry(name.to_string()).or_default();
    }

    pub fn define_table(&self, info: TableInfo) {
        let mut state = self.state();
        let tables = state
            .databases
            .entry(info.table_path.database().to_string())
            .or_default();
        match tables.entry(info.table_path.table().to_string()) {
            Entry::Occupied(mut entry) => entry.get_mut().info = info,
            Entry::Vacant(entry) => {
                entry.insert(FakeTable {
                    info,
                    partitions: Vec::new(),
                });
            }
        }
    }

    pub fn with_table(self, info: TableInfo) -> Self {
        self.define_table(info);
        self
    }

    pub fn fail_rows(&self, failures: Vec<(usize, GatewayError)>) {
        self.state().write_failures = failures;
    }

    pub fn writes(&self) -> Vec<Option<Vec<String>>> {
        self.state().writes.clone()
    }

    pub fn define_partition(&self, table: &TablePath, info: PartitionInfo) {
        let mut state = self.state();
        let entry = state
            .databases
            .get_mut(table.database())
            .and_then(|tables| tables.get_mut(table.table()))
            .expect("the fixture table is defined");
        entry.partitions.push(info);
    }

    pub fn fail_once(&self, operation: Operation, error: GatewayError) {
        self.state().failures.insert(operation, error);
    }

    pub fn calls(&self) -> Vec<FakeCall> {
        self.state().calls.clone()
    }

    fn call<T>(
        &self,
        ctx: &RequestContext,
        operation: Operation,
        mutation: Option<FakeCall>,
        answer: impl FnOnce(&FakeState) -> GatewayResult<T>,
    ) -> GatewayResult<T> {
        if !self.has_cluster(ctx.cluster_id().as_str()) {
            return Err(unknown_cluster(ctx.cluster_id().as_str()));
        }
        let mut state = self.state();
        if let Some(call) = mutation {
            state.calls.push(call);
        }
        if let Some(error) = state.failures.remove(&operation) {
            return Err(error);
        }
        answer(&state)
    }

    fn state(&self) -> MutexGuard<'_, FakeState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn cluster_id(id: &str) -> ClusterId {
    ClusterId::try_from(id).expect("valid fixture cluster ID")
}

fn fixture_table(table: TablePath) -> TableInfo {
    let schema = Schema::builder()
        .column("id", DataType::BigInt(fluss::metadata::BigIntType::new()))
        .build()
        .expect("the fixture schema is valid");
    let descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(1), Vec::new())
        .build()
        .expect("the fixture descriptor is valid");
    TableInfo::of(table, 1, 1, descriptor, FIXTURE_TIME, FIXTURE_TIME)
}

fn write_table_info(
    table: &str,
    schema_id: i32,
    columns: Vec<(&str, DataType)>,
    primary_key: Option<&[&str]>,
) -> TableInfo {
    let mut schema = Schema::builder();
    for (name, data_type) in columns {
        schema = schema.column(name, data_type);
    }
    if let Some(keys) = primary_key {
        schema = schema
            .primary_key(keys.iter().copied())
            .expect("valid fixture primary key");
    }
    let descriptor = TableDescriptor::builder()
        .schema(schema.build().expect("valid fixture schema"))
        .distributed_by(Some(3), Vec::new())
        .build()
        .expect("valid fixture table");
    TableInfo::of(
        TablePath::new("fluss", table),
        1,
        schema_id,
        descriptor,
        0,
        0,
    )
}

pub(crate) fn users_table_info(schema_id: i32) -> TableInfo {
    write_table_info(
        "users",
        schema_id,
        vec![
            (
                "id",
                DataType::Int(fluss::metadata::IntType::with_nullable(false)),
            ),
            (
                "name",
                DataType::String(fluss::metadata::StringType::with_nullable(true)),
            ),
        ],
        Some(&["id"]),
    )
}

pub(crate) fn log_table_info(schema_id: i32) -> TableInfo {
    write_table_info(
        "applog",
        schema_id,
        vec![
            (
                "ts",
                DataType::BigInt(fluss::metadata::BigIntType::with_nullable(false)),
            ),
            (
                "message",
                DataType::String(fluss::metadata::StringType::with_nullable(true)),
            ),
        ],
        None,
    )
}

fn database_of<'state>(
    state: &'state FakeState,
    database: &str,
) -> GatewayResult<&'state BTreeMap<String, FakeTable>> {
    state.databases.get(database).ok_or_else(|| {
        GatewayError::not_found(format!("database `{database}` does not exist"))
            .with_resource(Resource::Database)
    })
}

fn table_of<'state>(
    state: &'state FakeState,
    table: &TablePath,
) -> GatewayResult<&'state FakeTable> {
    database_of(state, table.database())?
        .get(table.table())
        .ok_or_else(|| {
            GatewayError::not_found(format!("table `{table}` does not exist"))
                .with_resource(Resource::Table)
        })
}

#[async_trait]
impl FlussBackend for FakeFlussBackend {
    fn clusters(&self) -> Vec<ClusterId> {
        self.clusters.clone()
    }

    fn has_cluster(&self, id: &str) -> bool {
        self.clusters.iter().any(|cluster| cluster.as_str() == id)
    }

    async fn list_databases(&self, ctx: &RequestContext) -> GatewayResult<Vec<String>> {
        self.call(ctx, Operation::ListDatabases, None, |state| {
            Ok(state.databases.keys().cloned().collect())
        })
    }

    async fn create_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::CreateDatabase,
            Some(FakeCall::CreateDatabase(database.to_string())),
            |_| Ok(()),
        )
    }

    async fn drop_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::DropDatabase,
            Some(FakeCall::DropDatabase(database.to_string())),
            |_| Ok(()),
        )
    }

    async fn list_tables(
        &self,
        ctx: &RequestContext,
        database: &str,
    ) -> GatewayResult<Vec<String>> {
        self.call(ctx, Operation::ListTables, None, |state| {
            Ok(database_of(state, database)?.keys().cloned().collect())
        })
    }

    async fn table_info(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<TableInfo> {
        self.call(ctx, Operation::DescribeTable, None, |state| {
            Ok(table_of(state, table)?.info.clone())
        })
    }

    async fn describe_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<TableInfo> {
        self.call(ctx, Operation::DescribeTable, None, |state| {
            Ok(table_of(state, table)?.info.clone())
        })
    }

    async fn create_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        descriptor: &TableDescriptor,
    ) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::CreateTable,
            Some(FakeCall::CreateTable(table.clone(), descriptor.clone())),
            |_| Ok(()),
        )
    }

    async fn alter_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        changes: AlterTableChanges,
    ) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::AlterTable,
            Some(FakeCall::AlterTable(table.clone(), changes)),
            |_| Ok(()),
        )
    }

    async fn drop_table(&self, ctx: &RequestContext, table: &TablePath) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::DropTable,
            Some(FakeCall::DropTable(table.clone())),
            |_| Ok(()),
        )
    }

    async fn list_partitions(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<Vec<PartitionInfo>> {
        self.call(ctx, Operation::ListPartitions, None, |state| {
            Ok(table_of(state, table)?.partitions.clone())
        })
    }

    async fn create_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::CreatePartition,
            Some(FakeCall::CreatePartition(table.clone(), spec.clone())),
            |_| Ok(()),
        )
    }

    async fn drop_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::DropPartition,
            Some(FakeCall::DropPartition(table.clone(), spec.clone())),
            |_| Ok(()),
        )
    }

    async fn write(
        &self,
        ctx: &RequestContext,
        request: WriteRequest,
    ) -> GatewayResult<WriteResult> {
        let row_count = request.rows().len() as u64;
        self.state()
            .writes
            .push(request.partial_update_columns().map(<[String]>::to_vec));
        self.call(ctx, Operation::Write, None, move |state| {
            let failures = state
                .write_failures
                .iter()
                .map(|(index, error)| RowWriteError {
                    index: *index,
                    error: error.clone(),
                })
                .collect();
            Ok(WriteResult {
                row_count,
                failures,
            })
        })
    }
}
