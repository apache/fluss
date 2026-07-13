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

use crate::async_nif;
use crate::atoms::to_nif_err;
use crate::connection::ConnectionResource;
use crate::schema::{NifSchema, TableDescriptorResource};
use fluss::client::FlussAdmin;
use fluss::error::Error;
use fluss::metadata::{DatabaseDescriptor, DatabaseInfo, SchemaInfo, TableInfo, TablePath};
use fluss::{ServerNode, ServerType};
use rustler::{Env, NifStruct, NifUnitEnum, ResourceArc, Term};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(NifUnitEnum)]
pub enum NifServerType {
    TabletServer,
    CoordinatorServer,
    Unknown,
}

#[derive(NifStruct)]
#[module = "Fluss.ServerNode"]
pub struct NifServerNode {
    pub id: i32,
    pub uid: String,
    pub host: String,
    pub port: u32,
    pub server_type: NifServerType,
}

impl NifServerNode {
    pub fn from_core(node: &ServerNode) -> Self {
        Self {
            id: node.id(),
            uid: node.uid().to_string(),
            host: node.host().to_string(),
            port: node.port(),
            server_type: match node.server_type() {
                ServerType::TabletServer => NifServerType::TabletServer,
                ServerType::CoordinatorServer => NifServerType::CoordinatorServer,
                ServerType::Unknown => NifServerType::Unknown,
            },
        }
    }
}

#[derive(NifStruct)]
#[module = "Fluss.DatabaseDescriptor"]
pub struct NifDatabaseDescriptor {
    pub comment: Option<String>,
    pub custom_properties: HashMap<String, String>,
}

impl NifDatabaseDescriptor {
    pub fn from_core(desc: &DatabaseDescriptor) -> Self {
        Self {
            comment: desc.comment().map(String::from),
            custom_properties: desc.custom_properties().clone(),
        }
    }

    pub fn to_core(&self) -> DatabaseDescriptor {
        let mut desc = DatabaseDescriptor::builder();
        if let Some(comment) = &self.comment {
            desc = desc.comment(comment);
        }

        desc.custom_properties(self.custom_properties.clone())
            .build()
    }
}

#[derive(NifStruct)]
#[module = "Fluss.DatabaseInfo"]
pub struct NifDatabaseInfo {
    pub database_name: String,
    pub descriptor: NifDatabaseDescriptor,
    pub created_time: i64,
    pub modified_time: i64,
}

impl NifDatabaseInfo {
    pub fn from_core(info: &DatabaseInfo) -> Self {
        Self {
            database_name: info.database_name().to_string(),
            descriptor: NifDatabaseDescriptor::from_core(info.database_descriptor()),
            created_time: info.created_time(),
            modified_time: info.modified_time(),
        }
    }
}

#[derive(NifStruct)]
#[module = "Fluss.TableInfo"]
pub struct NifTableInfo {
    pub database_name: String,
    pub table_name: String,
    pub table_id: i64,
    pub schema_id: i32,
    pub schema: NifSchema,
    pub num_buckets: i32,
    pub has_primary_key: bool,
    pub primary_keys: Vec<String>,
    pub physical_primary_keys: Vec<String>,
    pub bucket_keys: Vec<String>,
    pub has_bucket_key: bool,
    pub is_default_bucket_key: bool,
    pub is_partitioned: bool,
    pub is_auto_partitioned: bool,
    pub partition_keys: Vec<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub custom_properties: HashMap<String, String>,
    pub created_time: i64,
    pub modified_time: i64,
}

impl NifTableInfo {
    pub fn from_core(info: &TableInfo) -> Result<Self, Error> {
        let table_path: &TablePath = info.get_table_path();

        Ok(Self {
            database_name: table_path.database().to_string(),
            table_name: table_path.table().to_string(),
            table_id: info.get_table_id(),
            schema_id: info.get_schema_id(),
            schema: NifSchema::from_core(info.get_schema())?,
            num_buckets: info.get_num_buckets(),
            has_primary_key: info.has_primary_key(),
            primary_keys: info.get_primary_keys().to_vec(),
            physical_primary_keys: info.get_physical_primary_keys().to_vec(),
            bucket_keys: info.get_bucket_keys().to_vec(),
            has_bucket_key: info.has_bucket_key(),
            is_default_bucket_key: info.is_default_bucket_key(),
            is_partitioned: info.is_partitioned(),
            is_auto_partitioned: info.is_auto_partitioned(),
            partition_keys: info.get_partition_keys().to_vec(),
            comment: info.get_comment().map(String::from),
            properties: info.get_properties().clone(),
            custom_properties: info.get_custom_properties().clone(),
            created_time: info.get_created_time(),
            modified_time: info.get_modified_time(),
        })
    }
}

pub struct AdminResource {
    pub inner: Arc<FlussAdmin>,
}

impl std::panic::RefUnwindSafe for AdminResource {}

#[rustler::resource_impl]
impl rustler::Resource for AdminResource {}

#[rustler::nif]
fn admin_new(
    conn: ResourceArc<ConnectionResource>,
) -> Result<ResourceArc<AdminResource>, rustler::Error> {
    let inner = conn.inner.get_admin().map_err(to_nif_err)?;
    Ok(ResourceArc::new(AdminResource { inner }))
}

#[rustler::nif]
fn admin_get_server_nodes<'a>(env: Env<'a>, admin: ResourceArc<AdminResource>) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move {
        let nodes: Vec<ServerNode> = admin.inner.get_server_nodes().await?;
        let wrapped: Vec<NifServerNode> = nodes.iter().map(NifServerNode::from_core).collect();
        Ok(wrapped)
    })
}

#[rustler::nif]
fn admin_create_database<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    descriptor: Option<NifDatabaseDescriptor>,
    ignore_if_exists: bool,
) -> Term<'a> {
    async_nif::spawn_task(env, async move {
        let core_descriptor = descriptor.as_ref().map(NifDatabaseDescriptor::to_core);
        admin
            .inner
            .create_database(&database_name, core_descriptor.as_ref(), ignore_if_exists)
            .await
    })
}

#[rustler::nif]
fn admin_get_database_info<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move {
        let info: DatabaseInfo = admin.inner.get_database_info(&database_name).await?;
        Ok(NifDatabaseInfo::from_core(&info))
    })
}

#[rustler::nif]
fn admin_drop_database<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    ignore_if_not_exists: bool,
) -> Term<'a> {
    async_nif::spawn_task(env, async move {
        admin
            .inner
            .drop_database(&database_name, ignore_if_not_exists, false)
            .await
    })
}

#[rustler::nif]
fn admin_list_databases<'a>(env: Env<'a>, admin: ResourceArc<AdminResource>) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move { admin.inner.list_databases().await })
}

#[rustler::nif]
fn admin_database_exists<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move {
        admin.inner.database_exists(&database_name).await
    })
}

#[rustler::nif]
fn admin_create_table<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    table_name: String,
    descriptor: ResourceArc<TableDescriptorResource>,
    ignore_if_exists: bool,
) -> Term<'a> {
    async_nif::spawn_task(env, async move {
        let path = TablePath::new(&database_name, &table_name);
        admin
            .inner
            .create_table(&path, &descriptor.inner, ignore_if_exists)
            .await
    })
}

#[rustler::nif]
fn admin_drop_table<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    table_name: String,
    ignore_if_not_exists: bool,
) -> Term<'a> {
    async_nif::spawn_task(env, async move {
        let path = TablePath::new(&database_name, &table_name);
        admin.inner.drop_table(&path, ignore_if_not_exists).await
    })
}

#[rustler::nif]
fn admin_list_tables<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
) -> Term<'a> {
    async_nif::spawn_task_with_result(
        env,
        async move { admin.inner.list_tables(&database_name).await },
    )
}

#[rustler::nif]
fn admin_table_exists<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    table_name: String,
) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move {
        let table_path = TablePath::new(database_name, table_name);
        admin.inner.table_exists(&table_path).await
    })
}

#[rustler::nif]
fn admin_get_table_info<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    table_name: String,
) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move {
        let table_path = TablePath::new(database_name, table_name);
        let table_info = admin.inner.get_table_info(&table_path).await?;
        NifTableInfo::from_core(&table_info)
    })
}

#[derive(NifStruct)]
#[module = "Fluss.SchemaInfo"]
pub struct NifSchemaInfo {
    pub schema: NifSchema,
    pub schema_id: i32,
}

impl NifSchemaInfo {
    pub fn from_core(info: &SchemaInfo) -> Result<Self, Error> {
        Ok(Self {
            schema: NifSchema::from_core(info.schema())?,
            schema_id: info.schema_id(),
        })
    }
}

#[rustler::nif]
fn admin_get_table_schema<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    table_name: String,
    schema_id: Option<i32>,
) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move {
        let table_path = TablePath::new(database_name, table_name);
        let schema_info = admin.inner.get_table_schema(&table_path, schema_id).await?;
        NifSchemaInfo::from_core(&schema_info)
    })
}
