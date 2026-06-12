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

use crate::*;
use pyo3::conversion::IntoPyObject;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

/// Administrative client for managing Fluss tables
#[pyclass]
pub struct FlussAdmin {
    __admin: Arc<fcore::client::FlussAdmin>,
}

/// Validate bucket IDs are non-negative
fn validate_bucket_ids(bucket_ids: &[i32]) -> PyResult<()> {
    for &bucket_id in bucket_ids {
        if bucket_id < 0 {
            return Err(FlussError::new_err(format!(
                "Invalid bucket_id: {bucket_id}. Bucket IDs must be non-negative"
            )));
        }
    }
    Ok(())
}

#[pymethods]
impl FlussAdmin {
    /// Create a database.
    ///
    /// Args:
    ///     database_name: Name of the database
    ///     ignore_if_exists: If True, don't raise error if database already exists
    ///     database_descriptor: Optional descriptor (comment, custom_properties)
    ///
    /// Returns:
    ///     None
    #[pyo3(signature = (database_name, database_descriptor=None, ignore_if_exists=false))]
    pub fn create_database<'py>(
        &self,
        py: Python<'py>,
        database_name: &str,
        database_descriptor: Option<&DatabaseDescriptor>,
        ignore_if_exists: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let admin = self.__admin.clone();
        let name = database_name.to_string();
        let descriptor = database_descriptor.map(|d| d.to_core().clone());

        future_into_py(py, async move {
            admin
                .create_database(&name, descriptor.as_ref(), ignore_if_exists)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| Ok(py.None()))
        })
    }

    /// Drop a database.
    ///
    /// Args:
    ///     database_name: Name of the database
    ///     ignore_if_not_exists: If True, don't raise error if database does not exist
    ///     cascade: If True, drop tables in the database first
    ///
    /// Returns:
    ///     None
    #[pyo3(signature = (database_name, ignore_if_not_exists=false, cascade=true))]
    pub fn drop_database<'py>(
        &self,
        py: Python<'py>,
        database_name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let admin = self.__admin.clone();
        let name = database_name.to_string();

        future_into_py(py, async move {
            admin
                .drop_database(&name, ignore_if_not_exists, cascade)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| Ok(py.None()))
        })
    }

    /// List all databases.
    ///
    /// Returns:
    ///     List[str]: Names of all databases
    pub fn list_databases<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            let names = admin
                .list_databases()
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| {
                let py_list = pyo3::types::PyList::empty(py);
                for name in names {
                    py_list.append(name)?;
                }
                Ok(py_list.unbind())
            })
        })
    }

    /// Check if a database exists.
    ///
    /// Args:
    ///     database_name: Name of the database
    ///
    /// Returns:
    ///     bool: True if the database exists
    pub fn database_exists<'py>(
        &self,
        py: Python<'py>,
        database_name: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let admin = self.__admin.clone();
        let name = database_name.to_string();

        future_into_py(py, async move {
            let exists = admin
                .database_exists(&name)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| Ok(exists.into_pyobject(py)?.to_owned().into_any().unbind()))
        })
    }

    /// Get database information.
    ///
    /// Args:
    ///     database_name: Name of the database
    ///
    /// Returns:
    ///     DatabaseInfo: Database metadata
    pub fn get_database_info<'py>(
        &self,
        py: Python<'py>,
        database_name: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let admin = self.__admin.clone();
        let name = database_name.to_string();

        future_into_py(py, async move {
            let info = admin
                .get_database_info(&name)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| Py::new(py, DatabaseInfo::from_core(info)))
        })
    }

    /// List all tables in a database.
    ///
    /// Args:
    ///     database_name: Name of the database
    ///
    /// Returns:
    ///     List[str]: Names of all tables in the database
    pub fn list_tables<'py>(
        &self,
        py: Python<'py>,
        database_name: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let admin = self.__admin.clone();
        let name = database_name.to_string();

        future_into_py(py, async move {
            let names = admin
                .list_tables(&name)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| {
                let py_list = pyo3::types::PyList::empty(py);
                for name in names {
                    py_list.append(name)?;
                }
                Ok(py_list.unbind())
            })
        })
    }

    /// Check if a table exists.
    ///
    /// Args:
    ///     table_path: Path to the table (database, table)
    ///
    /// Returns:
    ///     bool: True if the table exists
    pub fn table_exists<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_table_path = table_path.to_core();
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            let exists = admin
                .table_exists(&core_table_path)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| Ok(exists.into_pyobject(py)?.to_owned().into_any().unbind()))
        })
    }

    /// Drop a partition from a partitioned table.
    ///
    /// Args:
    ///     table_path: Path to the table
    ///     partition_spec: Dict mapping partition column name to value (e.g., {"region": "US"})
    ///     ignore_if_not_exists: If True, don't raise error if partition does not exist
    ///
    /// Returns:
    ///     None
    #[pyo3(signature = (table_path, partition_spec, ignore_if_not_exists=false))]
    pub fn drop_partition<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
        partition_spec: std::collections::HashMap<String, String>,
        ignore_if_not_exists: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_table_path = table_path.to_core();
        let admin = self.__admin.clone();
        let core_partition_spec = fcore::metadata::PartitionSpec::new(partition_spec);

        future_into_py(py, async move {
            admin
                .drop_partition(&core_table_path, &core_partition_spec, ignore_if_not_exists)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| Ok(py.None()))
        })
    }

    /// Create a table with the given schema
    #[pyo3(signature = (table_path, table_descriptor, ignore_if_exists=None))]
    pub fn create_table<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        ignore_if_exists: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ignore = ignore_if_exists.unwrap_or(false);

        let core_table_path = table_path.to_core();
        let core_descriptor = table_descriptor.to_core().clone();
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            admin
                .create_table(&core_table_path, &core_descriptor, ignore)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| Ok(py.None()))
        })
    }

    /// Get table information
    pub fn get_table_info<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_table_path = table_path.to_core();
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            let core_table_info = admin
                .get_table_info(&core_table_path)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| {
                let table_info = TableInfo::from_core(core_table_info);
                Py::new(py, table_info)
            })
        })
    }

    /// Get the latest lake snapshot for a table
    pub fn get_latest_lake_snapshot<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_table_path = table_path.to_core();
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            let core_lake_snapshot = admin
                .get_latest_lake_snapshot(&core_table_path)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| {
                let lake_snapshot = LakeSnapshot::from_core(core_lake_snapshot);
                Py::new(py, lake_snapshot)
            })
        })
    }

    /// Drop a table
    #[pyo3(signature = (table_path, ignore_if_not_exists=false))]
    pub fn drop_table<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
        ignore_if_not_exists: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_table_path = table_path.to_core();
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            admin
                .drop_table(&core_table_path, ignore_if_not_exists)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| Ok(py.None()))
        })
    }

    /// List offsets for buckets (non-partitioned tables only).
    ///
    /// Args:
    ///     table_path: Path to the table
    ///     bucket_ids: List of bucket IDs to query
    ///     offset_spec: Offset specification (OffsetSpec.earliest(), OffsetSpec.latest(),
    ///         or OffsetSpec.timestamp(ts))
    ///
    /// Returns:
    ///     dict[int, int]: Mapping of bucket_id -> offset
    pub fn list_offsets<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
        bucket_ids: Vec<i32>,
        offset_spec: &OffsetSpec,
    ) -> PyResult<Bound<'py, PyAny>> {
        validate_bucket_ids(&bucket_ids)?;
        let offset_spec = offset_spec.inner.clone();

        let core_table_path = table_path.to_core();
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            let offsets = admin
                .list_offsets(&core_table_path, &bucket_ids, offset_spec)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| {
                let dict = pyo3::types::PyDict::new(py);
                for (bucket_id, offset) in offsets {
                    dict.set_item(bucket_id, offset)?;
                }
                Ok(dict.unbind())
            })
        })
    }

    /// List offsets for buckets in a specific partition of a partitioned table.
    ///
    /// Args:
    ///     table_path: Path to the table
    ///     partition_name: Partition value (e.g., "US" not "region=US")
    ///     bucket_ids: List of bucket IDs to query
    ///     offset_spec: Offset specification (OffsetSpec.earliest(), OffsetSpec.latest(),
    ///         or OffsetSpec.timestamp(ts))
    ///
    /// Returns:
    ///     dict[int, int]: Mapping of bucket_id -> offset
    pub fn list_partition_offsets<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
        partition_name: &str,
        bucket_ids: Vec<i32>,
        offset_spec: &OffsetSpec,
    ) -> PyResult<Bound<'py, PyAny>> {
        validate_bucket_ids(&bucket_ids)?;
        let offset_spec = offset_spec.inner.clone();

        let core_table_path = table_path.to_core();
        let admin = self.__admin.clone();
        let partition_name = partition_name.to_string();

        future_into_py(py, async move {
            let offsets = admin
                .list_partition_offsets(&core_table_path, &partition_name, &bucket_ids, offset_spec)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| {
                let dict = pyo3::types::PyDict::new(py);
                for (bucket_id, offset) in offsets {
                    dict.set_item(bucket_id, offset)?;
                }
                Ok(dict.unbind())
            })
        })
    }

    /// Create a partition for a partitioned table.
    ///
    /// Args:
    ///     table_path: Path to the table
    ///     partition_spec: Dict mapping partition column name to value (e.g., {"region": "US"})
    ///     ignore_if_exists: If True, don't raise error if partition already exists
    ///
    /// Returns:
    ///     None
    #[pyo3(signature = (table_path, partition_spec, ignore_if_exists=false))]
    pub fn create_partition<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
        partition_spec: std::collections::HashMap<String, String>,
        ignore_if_exists: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_table_path = table_path.to_core();
        let admin = self.__admin.clone();
        let core_partition_spec = fcore::metadata::PartitionSpec::new(partition_spec);

        future_into_py(py, async move {
            admin
                .create_partition(&core_table_path, &core_partition_spec, ignore_if_exists)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| Ok(py.None()))
        })
    }

    /// List partitions for a partitioned table.
    ///
    /// Args:
    ///     table_path: Path to the table
    ///     partition_spec: Optional partial partition spec to filter results.
    ///         Dict mapping partition column name to value (e.g., {"region": "US"}).
    ///         If None, returns all partitions.
    ///
    /// Returns:
    ///     List[PartitionInfo]: List of partition info objects
    #[pyo3(signature = (table_path, partition_spec=None))]
    pub fn list_partition_infos<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
        partition_spec: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_table_path = table_path.to_core();
        let admin = self.__admin.clone();
        let core_partition_spec = partition_spec.map(fcore::metadata::PartitionSpec::new);

        future_into_py(py, async move {
            let partition_infos = admin
                .list_partition_infos_with_spec(&core_table_path, core_partition_spec.as_ref())
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| {
                let py_list = pyo3::types::PyList::empty(py);
                for info in partition_infos {
                    let py_info = PartitionInfo::from_core(info);
                    py_list.append(Py::new(py, py_info)?)?;
                }
                Ok(py_list.unbind())
            })
        })
    }

    /// Get all alive server nodes in the cluster.
    ///
    /// Returns:
    ///     List[ServerNode]: List of server nodes (coordinator and tablet servers)
    pub fn get_server_nodes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            let nodes = admin
                .get_server_nodes()
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| {
                let py_list = pyo3::types::PyList::empty(py);
                for node in nodes {
                    let py_node = ServerNode::from_core(node);
                    py_list.append(Py::new(py, py_node)?)?;
                }
                Ok(py_list.unbind())
            })
        })
    }

    fn __repr__(&self) -> String {
        "FlussAdmin()".to_string()
    }
}

impl FlussAdmin {
    // Internal method to create FlussAdmin from core admin
    pub fn from_core(admin: Arc<fcore::client::FlussAdmin>) -> Self {
        Self { __admin: admin }
    }
}

/// Information about a partition
#[pyclass]
pub struct PartitionInfo {
    partition_id: i64,
    partition_name: String,
}

#[pymethods]
impl PartitionInfo {
    /// Get the partition ID (globally unique in the cluster)
    #[getter]
    fn partition_id(&self) -> i64 {
        self.partition_id
    }

    /// Get the partition name (e.g., "US" for a table partitioned by region)
    #[getter]
    fn partition_name(&self) -> &str {
        &self.partition_name
    }

    fn __repr__(&self) -> String {
        format!(
            "PartitionInfo(partition_id={}, partition_name='{}')",
            self.partition_id, self.partition_name
        )
    }
}

impl PartitionInfo {
    pub fn from_core(info: fcore::metadata::PartitionInfo) -> Self {
        Self {
            partition_id: info.get_partition_id(),
            partition_name: info.get_partition_name(),
        }
    }
}

/// Information about a server node in the Fluss cluster
#[pyclass]
pub struct ServerNode {
    id: i32,
    host: String,
    port: u32,
    server_type: String,
    uid: String,
}

#[pymethods]
impl ServerNode {
    #[getter]
    fn id(&self) -> i32 {
        self.id
    }

    #[getter]
    fn host(&self) -> &str {
        &self.host
    }

    #[getter]
    fn port(&self) -> u32 {
        self.port
    }

    #[getter]
    fn server_type(&self) -> &str {
        &self.server_type
    }

    #[getter]
    fn uid(&self) -> &str {
        &self.uid
    }

    fn __repr__(&self) -> String {
        format!(
            "ServerNode(id={}, host='{}', port={}, server_type='{}')",
            self.id, self.host, self.port, self.server_type
        )
    }
}

impl ServerNode {
    pub fn from_core(node: fcore::ServerNode) -> Self {
        Self {
            id: node.id(),
            host: node.host().to_string(),
            port: node.port(),
            server_type: node.server_type().to_string(),
            uid: node.uid().to_string(),
        }
    }
}
