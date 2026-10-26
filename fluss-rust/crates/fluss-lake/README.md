<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# UnionRead source preparation

`fluss-lake` captures reusable source boundaries without opening a lake catalog or reading lake files. This layer provides preparation and context transport; the default lake planner and reader are delivered separately.

```rust,no_run
# use std::sync::Arc;
# use fluss::client::FlussConnection;
# use fluss::metadata::TablePath;
# use fluss_lake::{FlussLakeTable, FlussLakeReadContext};
# async fn prepare(connection: Arc<FlussConnection>, path: &TablePath) -> fluss_lake::Result<()> {
let table = FlussLakeTable::open(connection, path).await?;
let context = table.prepare().await?;
let transported = FlussLakeReadContext::from_json(&context.to_json()?)?;
# Ok(())
# }
```

The context freezes table/schema identity, physical lake table mapping, a readable snapshot and half-open log ranges `[start, stop)`. Scan configuration, including projection and filters, belongs to the subsequent planning layer; preparation always captures table-wide boundaries. Actual per-partition bucket counts are preserved. The context contains no credentials or physical lake-file tasks, is not a global transactional snapshot and does not acquire a retention lease. Its JSON representation is an evolving Rust contract, not a stable cross-language ABI.

The lake crate requires Rust 1.91 independently of the core workspace MSRV. Run `cargo test -p fluss-lake --lib` for unit tests. `test_prepare` requires the `integration_tests` feature and the existing Fluss Docker test-cluster environment.
