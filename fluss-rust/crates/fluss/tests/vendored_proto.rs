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

//! Guards the vendored `proto/FlussApi.proto` copy that ships with the crate.
//! Both it and `src/proto/fluss.rs` come from the same `regen.sh` run, so this
//! check only needs to compare the copy against the canonical proto — and it
//! runs without protoc.

use std::path::Path;

#[test]
fn the_vendored_proto_matches_the_canonical_one() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let canonical = dir.join("../../../fluss-rpc/src/main/proto/FlussApi.proto");
    if !canonical.exists() {
        return; // released crate: only the vendored copy ships
    }
    assert!(
        std::fs::read(dir.join("proto/FlussApi.proto")).ok() == std::fs::read(&canonical).ok(),
        "FlussApi.proto changed; rerun fluss-rust/crates/fluss/regen.sh and commit the result"
    );
}
