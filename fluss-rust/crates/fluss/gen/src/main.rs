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

use std::error::Error;
use std::fs;
use std::path::Path;

const HEADER: &str = "\
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// \"License\"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This file was generated from the canonical FlussApi.proto by
// fluss-rust/crates/fluss/regen.sh, and should not be edited by hand.

";

fn main() -> Result<(), Box<dyn Error>> {
    let fluss_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("the gen crate must live inside crates/fluss")?;

    // The canonical proto lives in fluss-rpc and is shared with the Java server.
    // Regeneration always reads it and refreshes the copy vendored under proto/,
    // so the published crate ships the schema next to the generated code.
    let include_dir = fluss_dir.join("../../../fluss-rpc/src/main/proto");
    let proto = include_dir.join("FlussApi.proto");
    if !proto.exists() {
        return Err(format!(
            "canonical proto not found at {}; regeneration must run from the Fluss monorepo",
            proto.display()
        )
        .into());
    }

    let vendored_dir = fluss_dir.join("proto");
    fs::create_dir_all(&vendored_dir)?;
    fs::copy(&proto, vendored_dir.join("FlussApi.proto"))?;

    let mut config = prost_build::Config::new();
    config.bytes([
        ".fluss.PbProduceLogReqForBucket.records",
        ".fluss.PbPutKvReqForBucket.records",
        ".fluss.PbLookupReqForBucket.keys",
        ".fluss.PbPrefixLookupReqForBucket.keys",
        ".fluss.ScanKvResponse.records",
    ]);
    let out_dir = fluss_dir.join("src/proto");
    fs::create_dir_all(&out_dir)?;
    config.out_dir(&out_dir);
    config.compile_protos(&[&proto], &[&include_dir])?;

    let generated_path = out_dir.join("fluss.rs");
    let generated = fs::read_to_string(&generated_path)?;
    fs::write(&generated_path, format!("{HEADER}{generated}"))?;
    Ok(())
}
