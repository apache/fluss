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

use std::io::Result;
use std::path::Path;

fn main() -> Result<()> {
    let mut config = prost_build::Config::new();
    config.bytes([
        ".fluss.PbProduceLogReqForBucket.records",
        ".fluss.PbPutKvReqForBucket.records",
        ".fluss.PbLookupReqForBucket.keys",
        ".fluss.PbPrefixLookupReqForBucket.keys",
        ".fluss.ScanKvResponse.records",
    ]);
    // Published crates vendor the proto under proto/ (scripts/vendor-proto.sh);
    // monorepo builds read the canonical proto directly from fluss-rpc.
    let (proto, include_dir) = if Path::new("proto/FlussApi.proto").exists() {
        ("proto/FlussApi.proto", "proto")
    } else {
        (
            "../../../fluss-rpc/src/main/proto/FlussApi.proto",
            "../../../fluss-rpc/src/main/proto",
        )
    };
    config.compile_protos(&[proto], &[include_dir])?;
    Ok(())
}
