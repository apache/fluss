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

use crate::error::Result;
use opendal::Configurator;
use opendal::Operator;
use opendal::layers::TimeoutLayer;
use opendal::services::OssConfig;
use std::collections::HashMap;
use std::time::Duration;

pub(crate) fn oss_config_build(props: &HashMap<String, String>) -> Result<Operator> {
    let config = OssConfig::from_iter(props.clone())?;
    let op = Operator::from_config(config)?.finish();

    // Add timeout layer to prevent hanging on OSS operations
    let timeout_layer = TimeoutLayer::new()
        .with_timeout(Duration::from_secs(10))
        .with_io_timeout(Duration::from_secs(30));

    Ok(op.layer(timeout_layer))
}

pub(crate) fn parse_oss_path(path: &str) -> (&str, &str) {
    let path = path.strip_prefix("oss://").unwrap_or(path);

    match path.find('/') {
        Some(idx) => (&path[..idx], &path[idx + 1..]),
        None => (path, ""),
    }
}
