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

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::KvFormat;
use std::fmt::Display;

#[derive(Copy, Clone)]
pub enum WriteFormat {
    ArrowLog,
    CompactedLog,
    CompactedKv,
}

impl WriteFormat {
    pub const fn is_log(&self) -> bool {
        matches!(self, Self::ArrowLog | Self::CompactedLog)
    }

    pub fn is_kv(&self) -> bool {
        !self.is_log()
    }

    pub fn to_kv_format(&self) -> Result<KvFormat> {
        match self {
            WriteFormat::CompactedKv => Ok(KvFormat::COMPACTED),
            other => Err(IllegalArgument {
                message: format!("WriteFormat `{other}` is not a KvFormat"),
            }),
        }
    }

    pub fn from_kv_format(kv_format: &KvFormat) -> Result<Self> {
        match kv_format {
            KvFormat::COMPACTED => Ok(WriteFormat::CompactedKv),
            other => Err(IllegalArgument {
                message: format!("Unknown KvFormat: `{other}`"),
            }),
        }
    }
}

impl Display for WriteFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteFormat::ArrowLog => f.write_str("ArrowLog"),
            WriteFormat::CompactedLog => f.write_str("CompactedLog"),
            WriteFormat::CompactedKv => f.write_str("CompactedKv"),
        }
    }
}
