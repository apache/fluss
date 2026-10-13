/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use crate::error;
use crate::error::Result;
use crate::io::FileIOBuilder;
use opendal::{Operator, Scheme};
#[cfg(any(feature = "storage-s3", feature = "storage-oss"))]
use std::collections::HashMap;

/// The storage carries all supported storage services in fluss
#[derive(Debug)]
pub enum Storage {
    #[cfg(feature = "storage-memory")]
    Memory,
    #[cfg(feature = "storage-fs")]
    LocalFs,
    #[cfg(feature = "storage-s3")]
    S3 { props: HashMap<String, String> },
    #[cfg(feature = "storage-oss")]
    Oss { props: HashMap<String, String> },
}

impl Storage {
    #[allow(unused_variables)]
    pub(crate) fn build(file_io_builder: FileIOBuilder) -> Result<Self> {
        let (scheme_str, props) = file_io_builder.into_parts();
        let scheme = Self::parse_scheme(&scheme_str)?;

        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs),
            #[cfg(feature = "storage-s3")]
            Scheme::S3 => Ok(Self::S3 { props }),
            #[cfg(feature = "storage-oss")]
            Scheme::Oss => Ok(Self::Oss { props }),
            _ => Err(error::Error::IoUnsupported {
                message: format!("Unsupported storage feature {scheme_str}"),
            }),
        }
    }

    pub(crate) fn create<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
        match self {
            #[cfg(feature = "storage-memory")]
            Storage::Memory => {
                let op = super::memory_config_build()?;

                if let Some(stripped) = path.strip_prefix("memory:/") {
                    Ok((op, stripped))
                } else {
                    Ok((op, &path[1..]))
                }
            }
            #[cfg(feature = "storage-fs")]
            Storage::LocalFs => {
                let op = super::fs_config_build()?;
                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok((op, stripped))
                } else {
                    Ok((op, &path[1..]))
                }
            }
            #[cfg(feature = "storage-s3")]
            Storage::S3 { props } => {
                let (bucket, key) = super::parse_s3_path(path);
                let mut s3_props = props.clone();
                s3_props.insert("bucket".to_string(), bucket.to_string());
                let op = super::s3_config_build(&s3_props)?;
                Ok((op, key))
            }
            #[cfg(feature = "storage-oss")]
            Storage::Oss { props } => {
                let (bucket, key) = super::parse_oss_path(path);
                let mut oss_props = props.clone();
                oss_props.insert("bucket".to_string(), bucket.to_string());
                let op = super::oss_config_build(&oss_props)?;
                Ok((op, key))
            }
        }
    }

    fn parse_scheme(scheme: &str) -> Result<Scheme> {
        match scheme {
            "memory" => Ok(Scheme::Memory),
            "file" | "" => Ok(Scheme::Fs),
            "s3" | "s3a" => Ok(Scheme::S3),
            "oss" => Ok(Scheme::Oss),
            s => Ok(s.parse::<Scheme>()?),
        }
    }
}
