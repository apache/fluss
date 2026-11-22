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

use crate::error::{Error, Result};
use arrow::ipc::CompressionType;
use arrow_schema::ArrowError;
use std::collections::HashMap;

pub const TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL: &str = "table.log.arrow.compression.zstd.level";
pub const TABLE_LOG_ARROW_COMPRESSION_TYPE: &str = "table.log.arrow.compression.type";
pub const DEFAULT_NON_ZSTD_COMPRESSION_LEVEL: i32 = -1;
pub const DEFAULT_ZSTD_COMPRESSION_LEVEL: i32 = 3;

#[derive(Clone, Debug, PartialEq)]
pub enum ArrowCompressionType {
    None,
    Lz4Frame,
    Zstd,
}

impl ArrowCompressionType {
    fn from_conf(properties: &HashMap<String, String>) -> Result<Self> {
        match properties
            .get(TABLE_LOG_ARROW_COMPRESSION_TYPE)
            .map(|s| s.as_str())
        {
            Some("NONE") => Ok(Self::None),
            Some("LZ4_FRAME") => Ok(Self::Lz4Frame),
            Some("ZSTD") => Ok(Self::Zstd),
            Some(other) => Err(Error::IllegalArgument {
                message: format!("Unsupported compression type: {other}"),
            }),
            None => Ok(Self::Zstd),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ArrowCompressionInfo {
    pub compression_type: ArrowCompressionType,
    pub compression_level: i32,
}

impl ArrowCompressionInfo {
    pub fn from_conf(properties: &HashMap<String, String>) -> Result<Self> {
        let compression_type = ArrowCompressionType::from_conf(properties)?;

        if compression_type != ArrowCompressionType::Zstd {
            return Ok(Self {
                compression_type,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            });
        }

        match properties
            .get(TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL)
            .map(|s| s.as_str().parse::<i32>())
        {
            Some(Ok(level)) if !(1..=22).contains(&level) => Err(Error::IllegalArgument {
                message: format!(
                    "Invalid ZSTD compression level: {level}. Expected a value between 1 and 22."
                ),
            }),
            Some(Err(e)) => Err(Error::IllegalArgument {
                message: format!(
                    "Invalid ZSTD compression level. Expected a value between 1 and 22. {e}"
                ),
            }),
            Some(Ok(level)) => {
                // TODO Remove once non-default ZSTD compression level is implemented https://github.com/apache/fluss-rust/issues/109
                if level != DEFAULT_ZSTD_COMPRESSION_LEVEL {
                    return Err(Error::ArrowError {
                        message: format!(
                            "Rust client currently only implements default ZSTD compression level {DEFAULT_ZSTD_COMPRESSION_LEVEL}. Got: {level}."
                        ),
                        source: ArrowError::NotYetImplemented(format!(
                            "zstd compression level {level}."
                        )),
                    });
                }
                Ok(Self {
                    compression_type,
                    compression_level: level,
                })
            }
            None => Ok(Self {
                compression_type,
                compression_level: DEFAULT_ZSTD_COMPRESSION_LEVEL,
            }),
        }
    }

    #[cfg(test)]
    fn new(compression_type: ArrowCompressionType, compression_level: i32) -> ArrowCompressionInfo {
        Self {
            compression_type,
            compression_level,
        }
    }

    pub fn get_compression_type(&self) -> Option<CompressionType> {
        match self.compression_type {
            ArrowCompressionType::Zstd => Some(CompressionType::ZSTD),
            ArrowCompressionType::Lz4Frame => Some(CompressionType::LZ4_FRAME),
            ArrowCompressionType::None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_from_conf() {
        assert_eq!(
            ArrowCompressionType::from_conf(&HashMap::new()).unwrap(),
            ArrowCompressionType::Zstd
        );

        assert_eq!(
            ArrowCompressionType::from_conf(&mk_map(&[(
                "table.log.arrow.compression.type",
                "NONE"
            )]))
            .unwrap(),
            ArrowCompressionType::None
        );

        assert_eq!(
            ArrowCompressionType::from_conf(&mk_map(&[(
                "table.log.arrow.compression.type",
                "LZ4_FRAME"
            )]))
            .unwrap(),
            ArrowCompressionType::Lz4Frame
        );

        assert_eq!(
            ArrowCompressionType::from_conf(&mk_map(&[(
                "table.log.arrow.compression.type",
                "ZSTD"
            )]))
            .unwrap(),
            ArrowCompressionType::Zstd
        );
    }

    #[test]
    fn test_from_conf_invalid_compression_type() {
        let props = mk_map(&[("table.log.arrow.compression.type", "FOO")]);

        assert!(
            ArrowCompressionInfo::from_conf(&props)
                .unwrap_err()
                .to_string()
                .contains(
                    "Fluss hitting illegal argument error Unsupported compression type: FOO."
                )
        );
    }

    #[test]
    fn test_from_conf_zstd_compression_level() {
        let compression_info = ArrowCompressionInfo::from_conf(&mk_map(&[(
            "table.log.arrow.compression.type",
            "ZSTD",
        )]));
        assert_eq!(compression_info.unwrap().compression_level, 3);
    }

    // TODO Remove once non-default ZSTD compression level is implemented https://github.com/apache/fluss-rust/issues/109
    #[test]
    fn test_from_conf_zstd_compression_level_error_when_non_default() {
        let result = ArrowCompressionInfo::from_conf(&mk_map(&[
            ("table.log.arrow.compression.type", "ZSTD"),
            ("table.log.arrow.compression.zstd.level", "1"),
        ]));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "Rust client currently only implements default ZSTD compression level 3. Got: 1."
        ));
    }

    #[test]
    fn test_from_conf_compression_level_out_of_range() {
        let props = mk_map(&[
            ("table.log.arrow.compression.type", "ZSTD"),
            ("table.log.arrow.compression.zstd.level", "0"),
        ]);

        assert!(
            ArrowCompressionInfo::from_conf(&props)
                .unwrap_err()
                .to_string()
                .contains("Expected a value between 1 and 22.")
        );

        let props = mk_map(&[
            ("table.log.arrow.compression.type", "ZSTD"),
            ("table.log.arrow.compression.zstd.level", "23"),
        ]);

        assert!(
            ArrowCompressionInfo::from_conf(&props)
                .unwrap_err()
                .to_string()
                .contains("Expected a value between 1 and 22.")
        );
    }

    #[test]
    fn test_from_conf_compression_level_parse_error() {
        let props = mk_map(&[
            ("table.log.arrow.compression.type", "ZSTD"),
            ("table.log.arrow.compression.zstd.level", "not-a-number"),
        ]);

        assert!(
            ArrowCompressionInfo::from_conf(&props)
                .unwrap_err()
                .to_string()
                .contains("Expected a value between 1 and 22.")
        );
    }

    #[test]
    fn get_compression_type_maps_correctly() {
        assert_eq!(
            ArrowCompressionInfo::new(ArrowCompressionType::None, -1).get_compression_type(),
            None
        );
        assert_eq!(
            ArrowCompressionInfo::new(ArrowCompressionType::Lz4Frame, -1).get_compression_type(),
            Some(CompressionType::LZ4_FRAME)
        );
        assert_eq!(
            ArrowCompressionInfo::new(ArrowCompressionType::Zstd, -1).get_compression_type(),
            Some(CompressionType::ZSTD)
        );
    }

    fn mk_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
}
