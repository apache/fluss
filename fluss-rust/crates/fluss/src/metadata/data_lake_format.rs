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

use strum_macros::{Display, EnumString};

/// Identifies the logical format of a data lake table supported by Fluss.
///
/// This enum is typically used in metadata and configuration to distinguish
/// between different table formats so that the appropriate integration and
/// semantics can be applied.
#[derive(Debug, EnumString, Display, PartialEq)]
#[strum(ascii_case_insensitive)]
pub enum DataLakeFormat {
    #[strum(serialize = "paimon")]
    Paimon,

    #[strum(serialize = "lance")]
    Lance,

    #[strum(serialize = "iceberg")]
    Iceberg,
}

#[cfg(test)]
mod tests {
    use crate::metadata::DataLakeFormat;
    use crate::metadata::DataLakeFormat::{Iceberg, Lance, Paimon};

    #[test]
    fn test_parse() {
        let cases = vec![
            ("paimon", Paimon),
            ("Paimon", Paimon),
            ("PAIMON", Paimon),
            ("lance", Lance),
            ("LANCE", Lance),
            ("iceberg", Iceberg),
            ("ICEBERG", Iceberg),
        ];

        for (raw, expected) in cases {
            let parsed = raw.parse::<DataLakeFormat>().unwrap();
            assert_eq!(parsed, expected, "failed to parse: {raw}");
        }

        // negative cases
        assert!("unknown".parse::<DataLakeFormat>().is_err());
        assert!("".parse::<DataLakeFormat>().is_err());
    }
}
