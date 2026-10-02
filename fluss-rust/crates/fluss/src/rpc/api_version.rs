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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct ApiVersion(pub i16);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ApiVersionRange {
    min: ApiVersion,
    max: ApiVersion,
}

impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[allow(dead_code)]
impl ApiVersionRange {
    pub const fn new(min: ApiVersion, max: ApiVersion) -> Self {
        assert!(min.0 <= max.0);

        Self { min, max }
    }

    pub fn min(&self) -> ApiVersion {
        self.min
    }

    pub fn max(&self) -> ApiVersion {
        self.max
    }
}

impl std::fmt::Display for ApiVersionRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.min, self.max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_version_display() {
        let version = ApiVersion(3);
        assert_eq!(version.to_string(), "3");
    }

    #[test]
    fn api_version_range_accessors() {
        let range = ApiVersionRange::new(ApiVersion(1), ApiVersion(4));
        assert_eq!(range.min(), ApiVersion(1));
        assert_eq!(range.max(), ApiVersion(4));
        assert_eq!(range.to_string(), "1:4");
    }

    #[test]
    #[should_panic]
    fn api_version_range_panics_on_invalid_bounds() {
        let _ = ApiVersionRange::new(ApiVersion(4), ApiVersion(1));
    }
}
