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
use crate::proto::{
    PbAclFilter, PbAclInfo, PbCreateAclRespInfo, PbDropAclsFilterResult, PbDropAclsMatchingAcl,
};

/// Mirrors Java `org.apache.fluss.security.acl.ResourceType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceType {
    Any,
    Cluster,
    Database,
    Table,
}

impl ResourceType {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::Any => 1,
            Self::Cluster => 2,
            Self::Database => 3,
            Self::Table => 4,
        }
    }

    pub fn try_from_i32(value: i32) -> Result<Self> {
        match value {
            1 => Ok(Self::Any),
            2 => Ok(Self::Cluster),
            3 => Ok(Self::Database),
            4 => Ok(Self::Table),
            _ => Err(Error::IllegalArgument {
                message: format!("Unknown resource type code: {value}"),
            }),
        }
    }
}

/// Mirrors Java `org.apache.fluss.security.acl.OperationType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    Any,
    All,
    Read,
    Write,
    Create,
    Drop,
    Alter,
    Describe,
}

impl OperationType {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::Any => 1,
            Self::All => 2,
            Self::Read => 3,
            Self::Write => 4,
            Self::Create => 5,
            Self::Drop => 6,
            Self::Alter => 7,
            Self::Describe => 8,
        }
    }

    pub fn try_from_i32(value: i32) -> Result<Self> {
        match value {
            1 => Ok(Self::Any),
            2 => Ok(Self::All),
            3 => Ok(Self::Read),
            4 => Ok(Self::Write),
            5 => Ok(Self::Create),
            6 => Ok(Self::Drop),
            7 => Ok(Self::Alter),
            8 => Ok(Self::Describe),
            _ => Err(Error::IllegalArgument {
                message: format!("Unknown operation type code: {value}"),
            }),
        }
    }
}

/// Mirrors Java `org.apache.fluss.security.acl.PermissionType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionType {
    Any,
    Allow,
}

impl PermissionType {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::Any => 1,
            Self::Allow => 2,
        }
    }

    pub fn try_from_i32(value: i32) -> Result<Self> {
        match value {
            1 => Ok(Self::Any),
            2 => Ok(Self::Allow),
            _ => Err(Error::IllegalArgument {
                message: format!("Unknown permission type: {value}"),
            }),
        }
    }
}

/// Mirrors Java `org.apache.fluss.security.acl.AclBinding` (concrete ACL entry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclInfo {
    pub resource_name: String,
    pub resource_type: ResourceType,
    pub principal_name: String,
    pub principal_type: String,
    pub host: String,
    pub operation_type: OperationType,
    pub permission_type: PermissionType,
}

impl AclInfo {
    pub fn to_pb(&self) -> PbAclInfo {
        PbAclInfo {
            resource_name: self.resource_name.clone(),
            resource_type: self.resource_type.to_i32(),
            principal_name: self.principal_name.clone(),
            principal_type: self.principal_type.clone(),
            host: self.host.clone(),
            operation_type: self.operation_type.to_i32(),
            permission_type: self.permission_type.to_i32(),
        }
    }

    pub fn from_pb(pb: &PbAclInfo) -> Result<Self> {
        Ok(Self {
            resource_name: pb.resource_name.clone(),
            resource_type: ResourceType::try_from_i32(pb.resource_type)?,
            principal_name: pb.principal_name.clone(),
            principal_type: pb.principal_type.clone(),
            host: pb.host.clone(),
            operation_type: OperationType::try_from_i32(pb.operation_type)?,
            permission_type: PermissionType::try_from_i32(pb.permission_type)?,
        })
    }
}

/// Mirrors Java `org.apache.fluss.security.acl.AclBindingFilter` — like `AclInfo` but with
/// optional name/principal/host fields for partial matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclFilter {
    pub resource_name: Option<String>,
    pub resource_type: ResourceType,
    pub principal_name: Option<String>,
    pub principal_type: Option<String>,
    pub host: Option<String>,
    pub operation_type: OperationType,
    pub permission_type: PermissionType,
}

impl AclFilter {
    pub fn to_pb(&self) -> PbAclFilter {
        PbAclFilter {
            resource_name: self.resource_name.clone(),
            resource_type: self.resource_type.to_i32(),
            principal_name: self.principal_name.clone(),
            principal_type: self.principal_type.clone(),
            host: self.host.clone(),
            operation_type: self.operation_type.to_i32(),
            permission_type: self.permission_type.to_i32(),
        }
    }

    pub fn from_pb(pb: &PbAclFilter) -> Result<Self> {
        Ok(Self {
            resource_name: pb.resource_name.clone(),
            resource_type: ResourceType::try_from_i32(pb.resource_type)?,
            principal_name: pb.principal_name.clone(),
            principal_type: pb.principal_type.clone(),
            host: pb.host.clone(),
            operation_type: OperationType::try_from_i32(pb.operation_type)?,
            permission_type: PermissionType::try_from_i32(pb.permission_type)?,
        })
    }
}

/// One per ACL submitted to `create_acls`: success or a server-side error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAclResult {
    pub acl: AclInfo,
    pub error: Option<AclError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclError {
    pub code: i32,
    pub message: Option<String>,
}

impl CreateAclResult {
    pub fn from_pb(pb: &PbCreateAclRespInfo) -> Result<Self> {
        Ok(Self {
            acl: AclInfo::from_pb(&pb.acl)?,
            error: pb.error_code.map(|code| AclError {
                code,
                message: pb.error_message.clone(),
            }),
        })
    }
}

/// One ACL matched by a filter in `drop_acls`. Reports the bound ACL and any
/// server-side error encountered while dropping it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropAclMatchingAcl {
    pub acl: AclInfo,
    pub error: Option<AclError>,
}

impl DropAclMatchingAcl {
    pub fn from_pb(pb: &PbDropAclsMatchingAcl) -> Result<Self> {
        Ok(Self {
            acl: AclInfo::from_pb(&pb.acl)?,
            error: pb.error_code.map(|code| AclError {
                code,
                message: pb.error_message.clone(),
            }),
        })
    }
}

/// One per filter submitted to `drop_acls`: the matching ACLs that were
/// targeted plus any filter-level error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropAclsFilterResult {
    pub matching_acls: Vec<DropAclMatchingAcl>,
    pub error: Option<AclError>,
}

impl DropAclsFilterResult {
    pub fn from_pb(pb: &PbDropAclsFilterResult) -> Result<Self> {
        let matching_acls = pb
            .matching_acls
            .iter()
            .map(DropAclMatchingAcl::from_pb)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            matching_acls,
            error: pb.error_code.map(|code| AclError {
                code,
                message: pb.error_message.clone(),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_type_roundtrip() {
        for v in [
            ResourceType::Any,
            ResourceType::Cluster,
            ResourceType::Database,
            ResourceType::Table,
        ] {
            assert_eq!(ResourceType::try_from_i32(v.to_i32()).unwrap(), v);
        }
        assert!(ResourceType::try_from_i32(0).is_err());
    }

    #[test]
    fn test_operation_type_roundtrip() {
        for v in [
            OperationType::Any,
            OperationType::All,
            OperationType::Read,
            OperationType::Write,
            OperationType::Create,
            OperationType::Drop,
            OperationType::Alter,
            OperationType::Describe,
        ] {
            assert_eq!(OperationType::try_from_i32(v.to_i32()).unwrap(), v);
        }
        assert!(OperationType::try_from_i32(0).is_err());
    }

    #[test]
    fn test_permission_type_roundtrip() {
        for v in [PermissionType::Any, PermissionType::Allow] {
            assert_eq!(PermissionType::try_from_i32(v.to_i32()).unwrap(), v);
        }
        assert!(PermissionType::try_from_i32(0).is_err());
    }

    #[test]
    fn test_acl_info_pb_roundtrip() {
        let original = AclInfo {
            resource_name: "topic-a".to_string(),
            resource_type: ResourceType::Table,
            principal_name: "alice".to_string(),
            principal_type: "User".to_string(),
            host: "*".to_string(),
            operation_type: OperationType::Read,
            permission_type: PermissionType::Allow,
        };
        let pb = original.to_pb();
        assert_eq!(AclInfo::from_pb(&pb).unwrap(), original);
    }

    #[test]
    fn test_acl_filter_pb_roundtrip() {
        let original = AclFilter {
            resource_name: None,
            resource_type: ResourceType::Any,
            principal_name: Some("alice".to_string()),
            principal_type: None,
            host: Some("*".to_string()),
            operation_type: OperationType::Any,
            permission_type: PermissionType::Any,
        };
        let pb = original.to_pb();
        assert_eq!(AclFilter::from_pb(&pb).unwrap(), original);
    }
}
