/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.rpc.util;

import com.alibaba.fluss.rpc.messages.PbAclFilter;
import com.alibaba.fluss.rpc.messages.PbAclInfo;
import com.alibaba.fluss.security.acl.AccessControlEntry;
import com.alibaba.fluss.security.acl.AccessControlEntryFilter;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.PermissionType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.security.acl.ResourceFilter;
import com.alibaba.fluss.security.acl.ResourceType;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utils for making rpc request/response from inner object or convert inner class to rpc
 * request/response for client and server.
 */
public class CommonRpcMessageUtils {

    public static List<PbAclInfo> toPbAclInfos(Collection<AclBinding> aclBindings) {
        return aclBindings.stream()
                .map(CommonRpcMessageUtils::toPbAclInfo)
                .collect(Collectors.toList());
    }

    public static List<AclBinding> toAclBindings(Collection<PbAclInfo> pbAclInfos) {
        return pbAclInfos.stream()
                .map(CommonRpcMessageUtils::toAclBinding)
                .collect(Collectors.toList());
    }

    public static List<PbAclFilter> toPbAclBindingFilters(
            Collection<AclBindingFilter> aclBindingFilters) {
        return aclBindingFilters.stream()
                .map(CommonRpcMessageUtils::toPbAclFilter)
                .collect(Collectors.toList());
    }

    public static List<AclBindingFilter> toAclBindingFilters(Collection<PbAclFilter> pbAclFilters) {
        return pbAclFilters.stream()
                .map(CommonRpcMessageUtils::toAclFilter)
                .collect(Collectors.toList());
    }

    public static AclBinding toAclBinding(PbAclInfo pbAclInfo) {
        return new AclBinding(
                new Resource(
                        ResourceType.fromCode((byte) pbAclInfo.getResourceType()),
                        pbAclInfo.getResourceName()),
                new AccessControlEntry(
                        new FlussPrincipal(
                                pbAclInfo.getPrincipalName(), pbAclInfo.getPrincipalType()),
                        pbAclInfo.getHost(),
                        OperationType.fromCode((byte) pbAclInfo.getOperationType()),
                        PermissionType.fromCode((byte) pbAclInfo.getPermissionType())));
    }

    public static PbAclInfo toPbAclInfo(AclBinding aclBinding) {
        return new PbAclInfo()
                .setResourceType(aclBinding.getResource().getType().getCode())
                .setResourceName(aclBinding.getResource().getName())
                .setPrincipalName(aclBinding.getAccessControlEntry().getPrincipal().getName())
                .setPrincipalType(aclBinding.getAccessControlEntry().getPrincipal().getType())
                .setHost(aclBinding.getAccessControlEntry().getHost())
                .setOperationType(aclBinding.getAccessControlEntry().getOperationType().getCode())
                .setPermissionType(
                        aclBinding.getAccessControlEntry().getPermissionType().getCode());
    }

    public static PbAclFilter toPbAclFilter(AclBindingFilter aclBindingFilter) {
        AccessControlEntryFilter accessControlEntryFilter = aclBindingFilter.getEntryFilter();
        ResourceFilter resourceFilter = aclBindingFilter.getResourceFilter();

        PbAclFilter pbAclFilter = new PbAclFilter();
        pbAclFilter
                .setResourceType(resourceFilter.getType().getCode())
                .setOperationType(accessControlEntryFilter.getOperationType().getCode())
                .setPermissionType(accessControlEntryFilter.getPermissionType().getCode());
        if (resourceFilter.getName() != null) {
            pbAclFilter.setResourceName(resourceFilter.getName());
        }

        if (accessControlEntryFilter.getPrincipal() != null
                && accessControlEntryFilter.getPrincipal().getName() != null) {
            pbAclFilter.setPrincipalName(accessControlEntryFilter.getPrincipal().getName());
        }

        if (accessControlEntryFilter.getPrincipal() != null
                && accessControlEntryFilter.getPrincipal().getType() != null) {
            pbAclFilter.setPrincipalType(accessControlEntryFilter.getPrincipal().getType());
        }

        if (accessControlEntryFilter.getHost() != null) {
            pbAclFilter.setHost(accessControlEntryFilter.getHost());
        }
        return pbAclFilter;
    }

    public static AclBindingFilter toAclFilter(PbAclFilter pbAclFilter) {
        return new AclBindingFilter(
                new ResourceFilter(
                        ResourceType.fromCode((byte) pbAclFilter.getResourceType()),
                        pbAclFilter.hasResourceName() ? pbAclFilter.getResourceName() : null),
                new AccessControlEntryFilter(
                        pbAclFilter.hasPrincipalName() && pbAclFilter.hasPrincipalType()
                                ? new FlussPrincipal(
                                        pbAclFilter.getPrincipalName(),
                                        pbAclFilter.getPrincipalType())
                                : null,
                        pbAclFilter.hasHost() ? pbAclFilter.getHost() : null,
                        OperationType.fromCode((byte) pbAclFilter.getOperationType()),
                        PermissionType.fromCode((byte) pbAclFilter.getPermissionType())));
    }
}
