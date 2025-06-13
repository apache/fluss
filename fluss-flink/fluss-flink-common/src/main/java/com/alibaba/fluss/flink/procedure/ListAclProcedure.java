/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.procedure;

import com.alibaba.fluss.security.acl.AccessControlEntry;
import com.alibaba.fluss.security.acl.AccessControlEntryFilter;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.PermissionType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.security.acl.ResourceFilter;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.util.Collection;

/** Procedure to list acl. */
public class ListAclProcedure extends AbstractAclProcedure {
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "resource", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "permission",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "principal",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "operation",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "host", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext context,
            @Nullable String resource,
            @Nullable String permission,
            @Nullable String principal,
            @Nullable String operation,
            @Nullable String host)
            throws Exception {
        return internalCall(resource, permission, principal, operation, host == null ? "*" : host);
    }

    @Override
    protected String[] aclOperation(
            Resource resource,
            PermissionType permission,
            FlussPrincipal flussPrincipal,
            OperationType operationType,
            String host)
            throws Exception {
        Collection<AclBinding> aclBindings =
                admin.listAcls(
                                new AclBindingFilter(
                                        new ResourceFilter(resource.getType(), resource.getName()),
                                        new AccessControlEntryFilter(
                                                flussPrincipal, host, operationType, permission)))
                        .get();
        return aclBindingsToStrings(aclBindings);
    }

    private static String[] aclBindingsToStrings(Collection<AclBinding> aclBindings) {
        return aclBindings.stream()
                .map(ListAclProcedure::aclBindingToString)
                .toArray(String[]::new);
    }

    private static String aclBindingToString(AclBinding binding) {
        if (binding == null) {
            return "";
        }

        AccessControlEntry ace = binding.getAccessControlEntry();
        Resource resource = binding.getResource();

        if (ace == null || resource == null) {
            return "";
        }

        String principal =
                ace.getPrincipal() != null
                        ? (ace.getPrincipal().getType() + ":" + ace.getPrincipal().getName())
                        : null;

        return String.join(
                ";",
                formatEntry("resourceType", resource.getName()),
                formatEntry(
                        "permission",
                        ace.getPermissionType() != null ? ace.getPermissionType().name() : null),
                formatEntry("principal", principal),
                formatEntry(
                        "operation",
                        ace.getOperationType() != null ? ace.getOperationType().name() : null),
                formatEntry("host", ace.getHost()));
    }

    private static String formatEntry(String key, String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        return String.format("%s=\"%s\"", key, value);
    }
}
