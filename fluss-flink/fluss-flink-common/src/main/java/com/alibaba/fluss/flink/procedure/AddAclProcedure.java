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
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.PermissionType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.security.acl.ResourceType;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.util.Collections;

/** Procedure to add acl. */
public class AddAclProcedure extends AbstractAclProcedure {

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "resource", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "permission", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "principal", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "operation", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "host", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext context,
            String resource,
            String permission,
            String principal,
            String operation,
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
        if (resource.getType() == ResourceType.ANY
                || PermissionType.ANY == permission
                || FlussPrincipal.ANY.equals(flussPrincipal)
                || OperationType.ANY.equals(operationType)
                || host == null) {
            throw new IllegalArgumentException(
                    "Wildcard 'ANY' can only be used for filtering, not for adding ACL entries.");
        }
        admin.createAcls(
                        Collections.singletonList(
                                new AclBinding(
                                        resource,
                                        new AccessControlEntry(
                                                flussPrincipal, host, operationType, permission))))
                .all()
                .get();
        return new String[] {"success"};
    }
}
