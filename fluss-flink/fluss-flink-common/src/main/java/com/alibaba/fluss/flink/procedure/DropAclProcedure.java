/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.procedure;

import com.alibaba.fluss.security.acl.AccessControlEntryFilter;
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

import java.util.Collections;

/** Procedure to drop acl. */
public class DropAclProcedure extends AbstractAclProcedure {
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
        admin.dropAcls(
                        Collections.singletonList(
                                new AclBindingFilter(
                                        new ResourceFilter(resource.getType(), resource.getName()),
                                        new AccessControlEntryFilter(
                                                flussPrincipal, host, operationType, permission))))
                .all()
                .get();
        return new String[] {"success"};
    }
}
