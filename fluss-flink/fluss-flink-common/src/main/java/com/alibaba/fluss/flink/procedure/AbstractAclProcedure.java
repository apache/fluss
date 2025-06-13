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

import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.PermissionType;
import com.alibaba.fluss.security.acl.Resource;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutionException;

/** {@link org.apache.flink.table.procedures.Procedure} to operate acl. */
public abstract class AbstractAclProcedure extends ProcedureBase {

    /**
     * Invokes the ACL operation as a Flink table procedure.
     *
     * <p>This method serves as the entry point for executing ACL operations (ADD, DROP, LIST)
     * through the Flink SQL procedure interface. It delegates execution to the internalCall method.
     *
     * @param resource Resource on which the ACL operation applies. The format must be one of:
     *     <ul>
     *       <li>{@code cluster} - cluster level
     *       <li>{@code cluster.db_name} - database level
     *       <li>{@code cluster.db_name.table_name} - table level
     *     </ul>
     *
     * @param permission Permission type to grant or revoke. Common values include in {@link
     *     PermissionType}.
     * @param principal Principal (user or role) to apply the ACL to. Accepts:
     *     <ul>
     *       <li>{@code ANY}
     *       <li>{@code ALL}
     *       <li>{@code PrincipalType:PrincipalName}, e.g., {@code User:alice}
     *     </ul>
     *
     * @param operation Operation type applied on the resource. Common values include in {@link
     *     OperationType}.
     * @return An array of strings representing the result of the operation:
     *     <ul>
     *       <li>{@code ["success"]} for ADD/DROP operations upon success.
     *       <li>For LIST operations, returns a list of formatted ACL entries as strings.
     *     </ul>
     *
     * @throws ExecutionException if an error occurs during the execution of the ACL operation.
     * @throws InterruptedException if the current thread is interrupted while waiting for the
     *     operation to complete.
     */
    protected String[] internalCall(
            @Nullable String resource,
            @Nullable String permission,
            @Nullable String principal,
            @Nullable String operation,
            @Nullable String host)
            throws Exception {
        PermissionType permissionType =
                permission == null
                        ? PermissionType.ANY
                        : PermissionType.valueOf(permission.toUpperCase());
        FlussPrincipal flussPrincipal = parsePrincipal(principal);
        OperationType operationType =
                operation == null
                        ? OperationType.ANY
                        : OperationType.valueOf(operation.toUpperCase());
        Resource matchResource = parseResource(resource);
        return aclOperation(
                matchResource, permissionType, flussPrincipal, operationType, parseHost(host));
    }

    protected abstract String[] aclOperation(
            Resource resource,
            PermissionType permission,
            FlussPrincipal flussPrincipal,
            OperationType operationType,
            String host)
            throws Exception;

    private FlussPrincipal parsePrincipal(String principalStr) {
        if (principalStr == null || principalStr.equalsIgnoreCase("ANY")) {
            return FlussPrincipal.ANY;
        }

        if (principalStr.equalsIgnoreCase("ALL")) {
            return FlussPrincipal.WILD_CARD_PRINCIPAL;
        }

        String[] principalTypeAndName = principalStr.split(":");
        if (principalTypeAndName.length != 2) {
            throw new IllegalArgumentException(
                    "principal must be in format PrincipalType:PrincipalName");
        }
        return new FlussPrincipal(principalTypeAndName[1], principalTypeAndName[0]);
    }

    private Resource parseResource(String resourceStr) {
        if (resourceStr == null || resourceStr.equalsIgnoreCase("ANY")) {
            return Resource.any();
        }

        Resource resource;
        String[] resourcePath = resourceStr.split(Resource.TABLE_SPLITTER);
        if (resourcePath.length == 0
                || resourcePath.length > 3
                || !"cluster".equalsIgnoreCase(resourcePath[0])) {
            throw new IllegalArgumentException(
                    "resource must be in format cluster.${database}.${table}");
        } else if (resourcePath.length == 1) {
            resource = Resource.cluster();
        } else if (resourcePath.length == 2) {
            resource = Resource.database(resourcePath[1]);
        } else {
            resource = Resource.table(resourcePath[1], resourcePath[2]);
        }
        return resource;
    }

    private @Nullable String parseHost(@Nullable String hostStr) {
        if (hostStr != null && hostStr.equalsIgnoreCase("ANY")) {
            return null;
        }
        return hostStr;
    }
}
