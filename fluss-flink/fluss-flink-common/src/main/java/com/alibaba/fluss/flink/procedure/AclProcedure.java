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

import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

/** {@link org.apache.flink.table.procedures.Procedure} to operate acl. */
public class AclProcedure extends ProcedureBase {
    private static final String IDENTIFIER = "acl";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * Invokes the ACL operation as a Flink table procedure.
     *
     * <p>This method serves as the entry point for executing ACL operations (ADD, DROP, LIST)
     * through the Flink SQL procedure interface. It delegates execution to the internalCall method.
     *
     * @param context The procedure context used for execution environment and resource access.
     * @param action Actoion of ACL operation to perform. Valid values are:
     *     <ul>
     *       <li>{@code ADD}: Adds an ACL entry.
     *       <li>{@code DROP}: Removes an ACL entry.
     *       <li>{@code LIST}: Lists matching ACL entries.
     *     </ul>
     *
     * @param resource Resource on which the ACL operation applies. The format must be one of:
     *     <ul>
     *       <li>{@code fluss-cluster} - cluster level
     *       <li>{@code fluss-cluster.db_name} - database level
     *       <li>{@code fluss-cluster.db_name.table_name} - table level
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
    public String[] call(
            ProcedureContext context,
            String action,
            String resource,
            String permission,
            String principal,
            String operation)
            throws ExecutionException, InterruptedException {
        return internalCall(action, resource, permission, principal, operation);
    }

    private String[] internalCall(
            String action,
            String resource,
            String permissionType,
            String principal,
            String operation)
            throws ExecutionException, InterruptedException {
        Action type = Action.valueOf(action);
        PermissionType permission = PermissionType.valueOf(permissionType);
        FlussPrincipal flussPrincipal = parsePrincipal(principal);
        OperationType operationType = OperationType.valueOf(operation);
        Resource matchResource = parseResource(resource);
        switch (type) {
            case ADD:
                addAcl(matchResource, permission, flussPrincipal, operationType, "*");
                return new String[] {"success"};
            case DROP:
                dropAcl(matchResource, permission, flussPrincipal, operationType, null);
                return new String[] {"success"};
            case LIST:
                return listAcl(matchResource, permission, flussPrincipal, operationType, null);
            default:
                throw new IllegalArgumentException("unknown acl type: " + type);
        }
    }

    private void addAcl(
            Resource resource,
            PermissionType permission,
            FlussPrincipal flussPrincipal,
            OperationType operationType,
            String host)
            throws ExecutionException, InterruptedException {
        admin.createAcls(
                        Collections.singletonList(
                                new AclBinding(
                                        resource,
                                        new AccessControlEntry(
                                                flussPrincipal, host, operationType, permission))))
                .all()
                .get();
    }

    private void dropAcl(
            Resource resource,
            PermissionType permission,
            FlussPrincipal flussPrincipal,
            OperationType operationType,
            String host)
            throws ExecutionException, InterruptedException {
        admin.dropAcls(
                        Collections.singletonList(
                                new AclBindingFilter(
                                        new ResourceFilter(resource.getType(), resource.getName()),
                                        new AccessControlEntryFilter(
                                                flussPrincipal, host, operationType, permission))))
                .all()
                .get();
    }

    private String[] listAcl(
            Resource resource,
            PermissionType permission,
            FlussPrincipal flussPrincipal,
            OperationType operationType,
            String host)
            throws ExecutionException, InterruptedException {
        Collection<AclBinding> aclBindings =
                admin.listAcls(
                                new AclBindingFilter(
                                        new ResourceFilter(resource.getType(), resource.getName()),
                                        new AccessControlEntryFilter(
                                                flussPrincipal, host, operationType, permission)))
                        .get();
        return aclBindings.stream()
                .map(
                        aclBinding -> {
                            Resource matchResourceresource = aclBinding.getResource();
                            AccessControlEntry entry = aclBinding.getAccessControlEntry();
                            return String.format(
                                    "| %s | %s | %s | %s | %s | %s |",
                                    matchResourceresource.getType(),
                                    matchResourceresource.getName(),
                                    entry.getPermissionType(),
                                    entry.getPrincipal(),
                                    entry.getOperationType(),
                                    entry.getHost());
                        })
                .toArray(String[]::new);
    }

    private FlussPrincipal parsePrincipal(String principalStr) {
        if (principalStr.equalsIgnoreCase("ANY")) {
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
        if (resourceStr.equalsIgnoreCase("ANY")) {
            return Resource.any();
        }

        Resource resource;
        String[] resourcePath = resourceStr.split(Resource.TABLE_SPLITTER);
        if (resourcePath.length == 1 && Resource.FLUSS_CLUSTER.equalsIgnoreCase(resourcePath[0])) {
            resource = Resource.cluster();
        } else if (resourcePath.length == 2) {
            resource = Resource.database(resourcePath[1]);
        } else if (resourcePath.length == 3) {
            resource = Resource.table(resourcePath[1], resourcePath[2]);
        } else {
            throw new IllegalArgumentException(
                    "resource must be in format fluss-cluster.${database}.${table}");
        }
        return resource;
    }

    private enum Action {
        ADD,
        LIST,
        DROP
    }
}
