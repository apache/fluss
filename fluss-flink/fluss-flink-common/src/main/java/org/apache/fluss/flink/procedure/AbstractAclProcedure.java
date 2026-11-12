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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.metadata.GroupId;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutionException;

/** {@link org.apache.flink.table.procedures.Procedure} to operate acl. */
public abstract class AbstractAclProcedure extends ProcedureBase {

    private static final String DATABASE_RESOURCE_PREFIX = "database:";
    private static final String TABLE_RESOURCE_PREFIX = "table:";
    private static final String GROUP_RESOURCE_PREFIX = "group:";

    /**
     * Invokes the ACL operation as a Flink table procedure.
     *
     * <p>This method serves as the entry point for executing ACL operations (ADD, DROP, LIST)
     * through the Flink SQL procedure interface. It delegates execution to the internalCall method.
     *
     * @param resource Resource on which the ACL operation applies. The format must be one of:
     *     <ul>
     *       <li>{@code cluster} - cluster level
     *       <li>{@code database:db_name} - database level, or {@code database:*} for all databases
     *       <li>{@code table:db_name.table_name} - table level, or {@code table:*} for all tables
     *       <li>{@code group:group_id} - consumer-group level, or {@code group:*} for all consumer
     *           groups. A concrete group ID uses the same naming rules as a table name: 1 to 200
     *           ASCII letters, digits, underscores, or hyphens, and must not start with {@code __}.
     *       <li>{@code cluster.db_name} - legacy database syntax
     *       <li>{@code cluster.db_name.table_name} - legacy table syntax
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

        if (startsWithIgnoreCase(resourceStr, DATABASE_RESOURCE_PREFIX)) {
            String databaseName = resourceStr.substring(DATABASE_RESOURCE_PREFIX.length());
            if (databaseName.isEmpty()) {
                throw new IllegalArgumentException("Database name must not be null or empty.");
            }
            if (Resource.WILDCARD_RESOURCE.equals(databaseName)) {
                return Resource.databaseWildcard();
            }
            return Resource.database(databaseName);
        }

        if (startsWithIgnoreCase(resourceStr, TABLE_RESOURCE_PREFIX)) {
            String tablePath = resourceStr.substring(TABLE_RESOURCE_PREFIX.length());
            if (tablePath.isEmpty()) {
                throw new IllegalArgumentException("Table path must not be null or empty.");
            }
            if (Resource.WILDCARD_RESOURCE.equals(tablePath)) {
                return Resource.tableWildcard();
            }
            String[] databaseAndTable = tablePath.split(Resource.TABLE_SPLITTER);
            if (databaseAndTable.length != 2
                    || databaseAndTable[0].isEmpty()
                    || databaseAndTable[1].isEmpty()) {
                throw new IllegalArgumentException(
                        "Table resource must be in format table:${database}.${table}");
            }
            return Resource.table(databaseAndTable[0], databaseAndTable[1]);
        }

        if (startsWithIgnoreCase(resourceStr, GROUP_RESOURCE_PREFIX)) {
            return groupResource(resourceStr.substring(GROUP_RESOURCE_PREFIX.length()));
        }

        String[] resourcePath = resourceStr.split(Resource.TABLE_SPLITTER);
        if (resourcePath.length == 0
                || resourcePath.length > 3
                || !"cluster".equalsIgnoreCase(resourcePath[0])) {
            throw new IllegalArgumentException(
                    "resource must be in format cluster, cluster.${database}, "
                            + "cluster.${database}.${table}, database:${database}, "
                            + "table:${database}.${table}, or group:${group_id}");
        } else if (resourcePath.length == 1) {
            return Resource.cluster();
        } else if (resourcePath.length == 2) {
            return Resource.database(resourcePath[1]);
        } else {
            return Resource.table(resourcePath[1], resourcePath[2]);
        }
    }

    private static Resource groupResource(String groupId) {
        if (groupId.isEmpty()) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer group id must not be null or empty.");
        }
        if (Resource.WILDCARD_RESOURCE.equals(groupId)) {
            return Resource.groupWildcard();
        }
        return Resource.group(GroupId.of(groupId));
    }

    private static boolean startsWithIgnoreCase(String value, String prefix) {
        return value.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    private @Nullable String parseHost(@Nullable String hostStr) {
        if (hostStr != null && hostStr.equalsIgnoreCase("ANY")) {
            return null;
        }
        return hostStr;
    }
}
