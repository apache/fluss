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

package org.apache.fluss.cli.sql.executor;

import org.apache.fluss.cli.config.ConnectionManager;
import org.apache.fluss.cli.util.SqlParserUtil;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.CreateAclsResult;
import org.apache.fluss.client.admin.DropAclsResult;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AccessControlEntryFilter;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceFilter;
import org.apache.fluss.security.acl.ResourceType;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Handles ACL-related SQL operations. */
public class AclExecutor {
    private static final Pattern ACL_FILTER_PATTERN =
            Pattern.compile("FILTER\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE);

    private final ConnectionManager connectionManager;
    private final PrintWriter out;

    public AclExecutor(ConnectionManager connectionManager, PrintWriter out) {
        this.connectionManager = connectionManager;
        this.out = out;
    }

    public void executeShowAcls(
            org.apache.fluss.cli.sql.ast.FlussStatementNodes.ShowAclsStatement stmt)
            throws Exception {
        AclBindingFilter filter = parseAclBindingFilter(stmt.getOriginalSql(), false);
        Admin admin = connectionManager.getConnection().getAdmin();
        Collection<AclBinding> acls = admin.listAcls(filter).get();

        out.println("ACLs:");
        if (acls.isEmpty()) {
            out.println("  (none)");
            out.flush();
            return;
        }

        for (AclBinding acl : acls) {
            out.println(acl.toString());
        }
        out.flush();
    }

    public void executeCreateAcl(
            org.apache.fluss.cli.sql.ast.FlussStatementNodes.CreateAclStatement stmt)
            throws Exception {
        AclBinding aclBinding = parseAclBinding(stmt.getOriginalSql());
        Admin admin = connectionManager.getConnection().getAdmin();
        CreateAclsResult result = admin.createAcls(Collections.singletonList(aclBinding));
        result.all().get();
        out.println("ACL created: " + aclBinding);
        out.flush();
    }

    public void executeDropAcl(
            org.apache.fluss.cli.sql.ast.FlussStatementNodes.DropAclStatement stmt)
            throws Exception {
        AclBindingFilter filter = parseAclBindingFilter(stmt.getOriginalSql(), true);
        Admin admin = connectionManager.getConnection().getAdmin();
        DropAclsResult result = admin.dropAcls(Collections.singletonList(filter));
        Collection<AclBinding> deleted = result.all().get();
        out.println("ACLs dropped: " + deleted.size());
        for (AclBinding acl : deleted) {
            out.println("  " + acl.toString());
        }
        out.flush();
    }

    private AclBinding parseAclBinding(String sql) {
        String content = SqlParserUtil.extractParenthesizedContent(sql);
        Map<String, String> values = SqlParserUtil.parseKeyValueMap(content);
        String resourceType = values.get("resource_type");
        String resourceName = values.get("resource_name");
        String principalValue = values.get("principal");
        String principalType = values.get("principal_type");
        String host = values.getOrDefault("host", AccessControlEntry.WILD_CARD_HOST);
        String operation = values.get("operation");
        String permission = values.get("permission");

        if (resourceType == null
                || principalValue == null
                || principalValue.trim().isEmpty()
                || operation == null
                || permission == null) {
            throw new IllegalArgumentException(
                    "CREATE ACL requires resource_type, principal, operation, permission");
        }

        Resource resource = parseResource(resourceType, resourceName, false);
        FlussPrincipal principal = parsePrincipal(principalValue, principalType, false);
        OperationType operationType = parseOperationType(operation, false);
        PermissionType permissionType = parsePermissionType(permission, false);

        AccessControlEntry entry =
                new AccessControlEntry(principal, host, operationType, permissionType);
        return new AclBinding(resource, entry);
    }

    private AclBindingFilter parseAclBindingFilter(String sql, boolean required) {
        Matcher matcher = ACL_FILTER_PATTERN.matcher(sql);
        if (!matcher.find()) {
            if (required) {
                throw new IllegalArgumentException("DROP ACL requires FILTER clause");
            }
            return AclBindingFilter.ANY;
        }

        Map<String, String> values = SqlParserUtil.parseKeyValueMap(matcher.group(1));
        String resourceType = values.get("resource_type");
        String resourceName = values.get("resource_name");
        String principalValue = values.get("principal");
        String principalType = values.get("principal_type");
        String host = values.get("host");
        String operation = values.get("operation");
        String permission = values.get("permission");

        ResourceFilter resourceFilter = ResourceFilter.ANY;
        String resourceToken = values.get("resource");
        if (resourceToken != null && (resourceType == null && resourceName == null)) {
            resourceFilter = parseResourceFilter(resourceToken);
        } else if (resourceType != null || resourceName != null) {
            if (resourceType == null) {
                throw new IllegalArgumentException(
                        "resource_type is required when resource_name is set");
            }
            ResourceType type = ResourceType.fromName(resourceType);
            resourceFilter =
                    new ResourceFilter(
                            type,
                            resourceName == null ? null : SqlParserUtil.stripQuotes(resourceName));
        }

        FlussPrincipal principal = parsePrincipal(principalValue, principalType, true);
        OperationType operationType = parseOperationType(operation, true);
        PermissionType permissionType = parsePermissionType(permission, true);

        AccessControlEntryFilter entryFilter =
                new AccessControlEntryFilter(principal, host, operationType, permissionType);
        return new AclBindingFilter(resourceFilter, entryFilter);
    }

    private FlussPrincipal parsePrincipal(
            String principalValue, String principalType, boolean forFilter) {
        if (principalValue == null || principalValue.trim().isEmpty()) {
            return forFilter ? FlussPrincipal.ANY : null;
        }

        String trimmed = SqlParserUtil.stripQuotes(principalValue);
        String type = principalType == null ? "User" : SqlParserUtil.stripQuotes(principalType);

        if (trimmed.contains(":")) {
            String[] parts = trimmed.split(":", 2);
            type = parts[0];
            trimmed = parts[1];
        }

        if ("*".equals(trimmed) && "*".equals(type)) {
            return FlussPrincipal.WILD_CARD_PRINCIPAL;
        }

        if ("*".equals(trimmed)) {
            return new FlussPrincipal(trimmed, type);
        }

        return new FlussPrincipal(trimmed, type);
    }

    private ResourceFilter parseResourceFilter(String token) {
        String trimmed = SqlParserUtil.stripQuotes(token);
        if (trimmed.equals("*")) {
            return ResourceFilter.ANY;
        }
        if (!trimmed.contains(":")) {
            ResourceType type = ResourceType.fromName(trimmed);
            return new ResourceFilter(type, null);
        }
        String[] parts = trimmed.split(":", 2);
        ResourceType type = ResourceType.fromName(parts[0]);
        String name = parts[1];
        if (name.isEmpty() || "*".equals(name)) {
            return new ResourceFilter(type, null);
        }
        return new ResourceFilter(type, name);
    }

    private OperationType parseOperationType(String operation, boolean forFilter) {
        if (operation == null || operation.trim().isEmpty()) {
            return forFilter ? OperationType.ANY : null;
        }
        return OperationType.valueOf(SqlParserUtil.stripQuotes(operation).toUpperCase());
    }

    private PermissionType parsePermissionType(String permission, boolean forFilter) {
        if (permission == null || permission.trim().isEmpty()) {
            return forFilter ? PermissionType.ANY : null;
        }
        return PermissionType.valueOf(SqlParserUtil.stripQuotes(permission).toUpperCase());
    }

    private Resource parseResource(String resourceType, String resourceName, boolean forFilter) {
        ResourceType type = ResourceType.fromName(resourceType);
        if (type == ResourceType.ANY) {
            if (forFilter) {
                return Resource.any();
            }
            throw new IllegalArgumentException("resource_type ANY is only allowed in filters");
        }
        if (type == ResourceType.CLUSTER) {
            return Resource.cluster();
        }
        if (resourceName == null || resourceName.trim().isEmpty()) {
            if (forFilter) {
                return Resource.any();
            }
            throw new IllegalArgumentException("resource_name is required for " + resourceType);
        }
        String trimmedName = SqlParserUtil.stripQuotes(resourceName);
        if (Resource.WILDCARD_RESOURCE.equals(trimmedName)) {
            return new Resource(type, Resource.WILDCARD_RESOURCE);
        }
        return new Resource(type, trimmedName);
    }
}
