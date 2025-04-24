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

package com.alibaba.fluss.security.acl;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.Objects;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Represents an Access Control List (ACL) entry defining permissions for a principal on a specific
 * resource operation.
 *
 * <p>This class encapsulates the core elements of an ACL entry:
 *
 * <p>- Principal: User/role identifier
 *
 * <p>- Permission: Allow or deny access
 *
 * <p>- Host: Source host restriction (can be "*" for any host)
 *
 * <p>- Operation: Specific operation type (e.g., read/write)
 *
 * @since 0.7
 */
@PublicEvolving
public class AccessControlEntry {
    public static final String WILD_CARD_HOST = "*";

    private final FlussPrincipal principal;
    private final PermissionType permissionType;
    private final String host;
    private final OperationType operationType;

    public AccessControlEntry(
            FlussPrincipal principal,
            String host,
            OperationType operationType,
            PermissionType permissionType) {
        this.principal = checkNotNull(principal);
        this.host = checkNotNull(host);
        this.permissionType = checkNotNull(permissionType);
        this.operationType = checkNotNull(operationType);
    }

    public FlussPrincipal getPrincipal() {
        return principal;
    }

    public PermissionType getPermissionType() {
        return permissionType;
    }

    public String getHost() {
        return host;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AccessControlEntry that = (AccessControlEntry) o;
        return Objects.equals(principal, that.principal)
                && permissionType == that.permissionType
                && Objects.equals(host, that.host)
                && operationType == that.operationType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, permissionType, host, operationType);
    }

    @Override
    public String toString() {
        return "AccessControlEntry{"
                + "principal="
                + principal
                + ", permissionType="
                + permissionType
                + ", host='"
                + host
                + '\''
                + ", operationType="
                + operationType
                + '}';
    }
}
