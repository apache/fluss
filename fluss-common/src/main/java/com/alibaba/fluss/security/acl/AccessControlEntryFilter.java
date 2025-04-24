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

import javax.annotation.Nullable;

import java.util.Objects;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Represents a filter which matches access control entries.
 *
 * <p>The API for this class is still evolving and we may break compatibility in minor releases, if
 * necessary.
 *
 * @since 0.7
 */
@PublicEvolving
public class AccessControlEntryFilter {
    @Nullable private final FlussPrincipal principal;
    private final PermissionType permissionType;
    @Nullable private final String host;
    private final OperationType operationType;

    public static final AccessControlEntryFilter ANY =
            new AccessControlEntryFilter(
                    FlussPrincipal.ANY, null, OperationType.ANY, PermissionType.ANY);

    public AccessControlEntryFilter(
            @Nullable FlussPrincipal principal,
            @Nullable String host,
            OperationType operation,
            PermissionType permissionType) {
        this.principal = principal;
        this.host = host;
        this.operationType = checkNotNull(operation);
        this.permissionType = checkNotNull(permissionType);
    }

    /** Returns true if this filter matches the given AccessControlEntry. */
    public boolean matches(AccessControlEntry other) {
        if ((principal != null)
                && principal != FlussPrincipal.ANY
                && (!principal.equals(other.getPrincipal()))) {
            return false;
        }
        if ((host != null) && (!host.equals(other.getHost()))) {
            return false;
        }
        if ((operationType != OperationType.ANY)
                && (!operationType.equals(other.getOperationType()))) {
            return false;
        }
        if ((permissionType != PermissionType.ANY)
                && (!permissionType.equals(other.getPermissionType()))) {
            return false;
        }
        return true;
    }

    public @Nullable FlussPrincipal getPrincipal() {
        return principal;
    }

    public @Nullable String getHost() {
        return host;
    }

    public PermissionType getPermissionType() {
        return permissionType;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AccessControlEntryFilter that = (AccessControlEntryFilter) o;
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
        return "AccessControlEntryFilter{"
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
