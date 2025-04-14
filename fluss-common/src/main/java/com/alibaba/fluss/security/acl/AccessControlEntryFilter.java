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

import java.util.Objects;

/**
 * Represents a filter which matches access control entries.
 *
 * <p>The API for this class is still evolving and we may break compatibility in minor releases, if
 * necessary.
 */
public class AccessControlEntryFilter {
    private final AccessControlEntry data;

    public static final AccessControlEntryFilter ANY =
            new AccessControlEntryFilter(
                    FlussPrincipal.ANY, null, OperationType.ANY, PermissionType.ANY);

    public AccessControlEntryFilter(
            FlussPrincipal principal,
            String host,
            OperationType operation,
            PermissionType permissionType) {
        Objects.requireNonNull(operation);
        Objects.requireNonNull(permissionType);
        this.data = new AccessControlEntry(principal, host, operation, permissionType);
    }

    /** Returns true if this filter matches the given AccessControlEntry. */
    public boolean matches(AccessControlEntry other) {
        if ((data.getPrincipal() != null)
                && data.getPrincipal() != FlussPrincipal.ANY
                && (!data.getPrincipal().equals(other.getPrincipal()))) {
            return false;
        }
        if ((data.getHost() != null) && (!data.getHost().equals(other.getHost()))) {
            return false;
        }
        if ((data.getOperationType() != OperationType.ANY)
                && (!data.getOperationType().equals(other.getOperationType()))) {
            return false;
        }
        if ((data.getPermissionType() != PermissionType.ANY)
                && (!data.getPermissionType().equals(other.getPermissionType()))) {
            return false;
        }
        return true;
    }

    public AccessControlEntry getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AccessControlEntryFilter)) {
            return false;
        }
        AccessControlEntryFilter other = (AccessControlEntryFilter) o;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    @Override
    public String toString() {
        return "AccessControlEntryFilter{" + "data=" + data + '}';
    }
}
