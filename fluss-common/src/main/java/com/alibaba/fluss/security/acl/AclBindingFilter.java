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

/**
 * A filter which can match AclBinding objects.
 *
 * @since 0.7
 */
@PublicEvolving
public class AclBindingFilter {
    private final ResourceFilter resourceFilter;
    private final AccessControlEntryFilter entryFilter;

    /** A filter which matches any ACL binding. */
    public static final AclBindingFilter ANY =
            new AclBindingFilter(ResourceFilter.ANY, AccessControlEntryFilter.ANY);

    public AclBindingFilter(ResourceFilter resourceFilter, AccessControlEntryFilter entryFilter) {
        Objects.requireNonNull(resourceFilter);
        this.resourceFilter = resourceFilter;
        Objects.requireNonNull(entryFilter);
        this.entryFilter = entryFilter;
    }

    public boolean matches(AclBinding binding) {
        return resourceFilter.matches(binding.getResource())
                && entryFilter.matches(binding.getAccessControlEntry());
    }

    public ResourceFilter getResourceFilter() {
        return resourceFilter;
    }

    public AccessControlEntryFilter getEntryFilter() {
        return entryFilter;
    }

    @Override
    public String toString() {
        return "(resourceFilter=" + resourceFilter + ", entryFilter=" + entryFilter + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AclBindingFilter)) {
            return false;
        }
        AclBindingFilter other = (AclBindingFilter) o;
        return resourceFilter.equals(other.resourceFilter) && entryFilter.equals(other.entryFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceFilter, entryFilter);
    }
}
