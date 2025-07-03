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

package com.alibaba.fluss.security.acl;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.Objects;

/**
 * Represents an Access Control List (ACL) binding that associates a resource with specific access
 * control rules.
 *
 * <p>An {@code AclBinding} encapsulates the relationship between a {@link Resource} and an {@link
 * AccessControlEntry}, defining which principal (user/role) has what permissions on the resource.
 *
 * @since 0.7
 */
@PublicEvolving
public class AclBinding {
    private final Resource resource;
    private final AccessControlEntry accessControlEntry;

    public AclBinding(Resource resource, AccessControlEntry accessControlEntry) {
        this.resource = resource;
        this.accessControlEntry = accessControlEntry;
    }

    public Resource getResource() {
        return resource;
    }

    public AccessControlEntry getAccessControlEntry() {
        return accessControlEntry;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AclBinding that = (AclBinding) o;
        return Objects.equals(resource, that.resource)
                && Objects.equals(accessControlEntry, that.accessControlEntry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resource, accessControlEntry);
    }

    @Override
    public String toString() {
        return "AclBinding{"
                + "resource="
                + resource
                + ", accessControlEntry="
                + accessControlEntry
                + '}';
    }
}
