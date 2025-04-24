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

/**
 * A filter which matches Resource objects.
 *
 * @since 0.7
 */
@PublicEvolving
public class ResourceFilter {
    private final ResourceType type;
    @Nullable private final String name;

    public static final ResourceFilter ANY = new ResourceFilter(ResourceType.ANY, null);

    public ResourceFilter(ResourceType type, @Nullable String name) {
        Objects.requireNonNull(type);
        this.type = type;
        this.name = name;
    }

    public ResourceType getType() {
        return type;
    }

    public @Nullable String getName() {
        return name;
    }

    public boolean matches(Resource other) {
        if ((name != null) && (!name.equals(other.getName()))) {
            return false;
        }
        return (type == ResourceType.ANY) || (type.equals(other.getType()));
    }

    @Override
    public String toString() {
        return "(resourceType=" + type + ", name=" + ((name == null) ? "<any>" : name) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ResourceFilter)) {
            return false;
        }
        ResourceFilter other = (ResourceFilter) o;
        return type.equals(other.type) && Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }
}
