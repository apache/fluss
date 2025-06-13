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

import com.alibaba.fluss.metadata.TablePath;

import java.util.Objects;

/**
 * Represents a resource object with type and name, used for ACL (Access Control List) management.
 *
 * @since 0.7
 */
public class Resource {
    public static final String WILDCARD_RESOURCE = "*";
    public static final String TABLE_SPLITTER = "\\.";

    public static final String FLUSS_CLUSTER = "fluss-cluster";
    private final ResourceType type;
    private final String name;

    public Resource(ResourceType type, String name) {
        this.type = type;
        this.name = name;
    }

    public static Resource of(String resourceType, String resourceName) {
        return new Resource(ResourceType.fromName(resourceType), resourceName);
    }

    public ResourceType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Resource resource = (Resource) o;
        return type == resource.type && Objects.equals(name, resource.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }

    @Override
    public String toString() {
        return "Resource{" + "type=" + type + ", name='" + name + '\'' + '}';
    }

    public static Resource any() {
        return new Resource(ResourceType.ANY, "any");
    }

    public static Resource cluster() {
        return new Resource(ResourceType.CLUSTER, FLUSS_CLUSTER);
    }

    public static Resource database(String databaseName) {
        return new Resource(ResourceType.DATABASE, databaseName);
    }

    public static Resource table(String databaseName, String tableName) {
        return new Resource(ResourceType.TABLE, databaseName + "." + tableName);
    }

    public static Resource table(TablePath tablePath) {
        return table(tablePath.getDatabaseName(), tablePath.getTableName());
    }
}
