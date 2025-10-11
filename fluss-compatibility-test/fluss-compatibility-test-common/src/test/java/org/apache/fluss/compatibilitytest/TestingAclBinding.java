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

package org.apache.fluss.compatibilitytest;

import java.util.Objects;

/** An version free AclBinding to describe fluss AclBinding. */
public class TestingAclBinding {
    public final Resource resource;
    public final AccessControlEntry accessControlEntry;

    public TestingAclBinding(Resource resource, AccessControlEntry accessControlEntry) {
        this.resource = resource;
        this.accessControlEntry = accessControlEntry;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestingAclBinding that = (TestingAclBinding) o;
        return resource.equals(that.resource) && accessControlEntry.equals(that.accessControlEntry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resource, accessControlEntry);
    }

    public static final class Resource {
        public ResourceType type;
        public String name;

        public Resource(ResourceType type, String name) {
            this.type = type;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Resource resource = (Resource) o;
            return type == resource.type && name.equals(resource.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, name);
        }
    }

    public static final class AccessControlEntry {
        public String principalName;
        public String principalType;
        public PermissionType permissionType;
        public String host;
        public OperationType operationType;

        public AccessControlEntry(
                String principalName,
                String principalType,
                PermissionType permissionType,
                String host,
                OperationType operationType) {
            this.principalName = principalName;
            this.principalType = principalType;
            this.permissionType = permissionType;
            this.host = host;
            this.operationType = operationType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AccessControlEntry that = (AccessControlEntry) o;
            return principalName.equals(that.principalName)
                    && principalType.equals(that.principalType)
                    && permissionType == that.permissionType
                    && host.equals(that.host)
                    && operationType == that.operationType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(principalName, principalType, permissionType, host, operationType);
        }
    }

    public enum ResourceType {
        ANY,
        CLUSTER,
        DATABASE,
        TABLE
    }

    public enum PermissionType {
        ANY,
        ALLOW
    }

    public enum OperationType {
        ANY,
        ALL,
        READ,
        WRITE,
        CREATE,
        DROP,
        ALTER,
        DESCRIBE
    }
}
