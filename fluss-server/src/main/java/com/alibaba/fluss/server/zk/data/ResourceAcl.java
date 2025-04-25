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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.security.acl.AccessControlEntry;

import java.util.Objects;
import java.util.Set;

/**
 * The registration information of acl in {@link ZkData.ResourceAclNode}. It is used to store the
 * acl information in zookeeper.
 *
 * @see ResourceAclJsonSerde for json serialization and deserialization.
 */
public class ResourceAcl {
    private final Set<AccessControlEntry> entries;

    public ResourceAcl(Set<AccessControlEntry> entries) {
        this.entries = entries;
    }

    public Set<AccessControlEntry> getEntries() {
        return entries;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceAcl that = (ResourceAcl) o;
        return Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(entries);
    }

    @Override
    public String toString() {
        return "AclRegistration{" + "entries=" + entries + '}';
    }
}
