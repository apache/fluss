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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Objects;

/** Identity of a named MVCC read snapshot in one physical table. */
@PublicEvolving
public final class MvccSnapshotRef {
    private final long tableId;
    private final String namespace;
    private final String name;

    private MvccSnapshotRef(long tableId, String namespace, String name) {
        this.tableId = tableId;
        this.namespace = Objects.requireNonNull(namespace);
        this.name = Objects.requireNonNull(name);
    }

    /** References a named snapshot in the default namespace. */
    public static MvccSnapshotRef of(long tableId, String name) {
        return of(tableId, "default", name);
    }

    /** References a named snapshot in the specified namespace. */
    public static MvccSnapshotRef of(long tableId, String namespace, String name) {
        return new MvccSnapshotRef(tableId, namespace, name);
    }

    /** Returns the physical table id. */
    public long getTableId() {
        return tableId;
    }

    /** Returns the namespace. */
    public String getNamespace() {
        return namespace;
    }

    /** Returns the opaque snapshot name. */
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MvccSnapshotRef)) {
            return false;
        }
        MvccSnapshotRef ref = (MvccSnapshotRef) other;
        return tableId == ref.tableId && namespace.equals(ref.namespace) && name.equals(ref.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, namespace, name);
    }

    @Override
    public String toString() {
        return tableId + "/" + namespace + "/" + name;
    }
}
