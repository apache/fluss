/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.system;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;

/**
 * Lightweight metadata definition of a system view.
 *
 * <p>A system view definition describes the view's path, schema, and schema version without
 * providing the ability to produce data. This allows all servers (coordinator and tablet) to be
 * aware of which system views exist for metadata operations such as {@code listTables}, {@code
 * tableExists}, and {@code getTableInfo}, regardless of whether the server can actually serve the
 * view's data.
 *
 * <p>To produce data, a {@link SystemViewProvider} implementation is needed, which extends this
 * interface with a {@code scanRows} method.
 *
 * @see SystemViewProvider
 */
@Internal
public interface SystemViewDefinition {

    /** Returns the table path for this system view (e.g., {@code sys.tablet_servers}). */
    TablePath viewPath();

    /** Returns the schema for this system view. */
    Schema schema();

    /**
     * Returns the schema version for this system view.
     *
     * <p>Schema evolution is append-only: new columns may be added at the end. The version starts
     * at 0 and increments with each schema change. Clients using {@code KvFormat.INDEXED} can
     * safely decode records with trailing fields they don't know about.
     *
     * @return the schema version, defaults to 0
     */
    default int schemaId() {
        return 0;
    }

    /** Returns the table descriptor for this system view. */
    default TableDescriptor tableDescriptor() {
        return TableDescriptor.builder()
                .schema(schema())
                .comment("System view: " + viewPath().getTableName())
                .distributedBy(1)
                .build();
    }
}
