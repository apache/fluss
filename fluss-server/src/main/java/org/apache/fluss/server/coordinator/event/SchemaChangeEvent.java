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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TablePath;

import java.util.Objects;

/** An event for schema change. */
public class SchemaChangeEvent implements CoordinatorEvent {
    private final TablePath tablePath;
    private final SchemaInfo schemaInfo;

    public SchemaChangeEvent(TablePath tablePath, SchemaInfo schemaInfo) {
        this.tablePath = tablePath;
        this.schemaInfo = schemaInfo;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public String toString() {
        return "SchemaChangeEvent{" + "tablePath=" + tablePath + ", schemaInfo=" + schemaInfo + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SchemaChangeEvent)) {
            return false;
        }
        SchemaChangeEvent that = (SchemaChangeEvent) o;
        return Objects.equals(tablePath, that.tablePath)
                && Objects.equals(schemaInfo, that.schemaInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, schemaInfo);
    }
}
