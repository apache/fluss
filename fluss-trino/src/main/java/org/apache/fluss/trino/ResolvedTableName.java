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

package org.apache.fluss.trino;

import io.trino.spi.connector.SchemaTableName;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Resolved mapping between a Trino table name and its physical Fluss table identity. */
final class ResolvedTableName {

    private final SchemaTableName trinoName;
    private final String flussDatabaseName;
    private final String flussTableName;

    ResolvedTableName(SchemaTableName trinoName, String flussDatabaseName, String flussTableName) {
        this.trinoName = checkNotNull(trinoName, "trinoName is null");
        this.flussDatabaseName = checkNotNull(flussDatabaseName, "flussDatabaseName is null");
        this.flussTableName = checkNotNull(flussTableName, "flussTableName is null");
    }

    SchemaTableName getTrinoName() {
        return trinoName;
    }

    String getFlussDatabaseName() {
        return flussDatabaseName;
    }

    String getFlussTableName() {
        return flussTableName;
    }
}
