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

/** Resolved mapping between a Trino schema name and the physical Fluss database name. */
final class ResolvedSchemaName {

    private final String trinoName;
    private final String flussName;

    ResolvedSchemaName(String trinoName, String flussName) {
        this.trinoName = checkNotNull(trinoName, "trinoName is null");
        this.flussName = checkNotNull(flussName, "flussName is null");
    }

    String getTrinoName() {
        return trinoName;
    }

    String getFlussName() {
        return flussName;
    }

    ResolvedTableName table(String trinoTableName, String flussTableName) {
        return new ResolvedTableName(
                new SchemaTableName(trinoName, trinoTableName), flussName, flussTableName);
    }
}
