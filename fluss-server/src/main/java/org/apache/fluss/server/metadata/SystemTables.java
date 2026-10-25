/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.metadata;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TablePath;

/** Identifiers of tables managed internally by Fluss. */
@Internal
public final class SystemTables {

    /** Database containing Fluss internal system tables. */
    public static final String SYSTEM_DATABASE = "sys";

    /** Table storing committed consumer offsets. */
    public static final TablePath CONSUMER_OFFSETS =
            TablePath.of(SYSTEM_DATABASE, "consumer_offsets");

    private SystemTables() {}

    /** Returns whether the table belongs to the internal system database. */
    public static boolean isSystemTable(TablePath tablePath) {
        return SYSTEM_DATABASE.equals(tablePath.getDatabaseName());
    }
}
