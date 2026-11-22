/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.testutils;

import org.apache.fluss.metadata.TablePath;

import java.util.UUID;

/**
 * Table names scoped to one test invocation sharing a cluster with other tests.
 *
 * <p>Keep an instance field in a test using JUnit's per-method lifecycle. The same logical name
 * remains stable within that invocation, including intentional drop-and-recreate operations. For
 * reused test instances, renew the helper in per-test setup before deriving any names.
 */
public final class TestTableNames {

    private final String suffix = "_" + UUID.randomUUID().toString().replace("-", "");

    /** Returns this invocation's name for the given logical table name. */
    public String tableName(String name) {
        return name + suffix;
    }

    /** Returns a table path with the database preserved and the table name scoped. */
    public TablePath tablePath(String databaseName, String tableName) {
        return TablePath.of(databaseName, tableName(tableName));
    }

    /** Returns a scoped copy of a shared table-path fixture. */
    public TablePath tablePath(TablePath template) {
        return tablePath(template.getDatabaseName(), template.getTableName());
    }
}
