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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.FlussPrincipal;

/** A testing implementation of {@link LakeCatalog.Context}. */
public class TestingLakeCatalogContext implements LakeCatalog.Context {

    private final TableDescriptor currentTable;
    private final TableDescriptor expectedTable;
    private final TablePath currentLakeTablePath;

    public TestingLakeCatalogContext(TableDescriptor tableDescriptor) {
        this(tableDescriptor, tableDescriptor);
    }

    public TestingLakeCatalogContext(TableDescriptor currentTable, TableDescriptor expectedTable) {
        this(currentTable, expectedTable, null);
    }

    private TestingLakeCatalogContext(
            TableDescriptor currentTable,
            TableDescriptor expectedTable,
            TablePath currentLakeTablePath) {
        this.currentTable = currentTable;
        this.expectedTable = expectedTable;
        this.currentLakeTablePath = currentLakeTablePath;
    }

    public TestingLakeCatalogContext() {
        this(null);
    }

    /** Creates a testing context with the lake table path currently associated with the table. */
    public static TestingLakeCatalogContext withCurrentLakeTablePath(
            TablePath currentLakeTablePath) {
        return new TestingLakeCatalogContext(null, null, currentLakeTablePath);
    }

    @Override
    public boolean isCreatingFlussTable() {
        return false;
    }

    @Override
    public FlussPrincipal getFlussPrincipal() {
        return null;
    }

    @Override
    public TableDescriptor getCurrentTable() {
        return currentTable;
    }

    @Override
    public TablePath getCurrentLakeTablePath() {
        return currentLakeTablePath == null
                ? LakeCatalog.Context.super.getCurrentLakeTablePath()
                : currentLakeTablePath;
    }

    @Override
    public TableDescriptor getExpectedTable() {
        return expectedTable;
    }
}
