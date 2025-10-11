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

import java.util.List;
import java.util.Map;

/** An version free table descriptor to describe fluss table. */
public class TestingTableDescriptor {
    public static final String BOOLEAN_TYPE = "BOOLEAN";
    public static final String INT_TYPE = "INT";
    public static final String BIGINT_TYPE = "BIGINT";
    public static final String STRING_TYPE = "STRING";
    public static final String TINYINT_TYPE = "TINYINT";

    private final String dbName;
    private final String tableName;
    private final List<String> columns;
    /**
     * The value of columns need to be one of the following types: BOOLEAN, TINYINT, SMALLINT, INT,
     * BIGINT, FLOAT, DOUBLE, DECIMAL, VARCHAR, CHAR, DATE, TIMESTAMP
     */
    private final List<String> columnTypes;

    private final List<String> primaryKeys;
    private final List<String> partitionKeys;
    private final Map<String, String> properties;

    public TestingTableDescriptor(
            String dbName,
            String tableName,
            List<String> columns,
            List<String> columnTypes,
            List<String> primaryKeys,
            List<String> partitionKeys,
            Map<String, String> properties) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.properties = properties;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
