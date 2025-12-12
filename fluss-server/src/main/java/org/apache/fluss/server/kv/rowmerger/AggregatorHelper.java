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

package org.apache.fluss.server.kv.rowmerger;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import org.apache.fluss.config.TableConfig;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldAggregator;
import org.apache.fluss.server.kv.rowmerger.aggregate.factory.FieldAggregatorFactory;
import org.apache.fluss.server.kv.rowmerger.aggregate.factory.FieldLastNonNullValueAggFactory;
import org.apache.fluss.server.kv.rowmerger.aggregate.factory.FieldPrimaryKeyAggFactory;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.List;

/** Helper class for creating field aggregators based on table configuration. */
public class AggregatorHelper {

    /**
     * Creates an array of field aggregators for all fields in the schema.
     *
     * @param schema the row type schema
     * @param primaryKeys the list of primary key field names
     * @param tableConfig the table configuration
     * @return an array of field aggregators, one for each field
     */
    public static FieldAggregator[] createAggregators(
            RowType schema, List<String> primaryKeys, TableConfig tableConfig) {

        List<String> fieldNames = schema.getFieldNames();
        int fieldCount = schema.getFieldCount();

        FieldAggregator[] aggregators = new FieldAggregator[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = fieldNames.get(i);
            DataType fieldType = schema.getTypeAt(i);

            // Determine the aggregate function name for this field
            String aggFuncName = getAggFuncName(fieldName, primaryKeys, tableConfig);

            // Create the aggregator using the factory
            aggregators[i] =
                    FieldAggregatorFactory.create(fieldType, fieldName, aggFuncName, tableConfig);
        }

        return aggregators;
    }

    /**
     * Determines the aggregate function name for a field.
     *
     * <p>The priority is:
     *
     * <ol>
     *   <li>Primary key fields use "primary-key" (no aggregation)
     *   <li>Field-specific configuration: table.merge-engine.aggregate.&lt;field&gt;
     *   <li>Default configuration: table.merge-engine.aggregate.default-function
     *   <li>Final fallback: "last_non_null_value"
     * </ol>
     *
     * @param fieldName the field name
     * @param primaryKeys the list of primary key field names
     * @param tableConfig the table configuration
     * @return the aggregate function name to use
     */
    private static String getAggFuncName(
            String fieldName, List<String> primaryKeys, TableConfig tableConfig) {

        // 1. Primary key fields don't aggregate
        if (primaryKeys.contains(fieldName)) {
            return FieldPrimaryKeyAggFactory.NAME; // "primary-key"
        }

        // 2. Check field-specific configuration
        String fieldAggFunc = tableConfig.getFieldAggregateFunction(fieldName);
        if (fieldAggFunc != null) {
            return fieldAggFunc;
        }

        // 3. Check default configuration
        String defaultAggFunc = tableConfig.getDefaultAggregateFunction();
        if (defaultAggFunc != null) {
            return defaultAggFunc;
        }

        // 4. Final fallback
        return FieldLastNonNullValueAggFactory.NAME; // "last_non_null_value"
    }
}
