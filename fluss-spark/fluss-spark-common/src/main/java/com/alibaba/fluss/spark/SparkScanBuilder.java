/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.spark;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.spark.initializer.OffsetsInitializer;
import com.alibaba.fluss.spark.utils.SparkTypeUtils;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.List;

/** A Spark {@link ScanBuilder} for paimon. */
public class SparkScanBuilder
        implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

    private final Configuration flussConfig;
    private final TableInfo table;

    private OffsetsInitializer offsetsInitializer;
    private final CaseInsensitiveStringMap options;

    private Filter[] pushedFilters;
    private StructType requiredSchema;
    private int[] projectedFields;

    public SparkScanBuilder(
            TableInfo table, Configuration flussConfig, CaseInsensitiveStringMap options) {
        this.table = table;
        this.flussConfig = flussConfig;
        this.options = options;
        this.requiredSchema = SparkTypeUtils.fromFlussRowType(table.getRowType());
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        pushedFilters = filters;
        return filters;
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        String[] pruneFields = requiredSchema.fieldNames();
        List<String> fieldNames = table.getRowType().getFieldNames();
        int[] projected = new int[pruneFields.length];
        for (int i = 0; i < projected.length; i++) {
            projected[i] = fieldNames.indexOf(pruneFields[i]);
        }
        this.projectedFields = projected;
        this.requiredSchema = requiredSchema;
    }

    @Override
    public Scan build() {
        return new SparkScan(flussConfig, table, options, requiredSchema, projectedFields);
    }
}
