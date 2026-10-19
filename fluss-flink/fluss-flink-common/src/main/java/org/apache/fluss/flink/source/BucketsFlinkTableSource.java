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

package org.apache.fluss.flink.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/** A bounded Flink table source for the read-only {@code $buckets} virtual table. */
public class BucketsFlinkTableSource implements ScanTableSource {

    private final TablePath tablePath;
    private final Configuration flussConfig;
    private final RowType producedDataType;

    /** Creates a source for the bucket metadata of the given base table. */
    public BucketsFlinkTableSource(
            TablePath tablePath, Configuration flussConfig, RowType producedDataType) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        TypeInformation<RowData> resultTypeInfo =
                scanContext.createTypeInformation(producedDataType);
        return SourceProvider.of(new BucketsSource(tablePath, flussConfig, resultTypeInfo));
    }

    @Override
    public DynamicTableSource copy() {
        return new BucketsFlinkTableSource(tablePath, flussConfig, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Fluss Buckets Table Source";
    }
}
