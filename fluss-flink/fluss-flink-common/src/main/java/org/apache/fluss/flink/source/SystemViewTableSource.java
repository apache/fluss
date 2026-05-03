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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanUtils;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.flink.utils.PredicateConverter.convertToFlussPredicate;

/**
 * A Flink table source for system views.
 *
 * <p>System views are virtual tables that only support bounded batch scans. This source fetches all
 * rows from the coordinator server via the client {@link BatchScanner} API and emits them as a
 * bounded collection.
 */
public class SystemViewTableSource
        implements ScanTableSource, SupportsProjectionPushDown, SupportsFilterPushDown {

    private final TablePath tablePath;
    private final Configuration flussConfig;
    private final org.apache.flink.table.types.logical.RowType tableOutputType;

    @Nullable private int[] projectedFields;
    private LogicalType producedDataType;
    @Nullable private Predicate filterPredicate;

    public SystemViewTableSource(
            TablePath tablePath,
            Configuration flussConfig,
            org.apache.flink.table.types.logical.RowType tableOutputType) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.tableOutputType = tableOutputType;
        this.producedDataType = tableOutputType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        Collection<RowData> results = scanSystemView();
        TypeInformation<RowData> resultTypeInfo =
                scanContext.createTypeInformation(producedDataType);
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                return execEnv.fromCollection(results, resultTypeInfo);
            }

            @Override
            public boolean isBounded() {
                return true;
            }
        };
    }

    private Collection<RowData> scanSystemView() {
        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Table table = connection.getTable(tablePath);
                BatchScanner batchScanner =
                        table.newScan()
                                .project(projectedFields)
                                .filter(filterPredicate)
                                .createBatchScanner()) {
            List<InternalRow> scannedRows = BatchScanUtils.collectRows(batchScanner);

            RowType flussRowType = FlinkConversions.toFlussRowType(tableOutputType);
            RowType converterRowType =
                    projectedFields != null ? flussRowType.project(projectedFields) : flussRowType;
            FlussRowToFlinkRowConverter converter =
                    new FlussRowToFlinkRowConverter(converterRowType);

            List<RowData> results = new ArrayList<>();
            for (InternalRow row : scannedRows) {
                results.add(converter.toFlinkRowData(row));
            }
            return results;
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to scan system view '" + tablePath + "'.", e);
        }
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.projectedFields = Arrays.stream(projectedFields).mapToInt(value -> value[0]).toArray();
        this.producedDataType = producedDataType.getLogicalType();
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        List<Predicate> converted = new ArrayList<>();
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        for (ResolvedExpression filter : filters) {
            Optional<Predicate> predicateOpt = convertToFlussPredicate(tableOutputType, filter);
            if (predicateOpt.isPresent()) {
                converted.add(predicateOpt.get());
                acceptedFilters.add(filter);
            }
        }
        filterPredicate = converted.isEmpty() ? null : PredicateBuilder.and(converted);
        return Result.of(acceptedFilters, filters);
    }

    @Override
    public DynamicTableSource copy() {
        SystemViewTableSource copy =
                new SystemViewTableSource(tablePath, flussConfig, tableOutputType);
        copy.projectedFields = projectedFields;
        copy.producedDataType = producedDataType;
        copy.filterPredicate = filterPredicate;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "FlussSystemViewTableSource";
    }
}
