/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.source.lookup;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.lookup.LookupType;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.utils.FlinkConversions;
import com.alibaba.fluss.connector.flink.utils.FlinkRowToFlussRowConverter;
import com.alibaba.fluss.connector.flink.utils.FlinkUtils;
import com.alibaba.fluss.connector.flink.utils.FlussRowToFlinkRowConverter;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.ProjectedRow;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** A flink lookup function for fluss. */
public class FlinkLookupFunction extends LookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final int maxRetryTimes;
    private final RowType flinkRowType;
    private final int[] lookupKeyIndexes;
    private final LookupNormalizer lookupNormalizer;
    @Nullable private final int[] projection;
    private final LookupType flussLookupType;

    private transient FlinkRowToFlussRowConverter flinkRowToFlussRowConverter;
    private transient FlussRowToFlinkRowConverter flussRowToFlinkRowConverter;
    private transient Connection connection;
    private transient Table table;
    @Nullable private transient ProjectedRow projectedRow;

    public FlinkLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            int[] lookupKeyIndexes,
            int maxRetryTimes,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection) {
        this.flussConfig = flussConfig;
        this.tablePath = tablePath;
        this.maxRetryTimes = maxRetryTimes;
        this.flinkRowType = flinkRowType;
        this.lookupKeyIndexes = lookupKeyIndexes;
        this.lookupNormalizer = lookupNormalizer;
        this.projection = projection;
        this.flussLookupType = lookupNormalizer.getFlussLookupType();
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        connection = ConnectionFactory.createConnection(flussConfig);
        table = connection.getTable(tablePath);
        // TODO: convert to Fluss GenericRow to avoid unnecessary deserialization
        flinkRowToFlussRowConverter =
                FlinkRowToFlussRowConverter.create(
                        FlinkUtils.projectRowType(flinkRowType, lookupKeyIndexes),
                        table.getDescriptor().getKvFormat());

        final RowType outputRowType;
        if (projection == null) {
            outputRowType = flinkRowType;
            projectedRow = null;
        } else {
            outputRowType = FlinkUtils.projectRowType(flinkRowType, projection);
            // reuse the projected row
            projectedRow = ProjectedRow.from(projection);
        }
        flussRowToFlinkRowConverter =
                new FlussRowToFlinkRowConverter(FlinkConversions.toFlussRowType(outputRowType));

        LOG.info("end open.");
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param keyRow - A {@link RowData} that wraps lookup keys. Currently only support single
     *     rowkey.
     */
    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        RowData normalizedKeyRow = lookupNormalizer.normalizeLookupKey(keyRow);
        LookupNormalizer.RemainingFilter remainingFilter =
                lookupNormalizer.createRemainingFilter(keyRow);
        // to lookup a key, we will need to do two data conversion,
        // first is converting from flink row to fluss row,
        // second is extracting key from the fluss row when calling method table.get(flussKeyRow)
        // todo: may be reduce to one data conversion when it's a bottle neck
        InternalRow flussKeyRow = flinkRowToFlussRowConverter.toInternalRow(normalizedKeyRow);
        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                if (flussLookupType == LookupType.LOOKUP) {
                    InternalRow row = table.lookup(flussKeyRow).get().getRow();
                    if (row != null) {
                        RowData flinkRow =
                                flussRowToFlinkRowConverter.toFlinkRowData(maybeProject(row));
                        if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                            return Collections.singletonList(flinkRow);
                        } else {
                            return Collections.emptyList();
                        }
                    }
                } else {
                    List<RowData> projectedRows = new ArrayList<>();
                    List<InternalRow> lookupRows =
                            table.prefixLookup(flussKeyRow).get().getRowList();
                    for (InternalRow row : lookupRows) {
                        if (row != null) {
                            RowData flinkRow =
                                    flussRowToFlinkRowConverter.toFlinkRowData(maybeProject(row));
                            if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                                projectedRows.add(flinkRow);
                            }
                        }
                    }
                    return projectedRows;
                }
            } catch (Exception e) {
                LOG.error(String.format("Fluss lookup error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    String exceptionMsg =
                            String.format(
                                    "Execution of Fluss lookup failed, retry times = %d.", retry);
                    throw new RuntimeException(exceptionMsg, e);
                }

                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(interruptedException);
                }
            }
        }
        return Collections.emptyList();
    }

    private InternalRow maybeProject(InternalRow row) {
        if (projectedRow == null) {
            return row;
        }
        return projectedRow.replaceRow(row);
    }

    @Override
    public void close() throws Exception {
        LOG.info("start close ...");
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
        LOG.info("end close.");
    }
}
