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

package com.alibaba.fluss.flink.source.lookup;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.lookup.Lookup;
import com.alibaba.fluss.client.lookup.LookupType;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.flink.row.FlinkAsFlussRow;
import com.alibaba.fluss.flink.source.lookup.LookupNormalizer.RemainingFilter;
import com.alibaba.fluss.flink.utils.FlinkConversions;
import com.alibaba.fluss.flink.utils.FlinkUtils;
import com.alibaba.fluss.flink.utils.FlussRowToFlinkRowConverter;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.ProjectedRow;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** A flink async lookup function for fluss. */
public class FlinkAsyncLookupFunction extends AsyncLookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkAsyncLookupFunction.class);

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final int maxRetryTimes;
    private final RowType flinkRowType;
    private final LookupNormalizer lookupNormalizer;
    @Nullable private final int[] projection;

    private transient FlussRowToFlinkRowConverter flussRowToFlinkRowConverter;
    private transient Connection connection;
    private transient Table table;
    private transient Lookuper lookuper;
    private transient FlinkAsFlussRow lookupRow;

    public FlinkAsyncLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            int maxRetryTimes,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection) {
        this.flussConfig = flussConfig;
        this.tablePath = tablePath;
        this.maxRetryTimes = maxRetryTimes;
        this.flinkRowType = flinkRowType;
        this.lookupNormalizer = lookupNormalizer;
        this.projection = projection;
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        connection = ConnectionFactory.createConnection(flussConfig);
        table = connection.getTable(tablePath);
        lookupRow = new FlinkAsFlussRow();

        final RowType outputRowType;
        if (projection == null) {
            outputRowType = flinkRowType;
        } else {
            outputRowType = FlinkUtils.projectRowType(flinkRowType, projection);
        }
        flussRowToFlinkRowConverter =
                new FlussRowToFlinkRowConverter(FlinkConversions.toFlussRowType(outputRowType));

        Lookup lookup = table.newLookup();
        if (lookupNormalizer.getLookupType() == LookupType.PREFIX_LOOKUP) {
            int[] lookupKeyIndexes = lookupNormalizer.getLookupKeyIndexes();
            RowType lookupKeyRowType = FlinkUtils.projectRowType(flinkRowType, lookupKeyIndexes);
            lookup = lookup.lookupBy(lookupKeyRowType.getFieldNames());
        }
        lookuper = lookup.createLookuper();

        LOG.info("end open.");
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param keyRow A {@link RowData} that wraps lookup keys.
     */
    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        RowData normalizedKeyRow = lookupNormalizer.normalizeLookupKey(keyRow);
        RemainingFilter remainingFilter = lookupNormalizer.createRemainingFilter(keyRow);
        InternalRow flussKeyRow = lookupRow.replace(normalizedKeyRow);
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        // fetch result
        fetchResult(future, 0, flussKeyRow, remainingFilter);
        return future;
    }

    /**
     * Execute async fetch result .
     *
     * @param resultFuture The result or exception is returned.
     * @param currentRetry Current number of retries.
     * @param keyRow the key row to get.
     * @param remainingFilter the nullable remaining filter to filter the result.
     */
    private void fetchResult(
            CompletableFuture<Collection<RowData>> resultFuture,
            int currentRetry,
            InternalRow keyRow,
            @Nullable RemainingFilter remainingFilter) {
        lookuper.lookup(keyRow)
                .whenComplete(
                        (result, throwable) -> {
                            if (throwable != null) {
                                handleLookupFailed(
                                        resultFuture,
                                        throwable,
                                        currentRetry,
                                        keyRow,
                                        remainingFilter);
                            } else {
                                handleLookupSuccess(
                                        resultFuture, result.getRowList(), remainingFilter);
                            }
                        });
    }

    private void handleLookupFailed(
            CompletableFuture<Collection<RowData>> resultFuture,
            Throwable throwable,
            int currentRetry,
            InternalRow keyRow,
            @Nullable RemainingFilter remainingFilter) {
        if (throwable instanceof TableNotExistException) {
            LOG.error("Table '{}' not found ", tablePath, throwable);
            resultFuture.completeExceptionally(
                    new RuntimeException("Fluss table '" + tablePath + "' not found.", throwable));
        } else {
            LOG.error("Fluss asyncLookup error, retry times = {}", currentRetry, throwable);
            if (currentRetry >= maxRetryTimes) {
                String exceptionMsg =
                        String.format(
                                "Execution of Fluss asyncLookup failed: %s, retry times = %d.",
                                throwable.getMessage(), currentRetry);
                resultFuture.completeExceptionally(new RuntimeException(exceptionMsg, throwable));
            } else {
                try {
                    Thread.sleep(1000L * currentRetry);
                } catch (InterruptedException e1) {
                    resultFuture.completeExceptionally(e1);
                }
                fetchResult(resultFuture, currentRetry + 1, keyRow, remainingFilter);
            }
        }
    }

    private void handleLookupSuccess(
            CompletableFuture<Collection<RowData>> resultFuture,
            List<InternalRow> lookupResult,
            @Nullable RemainingFilter remainingFilter) {
        if (lookupResult.isEmpty()) {
            resultFuture.complete(Collections.emptyList());
            return;
        }

        List<RowData> projectedRow = new ArrayList<>();
        for (InternalRow row : lookupResult) {
            if (row != null) {
                RowData flinkRow = flussRowToFlinkRowConverter.toFlinkRowData(maybeProject(row));
                if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                    projectedRow.add(flinkRow);
                }
            }
        }
        resultFuture.complete(projectedRow);
    }

    private InternalRow maybeProject(InternalRow row) {
        if (projection == null) {
            return row;
        }
        // should not reuse objects for async operations
        return ProjectedRow.from(projection).replaceRow(row);
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
