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
import com.alibaba.fluss.client.lookup.LookupResult;
import com.alibaba.fluss.client.lookup.LookupType;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.lookup.PrefixLookup;
import com.alibaba.fluss.client.lookup.PrefixLookupResult;
import com.alibaba.fluss.client.lookup.PrefixLookuper;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.source.lookup.LookupNormalizer.RemainingFilter;
import com.alibaba.fluss.connector.flink.utils.FlinkConversions;
import com.alibaba.fluss.connector.flink.utils.FlinkRowToFlussRowConverter;
import com.alibaba.fluss.connector.flink.utils.FlinkUtils;
import com.alibaba.fluss.connector.flink.utils.FlussRowToFlinkRowConverter;
import com.alibaba.fluss.exception.TableNotExistException;
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

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

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
    private final LookupType flussLookupType;

    private transient FlinkRowToFlussRowConverter flinkRowToFlussRowConverter;
    private transient FlussRowToFlinkRowConverter flussRowToFlinkRowConverter;
    private transient Connection connection;
    private transient Table table;
    @Nullable private transient PrefixLookuper prefixLookuper;
    @Nullable private transient Lookuper lookuper;

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
        this.flussLookupType = lookupNormalizer.getLookupType();
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        connection = ConnectionFactory.createConnection(flussConfig);
        table = connection.getTable(tablePath);
        // TODO: convert to Fluss GenericRow to avoid unnecessary deserialization
        int[] lookupKeyIndexes = lookupNormalizer.getLookupKeyIndexes();
        RowType lookupKeyRowType = FlinkUtils.projectRowType(flinkRowType, lookupKeyIndexes);
        flinkRowToFlussRowConverter =
                FlinkRowToFlussRowConverter.create(
                        lookupKeyRowType, table.getDescriptor().getKvFormat());

        final RowType outputRowType;
        if (projection == null) {
            outputRowType = flinkRowType;
        } else {
            outputRowType = FlinkUtils.projectRowType(flinkRowType, projection);
        }
        flussRowToFlinkRowConverter =
                new FlussRowToFlinkRowConverter(FlinkConversions.toFlussRowType(outputRowType));

        if (flussLookupType == LookupType.PREFIX_LOOKUP) {
            prefixLookuper =
                    table.getPrefixLookuper(
                            new PrefixLookup(
                                    lookupKeyRowType.getFieldNames().toArray(new String[0])));
            lookuper = null;
        } else if (flussLookupType == LookupType.LOOKUP) {
            prefixLookuper = null;
            lookuper = table.getLookuper();
        }

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
        InternalRow flussKeyRow = flinkRowToFlussRowConverter.toInternalRow(normalizedKeyRow);
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
        if (flussLookupType == LookupType.LOOKUP) {
            checkArgument(lookuper != null, "Lookuper should not be null");
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
                                    handleLookupSuccess(resultFuture, result, remainingFilter);
                                }
                            });
        } else {
            checkArgument(prefixLookuper != null, "prefixLookuper should not be null");
            prefixLookuper
                    .prefixLookup(keyRow)
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
                                    handlePrefixLookupSuccess(
                                            resultFuture, result, remainingFilter);
                                }
                            });
        }
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
            LookupResult result,
            @Nullable RemainingFilter remainingFilter) {
        InternalRow row = result.getRow();
        if (row == null) {
            resultFuture.complete(Collections.emptyList());
        } else {
            RowData flinkRow = flussRowToFlinkRowConverter.toFlinkRowData(maybeProject(row));
            if (remainingFilter != null && !remainingFilter.isMatch(flinkRow)) {
                resultFuture.complete(Collections.emptyList());
            } else {
                resultFuture.complete(Collections.singletonList(flinkRow));
            }
        }
    }

    private void handlePrefixLookupSuccess(
            CompletableFuture<Collection<RowData>> resultFuture,
            PrefixLookupResult result,
            @Nullable RemainingFilter remainingFilter) {
        List<RowData> projectedRow = new ArrayList<>();
        for (InternalRow row : result.getRowList()) {
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
