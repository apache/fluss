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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.LookupType;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.source.lookup.LookupNormalizer.RemainingFilter;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.utils.ExceptionUtils;

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
import java.util.stream.IntStream;

/** A flink async lookup function for fluss. */
public class FlinkAsyncLookupFunction extends AsyncLookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkAsyncLookupFunction.class);

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final RowType flinkRowType;
    private final LookupNormalizer lookupNormalizer;
    @Nullable private int[] projection;

    private transient FlussRowToFlinkRowConverter flussRowToFlinkRowConverter;
    private transient Connection connection;
    private transient Table table;
    private transient Lookuper lookuper;
    private transient FlinkAsFlussRow lookupRow;

    public FlinkAsyncLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection) {
        this.flussConfig = flussConfig;
        this.tablePath = tablePath;
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
            // we force to do projection if no projection pushdown, in order to handle schema
            // changes (ADD COLUMN LAST), this guarantees the input row of
            // flussRowToFlinkRowConverter is in expected schema even new columns are added.
            projection = IntStream.range(0, flinkRowType.getFieldCount()).toArray();
        } else {
            outputRowType = FlinkUtils.projectRowType(flinkRowType, projection);
        }
        // TODO: currently, we assume only ADD COLUMN LAST schema changes, so the projection
        //  positions can still work even after such changes.
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

        // the retry mechanism is now handled by the underlying LookupClient layer,
        // we can't call lookuper.lookup() in whenComplete callback as lookuper is not thread-safe.
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        lookuper.lookup(flussKeyRow)
                .whenComplete(
                        (result, throwable) -> {
                            if (throwable != null) {
                                if (ExceptionUtils.findThrowable(
                                                throwable, TableNotExistException.class)
                                        .isPresent()) {
                                    LOG.error("Table '{}' not found ", tablePath, throwable);
                                    future.completeExceptionally(
                                            new RuntimeException(
                                                    "Fluss table '" + tablePath + "' not found.",
                                                    throwable));
                                } else {
                                    LOG.error("Fluss asyncLookup error", throwable);
                                    future.completeExceptionally(
                                            new RuntimeException(
                                                    "Execution of Fluss asyncLookup failed: "
                                                            + throwable.getMessage(),
                                                    throwable));
                                }
                            } else {
                                handleLookupSuccess(future, result.getRowList(), remainingFilter);
                            }
                        });
        return future;
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
