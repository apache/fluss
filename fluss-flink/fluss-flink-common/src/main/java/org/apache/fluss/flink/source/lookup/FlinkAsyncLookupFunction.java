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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A flink async lookup function for fluss. */
public class FlinkAsyncLookupFunction extends AsyncLookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkAsyncLookupFunction.class);

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final LookupNormalizer lookupNormalizer;
    private final FlussLookupRuntime flussLookupRuntime;
    private transient LookupResultConverter lookupResultConverter;

    public FlinkAsyncLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection,
            boolean insertIfNotExists) {
        this.tablePath = tablePath;
        this.lookupNormalizer = lookupNormalizer;
        this.flussLookupRuntime =
                new FlussLookupRuntime(
                        flussConfig, tablePath, flinkRowType, lookupNormalizer, insertIfNotExists);

        int[] resolvedProjection =
                projection == null
                        ? IntStream.range(0, flinkRowType.getFieldCount()).toArray()
                        : projection;
        RowType outputRowType = FlinkUtils.projectRowType(flinkRowType, resolvedProjection);
        lookupResultConverter =
                new LookupResultConverter(
                        FlinkConversions.toFlussRowType(outputRowType), resolvedProjection);
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        flussLookupRuntime.open();
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
        LookupNormalizer.RemainingFilter remainingFilter =
                lookupNormalizer.createRemainingFilter(keyRow);

        // the retry mechanism is now handled by the underlying LookupClient layer,
        // we can't call lookuper.lookup() in whenComplete callback as lookuper is not thread-safe.
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        try {
            flussLookupRuntime
                    .lookup(normalizedKeyRow)
                    .whenComplete(
                            (result, throwable) -> {
                                try {
                                    if (throwable != null) {
                                        if (ExceptionUtils.findThrowable(
                                                        throwable, TableNotExistException.class)
                                                .isPresent()) {
                                            LOG.error(
                                                    "Table '{}' not found ", tablePath, throwable);
                                            future.completeExceptionally(
                                                    new RuntimeException(
                                                            "Fluss table '"
                                                                    + tablePath
                                                                    + "' not found.",
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
                                        future.complete(
                                                checkNotNull(
                                                                lookupResultConverter,
                                                                "Lookup result converter is not initialized.")
                                                        .convert(
                                                                result == null
                                                                        ? null
                                                                        : result.getRowList(),
                                                                remainingFilter));
                                    }
                                } catch (Throwable t) {
                                    future.completeExceptionally(t);
                                }
                            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    @Override
    public void close() throws Exception {
        LOG.info("start close ...");
        flussLookupRuntime.close();
        LOG.info("end close.");
    }
}
