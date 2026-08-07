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
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A flink lookup function for fluss. */
public class FlinkLookupFunction extends LookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final LookupNormalizer lookupNormalizer;
    private final FlussLookupRuntime flussLookupRuntime;
    private transient LookupResultConverter lookupResultConverter;

    public FlinkLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection,
            boolean insertIfNotExists) {
        this.lookupNormalizer = lookupNormalizer;
        this.flussLookupRuntime =
                new FlussLookupRuntime(
                        flussConfig, tablePath, flinkRowType, lookupNormalizer, insertIfNotExists);

        int[] resolvedProjection =
                projection == null
                        ? IntStream.range(0, flinkRowType.getFieldCount()).toArray()
                        : projection;
        RowType outputRowType = FlinkUtils.projectRowType(flinkRowType, resolvedProjection);
        this.lookupResultConverter =
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
     * @param keyRow - A {@link RowData} that wraps lookup keys. Currently only support single
     *     rowkey.
     */
    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        RowData normalizedKeyRow = lookupNormalizer.normalizeLookupKey(keyRow);
        LookupNormalizer.RemainingFilter remainingFilter =
                lookupNormalizer.createRemainingFilter(keyRow);

        // the retry mechanism will be handled by the underlying LookupClient layer
        try {
            List<InternalRow> rows = flussLookupRuntime.lookup(normalizedKeyRow).get().getRowList();
            return checkNotNull(
                            lookupResultConverter, "Lookup result converter is not initialized.")
                    .convert(rows, remainingFilter);
        } catch (Exception e) {
            LOG.error("Fluss lookup error", e);
            throw new RuntimeException("Execution of Fluss lookup failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("start close ...");
        flussLookupRuntime.close();
        LOG.info("end close.");
    }
}
