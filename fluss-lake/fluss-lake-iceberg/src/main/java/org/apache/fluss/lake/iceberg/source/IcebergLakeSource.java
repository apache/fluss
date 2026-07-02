/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.utils.FlussToIcebergPredicateConverter;
import org.apache.fluss.lake.iceberg.utils.IcebergCatalogUtils;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;

/** Iceberg lake source. */
public class IcebergLakeSource implements LakeSource<IcebergSplit> {
    private static final long serialVersionUID = 1L;

    /**
     * Config key for table cache TTL in milliseconds. After this duration, the cached table is
     * reloaded on next use. Set to 0 to disable TTL (cache never expires). Default: 5 minutes.
     */
    public static final String TABLE_CACHE_TTL_MS_KEY = "iceberg.catalog.table-cache-ttl-ms";

    private static final long DEFAULT_TABLE_CACHE_TTL_MS = 5 * 60 * 1000L;

    private final Configuration icebergConfig;
    private final TablePath tablePath;
    private @Nullable int[][] project;
    private @Nullable Expression filter;

    /** Cached catalog and table; not serialized, lazily initialized per task. */
    private transient volatile Catalog catalog;

    private transient volatile Table table;

    /** When the table was loaded (ms); used for TTL. */
    private transient volatile long tableLoadedAtMs;

    public IcebergLakeSource(Configuration icebergConfig, TablePath tablePath) {
        this.icebergConfig = icebergConfig;
        this.tablePath = tablePath;
    }

    @Override
    public void withProject(int[][] project) {
        this.project = project;
    }

    @Override
    public void withLimit(int limit) {
        throw new UnsupportedOperationException("Not impl.");
    }

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        List<Predicate> unConsumedPredicates = new ArrayList<>();
        List<Predicate> consumedPredicates = new ArrayList<>();
        List<Expression> converted = new ArrayList<>();
        Schema schema = getSchema(tablePath);
        for (Predicate predicate : predicates) {
            Optional<Expression> optPredicate =
                    FlussToIcebergPredicateConverter.convert(schema, predicate);
            if (optPredicate.isPresent()) {
                consumedPredicates.add(predicate);
                converted.add(optPredicate.get());
            } else {
                unConsumedPredicates.add(predicate);
            }
        }
        if (!converted.isEmpty()) {
            filter = converted.stream().reduce(Expressions::and).orElse(null);
        }
        return FilterPushDownResult.of(consumedPredicates, unConsumedPredicates);
    }

    @Override
    public Planner<IcebergSplit> createPlanner(PlannerContext context) throws IOException {
        return new IcebergSplitPlanner(icebergConfig, tablePath, context.snapshotId(), filter);
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<IcebergSplit> context) throws IOException {
        Table table = getOrLoadTable();
        return new IcebergRecordReader(context.lakeSplit().fileScanTask(), table, project);
    }

    @Override
    public SimpleVersionedSerializer<IcebergSplit> getSplitSerializer() {
        return new IcebergSplitSerializer();
    }

    private Schema getSchema(TablePath tablePath) {
        Table t = table;
        if (t != null) {
            return t.schema();
        }
        Catalog c = IcebergCatalogUtils.createIcebergCatalog(icebergConfig);
        return c.loadTable(toIceberg(tablePath)).schema();
    }

    /**
     * Returns the cached table or loads it (and caches). Respects TTL: if cache is stale, reloads.
     */
    private Table getOrLoadTable() throws IOException {
        long ttlMs = getTableCacheTtlMs();
        Table t = table;
        if (t != null && (ttlMs <= 0 || System.currentTimeMillis() - tableLoadedAtMs <= ttlMs)) {
            return t;
        }
        synchronized (this) {
            t = table;
            if (t != null
                    && (ttlMs <= 0 || System.currentTimeMillis() - tableLoadedAtMs <= ttlMs)) {
                return t;
            }
            if (ttlMs > 0 && t != null && System.currentTimeMillis() - tableLoadedAtMs > ttlMs) {
                catalog = null;
                table = null;
            }
            catalog = IcebergCatalogUtils.createIcebergCatalog(icebergConfig);
            table = catalog.loadTable(toIceberg(tablePath));
            tableLoadedAtMs = System.currentTimeMillis();
            return table;
        }
    }

    private long getTableCacheTtlMs() {
        Optional<Object> raw = icebergConfig.getRawValue(TABLE_CACHE_TTL_MS_KEY);
        if (!raw.isPresent()) {
            return DEFAULT_TABLE_CACHE_TTL_MS;
        }
        Object v = raw.get();
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        try {
            return Long.parseLong(v.toString());
        } catch (NumberFormatException e) {
            return DEFAULT_TABLE_CACHE_TTL_MS;
        }
    }
}
