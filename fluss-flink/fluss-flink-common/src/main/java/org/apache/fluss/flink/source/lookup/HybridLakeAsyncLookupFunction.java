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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.PartitionUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.apache.fluss.flink.utils.LakeSourceUtils.createLakeSource;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An async lookup function that falls back to lake data when cold Fluss lookup misses. */
public class HybridLakeAsyncLookupFunction extends AsyncLookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(HybridLakeAsyncLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final RowType flinkRowType;
    private final int[] primaryKeyIndexes;
    private final int[] partitionKeyIndexes;
    private final LookupNormalizer lookupNormalizer;
    @Nullable private int[] projection;
    private final Map<String, String> tableOptions;
    private final Duration hotWindow;
    private final ZoneId lookupTimeZone;
    @Nullable private final String autoPartitionKey;
    private final AutoPartitionTimeUnit autoPartitionTimeUnit;
    private final Duration lakeFallbackTimeout;
    private final int lakeFallbackExecutorThreads;
    private final int lakeFallbackMaxConcurrency;

    private transient FlussRowToFlinkRowConverter flussRowToFlinkRowConverter;
    private transient Connection connection;
    private transient Admin admin;
    private transient Table table;
    private transient Lookuper lookuper;
    private transient FlinkAsFlussRow lookupRow;
    private transient InternalRow.FieldGetter[] primaryKeyFieldGetters;
    private transient int autoPartitionKeyPositionInPrimaryKey;
    private transient org.apache.fluss.types.RowType flussFullRowType;
    private transient ThreadPoolExecutor lakeLookupExecutor;
    private transient ScheduledExecutorService timeoutExecutor;
    private transient AtomicInteger lakeFallbackPendingCount;
    private transient Counter lookupHotFlussHitsTotal;
    private transient Counter lookupHotFlussMissesTotal;
    private transient Counter lookupColdFlussHitsTotal;
    private transient Counter lookupColdFlussMissesTotal;
    private transient Counter lakeFallbackRequestsTotal;
    private transient Counter lakeFallbackHitsTotal;
    private transient Counter lakeFallbackMissesTotal;
    private transient Counter lakeFallbackFailuresTotal;
    private transient Counter lakeFallbackTimeoutsTotal;
    private transient Counter lakeFallbackRejectedTotal;
    private transient Histogram lakeFallbackLatencyMs;

    public HybridLakeAsyncLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            int[] primaryKeyIndexes,
            int[] partitionKeyIndexes,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection,
            Map<String, String> tableOptions,
            Duration hotWindow,
            ZoneId lookupTimeZone,
            Duration lakeFallbackTimeout,
            int lakeFallbackExecutorThreads,
            int lakeFallbackMaxConcurrency) {
        this.flussConfig = flussConfig;
        this.tablePath = tablePath;
        this.flinkRowType = flinkRowType;
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.partitionKeyIndexes = partitionKeyIndexes;
        this.lookupNormalizer = lookupNormalizer;
        this.projection = projection;
        this.tableOptions = tableOptions;
        this.hotWindow = hotWindow;
        this.lookupTimeZone = lookupTimeZone;
        Configuration tableConfig = Configuration.fromMap(tableOptions);
        this.autoPartitionKey = tableConfig.getString(ConfigOptions.TABLE_AUTO_PARTITION_KEY);
        this.autoPartitionTimeUnit = tableConfig.get(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT);
        this.lakeFallbackTimeout = lakeFallbackTimeout;
        this.lakeFallbackExecutorThreads = lakeFallbackExecutorThreads;
        this.lakeFallbackMaxConcurrency = lakeFallbackMaxConcurrency;
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("Start opening hybrid lake async lookup function for table {}.", tablePath);
        flussFullRowType = FlinkConversions.toFlussRowType(flinkRowType);
        validateLookupShape();
        connection = ConnectionFactory.createConnection(flussConfig);
        admin = connection.getAdmin();
        table = connection.getTable(tablePath);
        lookupRow = new FlinkAsFlussRow();

        final RowType outputRowType;
        if (projection == null) {
            outputRowType = flinkRowType;
            projection = IntStream.range(0, flinkRowType.getFieldCount()).toArray();
        } else {
            outputRowType = FlinkUtils.projectRowType(flinkRowType, projection);
        }
        flussRowToFlinkRowConverter =
                new FlussRowToFlinkRowConverter(FlinkConversions.toFlussRowType(outputRowType));

        Lookup lookup = table.newLookup();
        lookuper = lookup.createLookuper();

        org.apache.fluss.types.RowType primaryKeyRowType =
                flussFullRowType.project(primaryKeyIndexes);
        primaryKeyFieldGetters = new InternalRow.FieldGetter[primaryKeyIndexes.length];
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            primaryKeyFieldGetters[i] =
                    InternalRow.createFieldGetter(primaryKeyRowType.getTypeAt(i), i);
        }
        autoPartitionKeyPositionInPrimaryKey = findAutoPartitionKeyPositionInPrimaryKey();

        lakeLookupExecutor =
                new ThreadPoolExecutor(
                        lakeFallbackExecutorThreads,
                        lakeFallbackExecutorThreads,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(lakeFallbackMaxConcurrency),
                        new ExecutorThreadFactory("fluss-lake-fallback-lookup"),
                        new ThreadPoolExecutor.AbortPolicy());
        timeoutExecutor =
                new ScheduledThreadPoolExecutor(
                        1, new ExecutorThreadFactory("fluss-lake-fallback-timeout"));
        lakeFallbackPendingCount = new AtomicInteger();
        registerMetrics(context);
        LOG.info("Finished opening hybrid lake async lookup function for table {}.", tablePath);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        RowData normalizedKeyRow = lookupNormalizer.normalizeLookupKey(keyRow);
        LookupNormalizer.RemainingFilter remainingFilter =
                lookupNormalizer.createRemainingFilter(keyRow);
        FlussLookupKey lookupKey = createLookupKey(normalizedKeyRow);
        InternalRow flussKeyRow = lookupRow.replace(normalizedKeyRow);

        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        lookuper.lookup(flussKeyRow)
                .whenComplete(
                        (result, throwable) -> {
                            if (throwable != null) {
                                LOG.error(
                                        "Fluss async lookup failed for table {}.",
                                        tablePath,
                                        throwable);
                                future.completeExceptionally(
                                        new RuntimeException(
                                                "Execution of Fluss async lookup failed: "
                                                        + throwable.getMessage(),
                                                throwable));
                            } else if (!isColdPartition(lookupKey.partitionValue)) {
                                if (result.getRowList().isEmpty()) {
                                    lookupHotFlussMissesTotal.inc();
                                } else {
                                    lookupHotFlussHitsTotal.inc();
                                }
                                handleLookupSuccess(future, result, remainingFilter);
                            } else if (!result.getRowList().isEmpty()) {
                                lookupColdFlussHitsTotal.inc();
                                handleLookupSuccess(future, result, remainingFilter);
                            } else {
                                lookupColdFlussMissesTotal.inc();
                                lookupLakeAsync(lookupKey, remainingFilter, future);
                            }
                        });
        return future;
    }

    private void lookupLakeAsync(
            FlussLookupKey lookupKey,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter,
            CompletableFuture<Collection<RowData>> future) {
        lakeFallbackRequestsTotal.inc();
        lakeFallbackPendingCount.incrementAndGet();
        long startMs = System.currentTimeMillis();
        scheduleTimeout(future, startMs);
        try {
            lakeLookupExecutor.execute(
                    () -> {
                        try {
                            Collection<RowData> rows = lookupLake(lookupKey, remainingFilter);
                            completeLakeFallbackSuccessfully(future, rows, startMs);
                        } catch (Throwable t) {
                            completeLakeFallbackExceptionally(
                                    future,
                                    new RuntimeException(
                                            "Execution of lake fallback lookup failed: "
                                                    + t.getMessage(),
                                            t),
                                    lakeFallbackFailuresTotal,
                                    startMs);
                        }
                    });
        } catch (RuntimeException e) {
            completeLakeFallbackExceptionally(
                    future,
                    new RuntimeException("Lake fallback lookup executor is overloaded.", e),
                    lakeFallbackRejectedTotal,
                    startMs);
        }
    }

    private void scheduleTimeout(CompletableFuture<Collection<RowData>> future, long startMs) {
        timeoutExecutor.schedule(
                () ->
                        completeLakeFallbackExceptionally(
                                future,
                                new TimeoutException(
                                        "Lake fallback lookup timed out after "
                                                + lakeFallbackTimeout),
                                lakeFallbackTimeoutsTotal,
                                startMs),
                lakeFallbackTimeout.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void completeLakeFallbackSuccessfully(
            CompletableFuture<Collection<RowData>> future, Collection<RowData> rows, long startMs) {
        if (future.complete(rows)) {
            if (rows.isEmpty()) {
                lakeFallbackMissesTotal.inc();
            } else {
                lakeFallbackHitsTotal.inc();
            }
            recordLakeFallbackCompletion(startMs);
        }
    }

    private void completeLakeFallbackExceptionally(
            CompletableFuture<Collection<RowData>> future,
            Throwable throwable,
            Counter failureCounter,
            long startMs) {
        if (future.completeExceptionally(throwable)) {
            failureCounter.inc();
            recordLakeFallbackCompletion(startMs);
        }
    }

    private void recordLakeFallbackCompletion(long startMs) {
        lakeFallbackLatencyMs.update(System.currentTimeMillis() - startMs);
        lakeFallbackPendingCount.decrementAndGet();
    }

    private Collection<RowData> lookupLake(
            FlussLookupKey lookupKey, @Nullable LookupNormalizer.RemainingFilter remainingFilter)
            throws Exception {
        LakeSnapshot lakeSnapshot;
        try {
            lakeSnapshot = admin.getReadableLakeSnapshot(tablePath).get();
        } catch (Exception e) {
            Throwable stripped = ExceptionUtils.stripExecutionException(e);
            if (stripped instanceof LakeTableSnapshotNotExistException) {
                return Collections.emptyList();
            }
            throw e;
        }

        LakeSource<LakeSplit> lakeSource =
                checkNotNull(
                        createLakeSource(tablePath, tableOptions),
                        "Lake source must not be null for lake fallback lookup.");
        if (projection != null) {
            lakeSource.withProject(toNestedProjection(projection));
        }
        Predicate predicate = createPrimaryKeyPredicate(lookupKey.primaryKeyValues);
        LakeSource.FilterPushDownResult pushDownResult =
                lakeSource.withFilters(Collections.singletonList(predicate));
        if (!pushDownResult.remainingPredicates().isEmpty()) {
            throw new TableException(
                    "Lake fallback lookup requires primary-key predicates to be pushed down.");
        }

        Planner<LakeSplit> planner = lakeSource.createPlanner(lakeSnapshot::getSnapshotId);
        List<LakeSplit> splits = planner.plan();
        for (LakeSplit split : splits) {
            RecordReader reader =
                    lakeSource.createRecordReader(
                            (LakeSource.ReaderContext<LakeSplit>) () -> split);
            try (CloseableIterator<LogRecord> iterator = reader.read()) {
                while (iterator.hasNext()) {
                    RowData row =
                            flussRowToFlinkRowConverter.toFlinkRowData(
                                    maybeProject(iterator.next().getRow()));
                    if (remainingFilter == null || remainingFilter.isMatch(row)) {
                        return Collections.singletonList(row);
                    }
                }
            }
        }
        return Collections.emptyList();
    }

    private Predicate createPrimaryKeyPredicate(Object[] primaryKeyValues) {
        PredicateBuilder builder = new PredicateBuilder(flussFullRowType);
        List<Predicate> predicates = new ArrayList<>();
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            predicates.add(builder.equal(primaryKeyIndexes[i], primaryKeyValues[i]));
        }
        return PredicateBuilder.and(predicates);
    }

    private void handleLookupSuccess(
            CompletableFuture<Collection<RowData>> resultFuture,
            LookupResult lookupResult,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter) {
        if (lookupResult.getRowList().isEmpty()) {
            resultFuture.complete(Collections.emptyList());
            return;
        }

        List<RowData> projectedRows = new ArrayList<>();
        for (InternalRow row : lookupResult.getRowList()) {
            if (row != null) {
                RowData flinkRow = flussRowToFlinkRowConverter.toFlinkRowData(maybeProject(row));
                if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                    projectedRows.add(flinkRow);
                }
            }
        }
        resultFuture.complete(projectedRows);
    }

    private InternalRow maybeProject(InternalRow row) {
        if (projection == null) {
            return row;
        }
        return ProjectedRow.from(projection).replaceRow(row);
    }

    private FlussLookupKey createLookupKey(RowData normalizedKeyRow) {
        InternalRow row = new FlinkAsFlussRow(normalizedKeyRow);
        Object[] primaryKeyValues = new Object[primaryKeyIndexes.length];
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            primaryKeyValues[i] = primaryKeyFieldGetters[i].getFieldOrNull(row);
        }
        String partitionValue =
                String.valueOf(primaryKeyValues[autoPartitionKeyPositionInPrimaryKey]);
        if (primaryKeyValues[autoPartitionKeyPositionInPrimaryKey] instanceof BinaryString) {
            partitionValue = primaryKeyValues[autoPartitionKeyPositionInPrimaryKey].toString();
        }
        return new FlussLookupKey(primaryKeyValues, partitionValue);
    }

    private boolean isColdPartition(String partitionValue) {
        validateAutoPartitionTime(partitionValue);
        String hotWindowStartPartition =
                PartitionUtils.generateAutoPartitionTime(
                        ZonedDateTime.now(lookupTimeZone).minus(hotWindow),
                        0,
                        autoPartitionTimeUnit);
        return partitionValue.compareTo(hotWindowStartPartition) < 0;
    }

    private void validateAutoPartitionTime(String partitionValue) {
        try {
            DateTimeFormatter.ofPattern(getPartitionTimeFormat()).parse(partitionValue);
        } catch (DateTimeParseException e) {
            throw new TableException(
                    "Lake fallback lookup requires the partition value to match auto partition time unit '"
                            + autoPartitionTimeUnit
                            + "' with format '"
                            + getPartitionTimeFormat()
                            + "', but was: "
                            + partitionValue,
                    e);
        }
    }

    private String getPartitionTimeFormat() {
        switch (autoPartitionTimeUnit) {
            case YEAR:
                return "yyyy";
            case QUARTER:
                return "yyyyQ";
            case MONTH:
                return "yyyyMM";
            case DAY:
                return "yyyyMMdd";
            case HOUR:
                return "yyyyMMddHH";
            default:
                throw new TableException(
                        "Unsupported auto partition time unit for lake fallback lookup: "
                                + autoPartitionTimeUnit);
        }
    }

    private int findAutoPartitionKeyPositionInPrimaryKey() {
        int partitionKeyIndex = findAutoPartitionKeyIndex();
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            if (primaryKeyIndexes[i] == partitionKeyIndex) {
                return i;
            }
        }
        throw new TableException(
                "Lake fallback lookup requires auto partition key to be part of primary key.");
    }

    private int findAutoPartitionKeyIndex() {
        if (autoPartitionKey == null) {
            return partitionKeyIndexes[0];
        }

        List<String> fieldNames = flinkRowType.getFieldNames();
        for (int partitionKeyIndex : partitionKeyIndexes) {
            if (fieldNames.get(partitionKeyIndex).equals(autoPartitionKey)) {
                return partitionKeyIndex;
            }
        }
        throw new TableException(
                "Lake fallback lookup requires auto partition key '"
                        + autoPartitionKey
                        + "' to be one of the partition keys.");
    }

    private void validateLookupShape() {
        if (partitionKeyIndexes.length == 0) {
            throw new TableException("Lake fallback lookup requires a partitioned table.");
        }
        if (lakeFallbackExecutorThreads <= 0 || lakeFallbackMaxConcurrency <= 0) {
            throw new TableException("Lake fallback lookup executor settings must be positive.");
        }
        if (lakeFallbackExecutorThreads > lakeFallbackMaxConcurrency) {
            throw new TableException(
                    "Option 'lookup.lake-fallback.executor-threads' must not exceed "
                            + "'lookup.lake-fallback.max-concurrency'.");
        }
        org.apache.fluss.types.DataType partitionType =
                flussFullRowType.getTypeAt(partitionKeyIndexes[0]);
        if (partitionType.getTypeRoot() != DataTypeRoot.STRING
                && partitionType.getTypeRoot() != DataTypeRoot.CHAR) {
            throw new TableException(
                    "Lake fallback lookup currently requires the partition key to be STRING/CHAR.");
        }
        if (!flinkRowType
                .getTypeAt(partitionKeyIndexes[0])
                .getTypeRoot()
                .getFamilies()
                .contains(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new TableException(
                    "Lake fallback lookup currently requires the partition key to be a character string.");
        }
    }

    private int[][] toNestedProjection(int[] projectedFields) {
        int[][] nestedProjection = new int[projectedFields.length][1];
        for (int i = 0; i < projectedFields.length; i++) {
            nestedProjection[i][0] = projectedFields[i];
        }
        return nestedProjection;
    }

    private void registerMetrics(@Nullable FunctionContext context) {
        if (context == null) {
            lookupHotFlussHitsTotal = NoOpCounter.INSTANCE;
            lookupHotFlussMissesTotal = NoOpCounter.INSTANCE;
            lookupColdFlussHitsTotal = NoOpCounter.INSTANCE;
            lookupColdFlussMissesTotal = NoOpCounter.INSTANCE;
            lakeFallbackRequestsTotal = NoOpCounter.INSTANCE;
            lakeFallbackHitsTotal = NoOpCounter.INSTANCE;
            lakeFallbackMissesTotal = NoOpCounter.INSTANCE;
            lakeFallbackFailuresTotal = NoOpCounter.INSTANCE;
            lakeFallbackTimeoutsTotal = NoOpCounter.INSTANCE;
            lakeFallbackRejectedTotal = NoOpCounter.INSTANCE;
            lakeFallbackLatencyMs = NoOpHistogram.INSTANCE;
            return;
        }

        MetricGroup metricGroup = context.getMetricGroup();
        lookupHotFlussHitsTotal = metricGroup.counter(MetricNames.LOOKUP_HOT_FLUSS_HITS_TOTAL);
        lookupHotFlussMissesTotal = metricGroup.counter(MetricNames.LOOKUP_HOT_FLUSS_MISSES_TOTAL);
        lookupColdFlussHitsTotal = metricGroup.counter(MetricNames.LOOKUP_COLD_FLUSS_HITS_TOTAL);
        lookupColdFlussMissesTotal =
                metricGroup.counter(MetricNames.LOOKUP_COLD_FLUSS_MISSES_TOTAL);
        lakeFallbackRequestsTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_REQUESTS_TOTAL);
        lakeFallbackHitsTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_HITS_TOTAL);
        lakeFallbackMissesTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_MISSES_TOTAL);
        lakeFallbackFailuresTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_FAILURES_TOTAL);
        lakeFallbackTimeoutsTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_TIMEOUTS_TOTAL);
        lakeFallbackRejectedTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_REJECTED_TOTAL);
        lakeFallbackLatencyMs =
                metricGroup.histogram(
                        MetricNames.LAKE_FALLBACK_LATENCY_MS, new SlidingWindowHistogram(1024));
        metricGroup.gauge(MetricNames.LAKE_FALLBACK_PENDING_COUNT, lakeFallbackPendingCount::get);
        metricGroup.gauge(
                MetricNames.LAKE_FALLBACK_QUEUE_SIZE,
                () -> lakeLookupExecutor == null ? 0 : lakeLookupExecutor.getQueue().size());
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing hybrid lake async lookup function for table {}.", tablePath);
        if (lakeLookupExecutor != null) {
            lakeLookupExecutor.shutdownNow();
        }
        if (timeoutExecutor != null) {
            timeoutExecutor.shutdownNow();
        }
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    private enum NoOpCounter implements Counter {
        INSTANCE;

        @Override
        public void inc() {}

        @Override
        public void inc(long n) {}

        @Override
        public void dec() {}

        @Override
        public void dec(long n) {}

        @Override
        public long getCount() {
            return 0;
        }
    }

    private enum NoOpHistogram implements Histogram {
        INSTANCE;

        @Override
        public void update(long value) {}

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public HistogramStatistics getStatistics() {
            return new SlidingWindowHistogramStatistics(new long[0]);
        }
    }

    private static class SlidingWindowHistogram implements Histogram {
        private final long[] values;
        private int position;
        private long count;

        private SlidingWindowHistogram(int size) {
            this.values = new long[size];
        }

        @Override
        public synchronized void update(long value) {
            values[position] = value;
            position = (position + 1) % values.length;
            count++;
        }

        @Override
        public synchronized long getCount() {
            return count;
        }

        @Override
        public synchronized HistogramStatistics getStatistics() {
            int size = (int) Math.min(count, values.length);
            long[] snapshot = new long[size];
            for (int i = 0; i < size; i++) {
                snapshot[i] = values[i];
            }
            return new SlidingWindowHistogramStatistics(snapshot);
        }
    }

    private static class SlidingWindowHistogramStatistics extends HistogramStatistics {
        private final long[] values;

        private SlidingWindowHistogramStatistics(long[] values) {
            this.values = values;
            Arrays.sort(this.values);
        }

        @Override
        public double getQuantile(double quantile) {
            if (values.length == 0) {
                return 0.0;
            }
            int index = (int) Math.ceil(quantile * values.length) - 1;
            index = Math.max(0, Math.min(index, values.length - 1));
            return values[index];
        }

        @Override
        public long[] getValues() {
            return Arrays.copyOf(values, values.length);
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public double getMean() {
            if (values.length == 0) {
                return 0.0;
            }
            long sum = 0;
            for (long value : values) {
                sum += value;
            }
            return (double) sum / values.length;
        }

        @Override
        public double getStdDev() {
            if (values.length <= 1) {
                return 0.0;
            }
            double mean = getMean();
            double sum = 0.0;
            for (long value : values) {
                double delta = value - mean;
                sum += delta * delta;
            }
            return Math.sqrt(sum / values.length);
        }

        @Override
        public long getMax() {
            return values.length == 0 ? 0 : values[values.length - 1];
        }

        @Override
        public long getMin() {
            return values.length == 0 ? 0 : values[0];
        }
    }

    private static class FlussLookupKey {
        private final Object[] primaryKeyValues;
        private final String partitionValue;

        private FlussLookupKey(Object[] primaryKeyValues, String partitionValue) {
            this.primaryKeyValues = primaryKeyValues;
            this.partitionValue = partitionValue;
        }
    }
}
