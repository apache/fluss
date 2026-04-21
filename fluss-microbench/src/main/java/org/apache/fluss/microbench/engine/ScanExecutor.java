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

package org.apache.fluss.microbench.engine;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.DataConfig;
import org.apache.fluss.microbench.config.DataTypeParser;
import org.apache.fluss.microbench.config.TableConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Executor for scan workload phases. Creates a LogScanner, subscribes to all buckets from the
 * configured offset, and polls in a loop. Supports optional filter pushdown via the {@code filter}
 * config in the workload phase.
 */
public class ScanExecutor implements PhaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ScanExecutor.class);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);

    private final int threadIndex;

    /** Creates a ScanExecutor for single-threaded use (thread index 0). */
    ScanExecutor() {
        this(0);
    }

    /**
     * Creates a ScanExecutor with an explicit thread index for bucket partitioning.
     *
     * @param threadIndex the zero-based index of this thread among all scan threads
     */
    ScanExecutor(int threadIndex) {
        this.threadIndex = threadIndex;
    }

    @Override
    public void execute(
            Connection conn,
            TableConfig tableConfig,
            DataConfig dataConfig,
            WorkloadPhaseConfig phaseConfig,
            LatencyRecorder latencyRecorder,
            ThroughputCounter throughputCounter,
            long startIndex,
            long endIndex)
            throws Exception {

        long totalRecords = endIndex - startIndex;
        long warmupOps =
                ExecutorUtils.parseWarmupOps(
                        phaseConfig.warmup(),
                        totalRecords,
                        phaseConfig.threads() != null ? phaseConfig.threads() : 1);
        Duration duration = ExecutorUtils.parseDuration(phaseConfig.duration());

        long fromOffset = parseFromOffset(phaseConfig.fromOffset());

        // Pre-compute estimated row size from column types for throughput calculation
        int estimatedRowBytes = ExecutorUtils.estimateRowBytesFromSchema(tableConfig.columns());

        TablePath tablePath = TablePath.of(ExecutorUtils.DEFAULT_DATABASE, tableConfig.name());

        try (Table table = conn.getTable(tablePath)) {
            TableInfo tableInfo = table.getTableInfo();
            int numBuckets = tableInfo.getNumBuckets();

            // Build filter predicate if configured
            Predicate predicate = buildPredicate(phaseConfig.filter(), tableConfig);

            Scan scan = table.newScan();
            if (predicate != null) {
                scan = scan.filter(predicate);
                LOG.info(
                        "Scan thread {} using filter pushdown: {}",
                        threadIndex,
                        phaseConfig.filter());
            }

            try (LogScanner scanner = scan.createLogScanner()) {
                // Partition buckets across threads using the configured thread count and
                // the explicit threadIndex assigned at construction time.
                int totalThreads = phaseConfig.threads() != null ? phaseConfig.threads() : 1;

                int bucketsAssigned = 0;
                for (int bucket = 0; bucket < numBuckets; bucket++) {
                    if (bucket % totalThreads == threadIndex) {
                        scanner.subscribe(bucket, fromOffset);
                        bucketsAssigned++;
                    }
                }
                LOG.info(
                        "Scan thread {} subscribed to {}/{} buckets",
                        threadIndex,
                        bucketsAssigned,
                        numBuckets);

                if (bucketsAssigned == 0) {
                    // More threads than buckets — this thread has nothing to scan.
                    return;
                }

                long startTime = System.nanoTime();
                long opsCount = 0;
                int consecutiveEmptyPolls = 0;
                // When duration is set, don't exit early on empty polls — keep polling
                // until the duration expires. Only use empty-poll exit for record-count mode.
                final int maxEmptyPolls = duration != null ? Integer.MAX_VALUE : 60;

                while (opsCount < totalRecords) {
                    if (duration != null && ExecutorUtils.isExpired(startTime, duration)) {
                        break;
                    }

                    long pollStart = System.nanoTime();
                    ScanRecords records = scanner.poll(POLL_TIMEOUT);
                    long pollLatency = System.nanoTime() - pollStart;

                    if (records.isEmpty()) {
                        consecutiveEmptyPolls++;
                        if (consecutiveEmptyPolls >= maxEmptyPolls) {
                            LOG.warn(
                                    "No data after {} consecutive polls ({}ms), stopping early",
                                    consecutiveEmptyPolls,
                                    consecutiveEmptyPolls * POLL_TIMEOUT.toMillis());
                            break;
                        }
                        continue;
                    }

                    // Reset counter on successful poll
                    consecutiveEmptyPolls = 0;

                    // Count records in this batch and estimate bytes
                    int batchSize = 0;
                    for (ScanRecord ignored : records) {
                        batchSize++;
                        opsCount++;
                        if (opsCount >= totalRecords) {
                            break;
                        }
                    }

                    // Record poll latency as batch-level metric (not amortized per record).
                    // This represents the true poll-to-delivery latency for each batch.
                    if (opsCount > warmupOps && batchSize > 0) {
                        latencyRecorder.record(pollLatency);
                        throughputCounter.record((long) batchSize * estimatedRowBytes);
                    }
                }
            }
        }
    }

    /**
     * Builds a {@link Predicate} from the filter config map. Supports ops: eq, neq, gt, gte, lt,
     * lte. The value is coerced to the column's data type.
     */
    @Nullable
    static Predicate buildPredicate(
            @Nullable Map<String, String> filterConfig, TableConfig tableConfig) {
        if (filterConfig == null || filterConfig.isEmpty()) {
            return null;
        }
        String column = filterConfig.get("column");
        String op = filterConfig.get("op");
        String value = filterConfig.get("value");
        if (column == null || op == null || value == null) {
            throw new IllegalArgumentException(
                    "Filter config must have 'column', 'op', and 'value' keys, got: "
                            + filterConfig);
        }

        // Find column index and type
        List<ColumnConfig> columns = tableConfig.columns();
        int colIndex = -1;
        DataType colType = null;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(column)) {
                colIndex = i;
                colType = DataTypeParser.parse(columns.get(i).type());
                break;
            }
        }
        if (colIndex < 0) {
            throw new IllegalArgumentException(
                    "Filter column '" + column + "' not found in table schema");
        }

        // Build RowType for PredicateBuilder
        String[] fieldNames = new String[columns.size()];
        DataType[] fieldTypes = new DataType[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            fieldNames[i] = columns.get(i).name();
            fieldTypes[i] = DataTypeParser.parse(columns.get(i).type());
        }
        RowType rowType = RowType.of(fieldTypes, fieldNames);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Object literal = coerceValue(value, colType);

        switch (op.toLowerCase()) {
            case "eq":
                return builder.equal(colIndex, literal);
            case "neq":
                return builder.notEqual(colIndex, literal);
            case "gt":
                return builder.greaterThan(colIndex, literal);
            case "gte":
                return builder.greaterOrEqual(colIndex, literal);
            case "lt":
                return builder.lessThan(colIndex, literal);
            case "lte":
                return builder.lessOrEqual(colIndex, literal);
            default:
                throw new IllegalArgumentException("Unsupported filter op: " + op);
        }
    }

    /** Coerces a string value to the appropriate Java type for the given Fluss DataType. */
    private static Object coerceValue(String value, DataType dataType) {
        String typeName = dataType.toString().toUpperCase();
        if (typeName.startsWith("INT") || typeName.equals("INTEGER")) {
            return Integer.parseInt(value);
        } else if (typeName.startsWith("BIGINT") || typeName.equals("LONG")) {
            return Long.parseLong(value);
        } else if (typeName.startsWith("SMALLINT") || typeName.equals("SHORT")) {
            return Short.parseShort(value);
        } else if (typeName.startsWith("TINYINT") || typeName.equals("BYTE")) {
            return Byte.parseByte(value);
        } else if (typeName.startsWith("FLOAT")) {
            return Float.parseFloat(value);
        } else if (typeName.startsWith("DOUBLE")) {
            return Double.parseDouble(value);
        } else if (typeName.startsWith("BOOLEAN")) {
            return Boolean.parseBoolean(value);
        } else if (typeName.startsWith("STRING")
                || typeName.startsWith("CHAR")
                || typeName.startsWith("VARCHAR")) {
            return org.apache.fluss.row.BinaryString.fromString(value);
        }
        // Default: return as string
        return value;
    }

    static long parseFromOffset(String fromOffset) {
        if (fromOffset == null || fromOffset.isEmpty() || "earliest".equalsIgnoreCase(fromOffset)) {
            return LogScanner.EARLIEST_OFFSET;
        }
        if ("latest".equalsIgnoreCase(fromOffset)) {
            throw new IllegalArgumentException(
                    "from-offset=latest is not supported by LogScanner. "
                            + "Use 'earliest' or an explicit numeric offset.");
        }
        return Long.parseLong(fromOffset);
    }
}
