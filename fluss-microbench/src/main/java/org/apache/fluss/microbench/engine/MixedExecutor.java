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
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.DataConfig;
import org.apache.fluss.microbench.config.PhaseType;
import org.apache.fluss.microbench.config.TableConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.datagen.FieldGenerator;
import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;
import org.apache.fluss.row.GenericRow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** Runs weighted write and lookup operations against one table. */
public class MixedExecutor implements PhaseExecutor {

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
        List<Map.Entry<String, Integer>> mix = new ArrayList<>(phaseConfig.mix().entrySet());
        mix.sort(Map.Entry.comparingByKey());
        int totalWeight = mix.stream().mapToInt(Map.Entry::getValue).sum();
        boolean writes =
                mix.stream()
                        .anyMatch(entry -> PhaseType.fromString(entry.getKey()) == PhaseType.WRITE);
        boolean lookups =
                mix.stream()
                        .anyMatch(
                                entry -> PhaseType.fromString(entry.getKey()) == PhaseType.LOOKUP);
        if (lookups && !tableConfig.hasPrimaryKey()) {
            throw new IllegalArgumentException("Lookup in mix requires a primary key table");
        }

        List<ColumnConfig> columns = tableConfig.columns();
        FieldGenerator[] generators = ExecutorUtils.buildGenerators(columns, dataConfig);
        Random random = ExecutorUtils.createSeededRandom(dataConfig, startIndex);
        long keyMin = ExecutorUtils.resolveKeyMin(phaseConfig.keyRange(), startIndex);
        long keyMax = ExecutorUtils.resolveKeyMax(phaseConfig.keyRange(), endIndex);
        long warmupOps =
                ExecutorUtils.parseWarmupOps(
                        phaseConfig.warmup(),
                        endIndex - startIndex,
                        phaseConfig.threads() == null ? 1 : phaseConfig.threads());
        Duration duration = ExecutorUtils.parseDuration(phaseConfig.duration());
        AtomicReference<Throwable> asyncError = new AtomicReference<>();
        Semaphore inFlight = new Semaphore(256);
        int[] keyIndexes =
                lookups ? ExecutorUtils.resolvePkIndexes(tableConfig.primaryKey(), columns) : null;
        int keyBytes = lookups ? ExecutorUtils.estimateKeyBytes(columns, keyIndexes) : 0;

        try (Table table =
                conn.getTable(TablePath.of(ExecutorUtils.DEFAULT_DATABASE, tableConfig.name()))) {
            TableWriter writer = null;
            Function<GenericRow, CompletableFuture<?>> write = null;
            if (writes) {
                if (tableConfig.hasPrimaryKey()) {
                    UpsertWriter upsert = table.newUpsert().createWriter();
                    writer = upsert;
                    write = row -> upsert.upsert(row);
                } else {
                    AppendWriter append = table.newAppend().createWriter();
                    writer = append;
                    write = row -> append.append(row);
                }
            }
            Lookuper lookuper = lookups ? table.newLookup().createLookuper() : null;
            long started = System.nanoTime();
            long operations = 0;
            for (long index = startIndex; index < endIndex; index++) {
                if (duration != null && ExecutorUtils.isExpired(started, duration)) {
                    break;
                }
                rethrow(asyncError.get());
                int roll = random.nextInt(totalWeight);
                PhaseType selected = null;
                for (Map.Entry<String, Integer> entry : mix) {
                    roll -= entry.getValue();
                    if (roll < 0) {
                        selected = PhaseType.fromString(entry.getKey());
                        break;
                    }
                }
                boolean measured = index - startIndex + 1 > warmupOps;
                inFlight.acquire();
                try {
                    GenericRow row;
                    CompletableFuture<?> future;
                    int bytes;
                    long submitted = System.nanoTime();
                    if (selected == PhaseType.WRITE) {
                        row = ExecutorUtils.buildRow(generators, columns, index);
                        bytes = ExecutorUtils.estimateRowBytes(row);
                        future = write.apply(row);
                    } else if (selected == PhaseType.LOOKUP) {
                        long key = keyMin + (long) (random.nextDouble() * (keyMax - keyMin));
                        row = ExecutorUtils.buildKeyRow(generators, columns, keyIndexes, key);
                        bytes = keyBytes;
                        future = lookuper.lookup(row);
                    } else {
                        throw new IllegalArgumentException(
                                "Unsupported operation in mix: " + selected);
                    }
                    future.whenComplete(
                            (result, error) -> {
                                inFlight.release();
                                if (error != null) {
                                    asyncError.compareAndSet(null, error);
                                } else if (measured) {
                                    latencyRecorder.record(System.nanoTime() - submitted);
                                    throughputCounter.record(bytes);
                                }
                            });
                } catch (RuntimeException e) {
                    inFlight.release();
                    throw e;
                }
                ExecutorUtils.throttleIfNeeded(phaseConfig.rateLimit(), ++operations, started);
            }
            if (writer != null) {
                writer.flush();
            }
            inFlight.acquire(256);
            rethrow(asyncError.get());
        }
    }

    private static void rethrow(Throwable error) throws Exception {
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        if (error != null) {
            throw new RuntimeException(error);
        }
    }
}
