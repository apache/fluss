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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.DataConfig;
import org.apache.fluss.microbench.config.TableConfig;
import org.apache.fluss.microbench.config.WorkloadPhaseConfig;
import org.apache.fluss.microbench.datagen.FieldGenerator;
import org.apache.fluss.microbench.stats.LatencyRecorder;
import org.apache.fluss.microbench.stats.ThroughputCounter;
import org.apache.fluss.row.GenericRow;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/** Runs bounded asynchronous point lookups. */
public class LookupExecutor implements PhaseExecutor {

    private static final int MAX_IN_FLIGHT = 256;

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
        if (!tableConfig.hasPrimaryKey()) {
            throw new IllegalArgumentException("Lookup requires a primary key table");
        }
        List<ColumnConfig> columns = tableConfig.columns();
        int[] keyIndexes = ExecutorUtils.resolvePkIndexes(tableConfig.primaryKey(), columns);
        FieldGenerator[] generators = ExecutorUtils.buildGenerators(columns, dataConfig);
        Random random = ExecutorUtils.createSeededRandom(dataConfig, startIndex);
        long keyMin = ExecutorUtils.resolveKeyMin(phaseConfig.keyRange(), startIndex);
        long keyMax = ExecutorUtils.resolveKeyMax(phaseConfig.keyRange(), endIndex);
        long warmupOps =
                ExecutorUtils.parseWarmupOps(
                        phaseConfig.warmup(),
                        endIndex - startIndex,
                        phaseConfig.threads() == null ? 1 : phaseConfig.threads());
        int keyBytes = ExecutorUtils.estimateKeyBytes(columns, keyIndexes);
        Duration duration = ExecutorUtils.parseDuration(phaseConfig.duration());
        Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);
        AtomicReference<Throwable> asyncError = new AtomicReference<>();

        try (Table table =
                conn.getTable(TablePath.of(ExecutorUtils.DEFAULT_DATABASE, tableConfig.name()))) {
            Lookuper lookuper = table.newLookup().createLookuper();
            long started = System.nanoTime();
            long operations = 0;
            for (long index = startIndex; index < endIndex; index++) {
                if (duration != null && ExecutorUtils.isExpired(started, duration)) {
                    break;
                }
                rethrow(asyncError.get());
                long key = keyMin + (long) (random.nextDouble() * (keyMax - keyMin));
                GenericRow row = ExecutorUtils.buildKeyRow(generators, columns, keyIndexes, key);
                boolean measured = ++operations > warmupOps;
                inFlight.acquire();
                long submitted = System.nanoTime();
                try {
                    lookuper.lookup(row)
                            .whenComplete(
                                    (result, error) -> {
                                        inFlight.release();
                                        if (error != null) {
                                            asyncError.compareAndSet(null, error);
                                        } else if (measured) {
                                            latencyRecorder.record(System.nanoTime() - submitted);
                                            throughputCounter.record(keyBytes);
                                        }
                                    });
                } catch (Exception e) {
                    inFlight.release();
                    throw e;
                }
                ExecutorUtils.throttleIfNeeded(phaseConfig.rateLimit(), operations, started);
            }
            inFlight.acquire(MAX_IN_FLIGHT);
            inFlight.release(MAX_IN_FLIGHT);
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
