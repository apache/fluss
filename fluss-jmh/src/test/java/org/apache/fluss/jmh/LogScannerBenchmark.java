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

package org.apache.fluss.jmh;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.fluss.testutils.DataTestUtils.row;

/**
 * Benchmark for log fetching via Netty.
 *
 * <p>Note: when profiling the benchmark process, the frame graph should show little percentage of
 * {@code RequestProcessor#sendResponse()} which shouldn't involve large bytes serialization and
 * copy (eliminated by send-file zero-copy).
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 3)
@Fork(value = 0)
public class LogScannerBenchmark {

    // 1 MB
    private static final long RECORDS_SIZE = 1_000;

    private final FlussClusterExtension flussCluster =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();
    private Connection conn;
    private Table table;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        flussCluster.start();

        Configuration clientConf = flussCluster.getClientConfig();
        this.conn = ConnectionFactory.createConnection(clientConf);
        Admin admin = conn.getAdmin();

        // create table
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("small_str", DataTypes.STRING())
                                        .column("bi", DataTypes.BIGINT())
                                        .column("long_str", DataTypes.STRING())
                                        .build())
                        .distributedBy(1) // 1 bucket for benchmark
                        .build();
        admin.createDatabase("benchmark_db", DatabaseDescriptor.EMPTY, false).get();
        admin.createTable(TablePath.of("benchmark_db", "benchmark_table"), descriptor, false).get();

        // produce logs
        this.table = conn.getTable(TablePath.of("benchmark_db", "benchmark_table"));
        AppendWriter appendWriter = table.newAppend().createWriter();
        for (long i = 0; i < RECORDS_SIZE; i++) {
            GenericRow row = row(randomAlphanumeric(10), i, randomAlphanumeric(1000));
            appendWriter.append(row);
        }
        appendWriter.flush();
    }

    @TearDown
    public void teardown() throws Exception {
        table.close();
        conn.close();
        flussCluster.close();
    }

    @Benchmark
    public void scanLog() throws Exception {
        LogScanner logScanner = table.newScan().createLogScanner();
        logScanner.subscribeFromBeginning(0);
        long scanned = 0;
        while (scanned < RECORDS_SIZE) {
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            scanned += scanRecords.count();
        }
        logScanner.close();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + LogScannerBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }
}
