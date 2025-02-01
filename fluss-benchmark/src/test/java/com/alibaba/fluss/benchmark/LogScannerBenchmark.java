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

package com.alibaba.fluss.benchmark;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

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

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

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
        admin.createDatabase("benchmark_db", false).get();
        admin.createTable(TablePath.of("benchmark_db", "benchmark_table"), descriptor, false).get();

        // produce logs
        RowType rowType = descriptor.getSchema().toRowType();
        this.table = conn.getTable(TablePath.of("benchmark_db", "benchmark_table"));
        AppendWriter appendWriter = table.newAppend().createWriter();
        for (long i = 0; i < RECORDS_SIZE; i++) {
            Object[] columns = new Object[] {randomAlphanumeric(10), i, randomAlphanumeric(1000)};
            appendWriter.append(row(rowType, columns));
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
