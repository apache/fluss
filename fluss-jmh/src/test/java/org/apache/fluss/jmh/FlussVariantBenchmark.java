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
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.variant.Variant;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.DataTestUtils.row;

/**
 * End-to-end benchmark for Variant shredding column pruning via a real Fluss cluster.
 *
 * <p>Starts a Fluss cluster with 1 TabletServer, writes random 50-column JSON data as Variant with
 * shredding enabled, then benchmarks LogScanner read performance with and without variant sub-field
 * projection ({@code variantFieldProjection}).
 *
 * <ul>
 *   <li>readWithoutProjection: server sends all shredded typed_value children (no pruning)
 *   <li>readWithProjection: server sends only top-N typed_value children (column pruning)
 * </ul>
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 5)
@Fork(value = 0)
@BenchmarkMode(Mode.AverageTime)
public class FlussVariantBenchmark {

    private static final int ROW_COUNT = 10000000;
    private static final int TOTAL_FIELDS = 50;
    private static final String DB_NAME = "bench_db";
    private static final String TABLE_NAME = "variant_bench";
    private static final TablePath TABLE_PATH = TablePath.of(DB_NAME, TABLE_NAME);

    // Static state - initialized once, shared across all @Fork(0) benchmark invocations
    private static FlussClusterExtension flussCluster;
    private static Connection conn;
    private static Table table;
    private static List<String> allFieldNames;

    // Accumulated data sizes: benchmarkName -> (readColumnCount -> totalBytes)
    private static final Map<String, Map<Integer, Long>> dataSizes = new ConcurrentHashMap<>();

    @Param({"1", "5", "10", "20"})
    private int readColumnCount;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        if (flussCluster != null) {
            return; // Already initialized (shared static state with @Fork(0))
        }

        // Pre-compute field names (field_0, field_1, ..., field_49)
        allFieldNames = new ArrayList<>(TOTAL_FIELDS);
        for (int i = 0; i < TOTAL_FIELDS; i++) {
            allFieldNames.add("field_" + i);
        }

        // Start Fluss cluster
        System.out.println("  Starting Fluss cluster...");
        flussCluster = FlussClusterExtension.builder().setNumOfTabletServers(1).build();
        flussCluster.start();
        System.out.println("  Fluss cluster started.");

        // Create connection
        Configuration clientConf = flussCluster.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);

        // Create database and table with shredding enabled
        Admin admin = conn.getAdmin();
        admin.createDatabase(DB_NAME, DatabaseDescriptor.EMPTY, true).get();
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("data", DataTypes.VARIANT())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property("table.variant.shredding.enabled", "true")
                        .property("table.variant.shredding.min-sample-size", "1")
                        .build();
        admin.createTable(TABLE_PATH, tableDescriptor, true).get();
        admin.close();

        // Open table for benchmark use
        table = conn.getTable(TABLE_PATH);

        // Write data to cluster
        writeData();

        // Verify data correctness: projected reads must match non-projected reads
        verifyCorrectness();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        // Resources are shared static state with @Fork(0), cleaned up in main()
    }

    // -------------------------------------------------------------------------
    // Benchmarks
    // -------------------------------------------------------------------------

    @Benchmark
    public long readWithoutProjection() throws Exception {
        // Read all data via LogScanner with ALL variant fields projected (= no actual pruning).
        // This is semantically equivalent to "no projection" since the server sends all fields.
        List<String> fieldsToRead =
                allFieldNames.subList(0, Math.min(readColumnCount, allFieldNames.size()));

        Map<Integer, List<String>> variantHints = new HashMap<>();
        variantHints.put(1, allFieldNames); // all fields = no pruning

        LogScanner logScanner =
                table.newScan()
                        .project(new int[] {1})
                        .variantFieldProjection(variantHints)
                        .createLogScanner();
        logScanner.subscribeFromBeginning(0);

        long sum = 0;
        long totalBytes = 0;
        int count = 0;
        while (count < ROW_COUNT) {
            ScanRecords records = logScanner.poll(Duration.ofSeconds(5));
            for (ScanRecord record : records) {
                int size = record.getSizeInBytes();
                if (size > 0) {
                    totalBytes += size;
                }
                Variant variant = record.getRow().getVariant(0); // projected index 0 = "data"
                for (String fieldName : fieldsToRead) {
                    Variant fieldValue = variant.getFieldByName(fieldName);
                    if (fieldValue != null && !fieldValue.isNull()) {
                        sum++;
                    }
                }
                count++;
            }
        }
        logScanner.close();
        dataSizes
                .computeIfAbsent("readWithoutProjection", k -> new ConcurrentHashMap<>())
                .put(readColumnCount, totalBytes);
        return sum;
    }

    @Benchmark
    public long readWithProjection() throws Exception {
        // Read all data via LogScanner WITH variant sub-field projection (column pruning)
        List<String> fieldsToRead =
                allFieldNames.subList(0, Math.min(readColumnCount, allFieldNames.size()));

        Map<Integer, List<String>> variantHints = new HashMap<>();
        variantHints.put(1, fieldsToRead);

        LogScanner logScanner =
                table.newScan()
                        .project(new int[] {1})
                        .variantFieldProjection(variantHints)
                        .createLogScanner();
        logScanner.subscribeFromBeginning(0);

        long sum = 0;
        long totalBytes = 0;
        int count = 0;
        while (count < ROW_COUNT) {
            ScanRecords records = logScanner.poll(Duration.ofSeconds(5));
            for (ScanRecord record : records) {
                int size = record.getSizeInBytes();
                if (size > 0) {
                    totalBytes += size;
                }
                Variant variant = record.getRow().getVariant(0); // projected index 0 = "data"
                for (String fieldName : fieldsToRead) {
                    Variant fieldValue = variant.getFieldByName(fieldName);
                    if (fieldValue != null && !fieldValue.isNull()) {
                        sum++;
                    }
                }
                count++;
            }
        }
        logScanner.close();
        dataSizes
                .computeIfAbsent("readWithProjection", k -> new ConcurrentHashMap<>())
                .put(readColumnCount, totalBytes);
        return sum;
    }

    // -------------------------------------------------------------------------
    // Data write
    // -------------------------------------------------------------------------

    private static void writeData() throws Exception {
        System.out.printf("  Writing %,d rows to Fluss cluster...%n", ROW_COUNT);
        long start = System.currentTimeMillis();

        AppendWriter writer = table.newAppend().createWriter();
        Random random = new Random(42);
        for (int i = 0; i < ROW_COUNT; i++) {
            String json = generateRandomJson(random);
            writer.append(row((long) i, Variant.fromJson(json)));
        }
        writer.flush();

        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("  Write complete in %,d ms.%n", elapsed);

        // Verify all data is readable by doing a test scan
        System.out.println("  Verifying data readability...");
        LogScanner scanner = table.newScan().createLogScanner();
        scanner.subscribeFromBeginning(0);
        int count = 0;
        while (count < ROW_COUNT) {
            ScanRecords records = scanner.poll(Duration.ofSeconds(5));
            for (ScanRecord ignored : records) {
                count++;
            }
        }
        scanner.close();
        System.out.printf("  All %,d rows readable.%n", count);
    }

    // -------------------------------------------------------------------------
    // Data generation
    // -------------------------------------------------------------------------

    /**
     * Generates a JSON string with up to 50 fields. Field presence probability:
     *
     * <ul>
     *   <li>field_0..field_19: 100% (hot)
     *   <li>field_20..field_34: 80% (warm)
     *   <li>field_35..field_49: 40% (cold)
     * </ul>
     */
    private static String generateRandomJson(Random random) {
        StringBuilder json = new StringBuilder(1024);
        json.append('{');
        boolean first = true;

        for (int f = 0; f < TOTAL_FIELDS; f++) {
            double presenceThreshold;
            if (f < 20) {
                presenceThreshold = 1.0;
            } else if (f < 35) {
                presenceThreshold = 0.8;
            } else {
                presenceThreshold = 0.4;
            }

            if (random.nextDouble() < presenceThreshold) {
                if (!first) {
                    json.append(',');
                }
                first = false;
                json.append("\"field_").append(f).append("\":");

                // Assign type based on field index modulo
                switch (f % 5) {
                    case 0: // INT
                        json.append(random.nextInt(100000));
                        break;
                    case 1: // LONG
                        json.append(random.nextInt(1000000000));
                        break;
                    case 2: // DOUBLE (use exponent notation for double encoding)
                        json.append(String.format("%.4e", random.nextDouble() * 1000));
                        break;
                    case 3: // BOOLEAN
                        json.append(random.nextBoolean());
                        break;
                    case 4: // STRING
                        json.append('"');
                        appendRandomString(json, random, 10 + random.nextInt(50));
                        json.append('"');
                        break;
                }
            }
        }

        json.append('}');
        return json.toString();
    }

    private static void appendRandomString(StringBuilder sb, Random random, int length) {
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
    }

    // -------------------------------------------------------------------------
    // Data correctness verification
    // -------------------------------------------------------------------------

    /**
     * Verifies that reading Variant fields with projection gives the same results as without.
     * Compares top-20 fields for all rows.
     */
    private static void verifyCorrectness() throws Exception {
        System.out.println("  Verifying data correctness...");
        List<String> fieldsToVerify = allFieldNames.subList(0, Math.min(20, allFieldNames.size()));

        // Read all field values with ALL variant fields projected (= no actual pruning)
        String[][] expected = new String[ROW_COUNT][fieldsToVerify.size()];
        Map<Integer, List<String>> allHints = new HashMap<>();
        allHints.put(1, allFieldNames);
        LogScanner noProjectionScanner =
                table.newScan()
                        .project(new int[] {1})
                        .variantFieldProjection(allHints)
                        .createLogScanner();
        noProjectionScanner.subscribeFromBeginning(0);
        int count = 0;
        while (count < ROW_COUNT) {
            ScanRecords records = noProjectionScanner.poll(Duration.ofSeconds(5));
            for (ScanRecord record : records) {
                Variant variant = record.getRow().getVariant(0);
                for (int f = 0; f < fieldsToVerify.size(); f++) {
                    Variant fv = variant.getFieldByName(fieldsToVerify.get(f));
                    expected[count][f] = (fv != null && !fv.isNull()) ? fv.toJson() : null;
                }
                count++;
            }
        }
        noProjectionScanner.close();

        // Read with variant sub-field projection (top-20 only) and compare
        Map<Integer, List<String>> variantHints = new HashMap<>();
        variantHints.put(1, fieldsToVerify);
        LogScanner projectionScanner =
                table.newScan()
                        .project(new int[] {1})
                        .variantFieldProjection(variantHints)
                        .createLogScanner();
        projectionScanner.subscribeFromBeginning(0);

        int mismatches = 0;
        count = 0;
        while (count < ROW_COUNT) {
            ScanRecords records = projectionScanner.poll(Duration.ofSeconds(5));
            for (ScanRecord record : records) {
                Variant variant = record.getRow().getVariant(0);
                for (int f = 0; f < fieldsToVerify.size(); f++) {
                    Variant fv = variant.getFieldByName(fieldsToVerify.get(f));
                    String actual = (fv != null && !fv.isNull()) ? fv.toJson() : null;
                    if (!Objects.equals(actual, expected[count][f])) {
                        mismatches++;
                        if (mismatches <= 5) {
                            System.out.printf(
                                    "    MISMATCH row=%d field=%s: expected=%s actual=%s%n",
                                    count, fieldsToVerify.get(f), expected[count][f], actual);
                        }
                    }
                }
                count++;
            }
        }
        projectionScanner.close();

        int totalChecked = ROW_COUNT * fieldsToVerify.size();
        if (mismatches > 0) {
            throw new RuntimeException(
                    "Data correctness FAILED: " + mismatches + "/" + totalChecked + " mismatches");
        }
        System.out.printf(
                "    PASSED: %,d values verified (%,d rows x %d fields)%n",
                totalChecked, ROW_COUNT, fieldsToVerify.size());
    }

    // -------------------------------------------------------------------------
    // Final report (printed after all benchmarks complete)
    // -------------------------------------------------------------------------

    private static void printFinalReport(Collection<RunResult> results) {
        // Collect scores: benchmarkName -> (readColumnCount -> score)
        Map<String, Map<Integer, Double>> scores = new LinkedHashMap<>();
        for (RunResult result : results) {
            String benchName =
                    result.getParams()
                            .getBenchmark()
                            .substring(result.getParams().getBenchmark().lastIndexOf('.') + 1);
            int paramN = Integer.parseInt(result.getParams().getParam("readColumnCount"));
            scores.computeIfAbsent(benchName, k -> new LinkedHashMap<>())
                    .put(paramN, result.getPrimaryResult().getScore());
        }

        int[] projections = {1, 5, 10, 20};
        Map<Integer, Double> noProjectionTimes =
                scores.getOrDefault("readWithoutProjection", new LinkedHashMap<>());
        Map<Integer, Double> projectionTimes =
                scores.getOrDefault("readWithProjection", new LinkedHashMap<>());
        Map<Integer, Long> noProjectionSizes =
                dataSizes.getOrDefault("readWithoutProjection", new LinkedHashMap<>());
        Map<Integer, Long> projectionSizes =
                dataSizes.getOrDefault("readWithProjection", new LinkedHashMap<>());

        System.out.println();
        System.out.println(
                "===========================================================================");
        System.out.println("  Fluss Variant Shredding Column Pruning Benchmark Report");
        System.out.printf(
                "  Rows: %,d   Total Fields: %d   Cluster: 1 TabletServer%n",
                ROW_COUNT, TOTAL_FIELDS);
        System.out.println(
                "===========================================================================");

        // Data Size table
        System.out.println();
        System.out.println("  Data Size (bytes read per iteration):");
        System.out.printf(
                "  %-12s | %-18s | %-18s | %-10s%n",
                "Projection", "No Pruning", "With Pruning", "Reduction");
        System.out.println("  -------------|--------------------|--------------------|----------");
        for (int n : projections) {
            Long noPruningBytes = noProjectionSizes.get(n);
            Long pruningBytes = projectionSizes.get(n);
            String reduction = "-";
            if (noPruningBytes != null && pruningBytes != null && noPruningBytes > 0) {
                double ratio = 1.0 - (double) pruningBytes / noPruningBytes;
                reduction = String.format("%.1f%%", ratio * 100);
            }
            System.out.printf(
                    "  top-%-7d | %-18s | %-18s | %-10s%n",
                    n,
                    noPruningBytes != null ? formatBytes(noPruningBytes) : "-",
                    pruningBytes != null ? formatBytes(pruningBytes) : "-",
                    reduction);
        }

        // Read Time table
        System.out.println();
        System.out.println("  Read Time (end-to-end via LogScanner):");
        System.out.printf(
                "  %-12s | %-22s | %-22s | %-8s%n",
                "Projection", "No Pruning(ms/op)", "With Pruning(ms/op)", "Speedup");
        System.out.println(
                "  -------------|------------------------|------------------------|--------");
        for (int n : projections) {
            Double noProjectionMs = noProjectionTimes.get(n);
            Double projectionMs = projectionTimes.get(n);
            String speedup = "-";
            if (noProjectionMs != null && projectionMs != null && projectionMs > 0) {
                speedup = String.format("%.1fx", noProjectionMs / projectionMs);
            }
            System.out.printf(
                    "  top-%-7d | %-22s | %-22s | %-8s%n",
                    n,
                    noProjectionMs != null ? String.format("%.3f", noProjectionMs) : "-",
                    projectionMs != null ? String.format("%.3f", projectionMs) : "-",
                    speedup);
        }

        System.out.println(
                "===========================================================================");
        System.out.println();
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        }
    }

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + FlussVariantBenchmark.class.getCanonicalName() + ".*")
                        .build();

        Collection<RunResult> results = new Runner(opt).run();
        printFinalReport(results);

        // Cleanup cluster resources
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
            if (flussCluster != null) {
                flussCluster.close();
            }
        } catch (Exception e) {
            System.err.println("Warning: error during cleanup: " + e.getMessage());
        }
    }
}
