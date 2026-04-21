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

package org.apache.fluss.microbench;

import org.apache.fluss.microbench.baseline.BaselineManager;
import org.apache.fluss.microbench.config.ColumnConfig;
import org.apache.fluss.microbench.config.ScenarioConfig;
import org.apache.fluss.microbench.config.ScenarioValidator;
import org.apache.fluss.microbench.config.ThresholdsConfig;
import org.apache.fluss.microbench.datagen.DatasetWriter;
import org.apache.fluss.microbench.datagen.FieldGenerator;
import org.apache.fluss.microbench.engine.ExecutorUtils;
import org.apache.fluss.microbench.engine.YamlDrivenEngine;
import org.apache.fluss.microbench.report.DiffResult;
import org.apache.fluss.microbench.report.PerfReport;
import org.apache.fluss.microbench.report.PhaseResult;
import org.apache.fluss.microbench.report.ReportDiffer;
import org.apache.fluss.microbench.report.ReportWriter;
import org.apache.fluss.microbench.sampling.ResourceSnapshot;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** CLI entry point for the Fluss performance benchmark tool. */
public class PerfRunner {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private static final DateTimeFormatter TIMESTAMP_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(ExitCode.CONFIG_ERROR.code());
            return;
        }

        String command = args[0];
        String[] subArgs = Arrays.copyOfRange(args, 1, args.length);

        try {
            int exitCode;
            if ("run".equals(command)) {
                exitCode = runCommand(subArgs);
            } else if ("generate".equals(command)) {
                exitCode = generateCommand(subArgs);
            } else if ("validate".equals(command)) {
                exitCode = validateCommand(subArgs);
            } else if ("diff".equals(command)) {
                exitCode = diffCommand(subArgs);
            } else if ("baseline".equals(command)) {
                exitCode = baselineCommand(subArgs);
            } else if ("list".equals(command)) {
                exitCode = listCommand(subArgs);
            } else if ("clean".equals(command)) {
                exitCode = cleanCommand(subArgs);
            } else {
                System.err.println("Unknown command: " + command);
                printUsage();
                exitCode = ExitCode.CONFIG_ERROR.code();
            }
            System.exit(exitCode);
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(ExitCode.EXECUTION_ERROR.code());
        }
    }

    // -------------------------------------------------------------------------
    //  run command
    // -------------------------------------------------------------------------

    private static int runCommand(String[] args) throws Exception {
        Options options = buildRunOptions();
        CommandLine cli;
        try {
            cli = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println("Invalid options: " + e.getMessage());
            new HelpFormatter().printHelp("fluss-microbench run", options);
            return ExitCode.CONFIG_ERROR.code();
        }

        if (!cli.hasOption("scenario-file")) {
            System.err.println("Missing required option: --scenario-file");
            return ExitCode.CONFIG_ERROR.code();
        }

        // Load and validate config
        ScenarioConfig config = PerfConfig.load(cli.getOptionValue("scenario-file"), cli);
        List<String> errors = ScenarioValidator.validate(config);
        if (!errors.isEmpty()) {
            System.err.println("Validation errors:");
            for (String err : errors) {
                System.err.println("  - " + err);
            }
            return ExitCode.CONFIG_ERROR.code();
        }
        if (!config.parseWarnings().isEmpty()) {
            System.err.println("Config warnings (unrecognized fields):");
            for (String w : config.parseWarnings()) {
                System.err.println("  - " + w);
            }
        }

        // Timeout watchdog (default 1 hour)
        long timeoutMs =
                cli.hasOption("timeout") ? parseDurationMs(cli.getOptionValue("timeout")) : 3600000;
        startTimeoutWatchdog(timeoutMs);

        // Determine json-lines output
        List<String> formats = PerfConfig.getReportFormats(config);
        PrintStream jsonLinesOut = null;
        if (formats.contains("json-lines")) {
            jsonLinesOut = System.out;
        }

        // Resolve output directory via MicrobenchPaths
        MicrobenchPaths paths = MicrobenchPaths.fromSystemProperty();
        String scenario = resolveScenarioName(config, cli.getOptionValue("scenario-file"));
        String timestamp = TIMESTAMP_FMT.format(LocalDateTime.now());
        Path runDir = paths.runDir(scenario, timestamp);

        // Run engine (single iteration)
        YamlDrivenEngine engine = new YamlDrivenEngine();
        PerfReport finalReport = engine.run(config, jsonLinesOut, runDir);

        // Write report
        try {
            ReportWriter.write(finalReport, runDir, formats);
        } catch (Exception e) {
            System.err.println("Failed to write report: " + e.getMessage());
            return ExitCode.REPORT_ERROR.code();
        }

        // Update latest/previous symlinks
        updateSymlinks(paths, scenario, runDir);

        System.err.println("Report written to: " + runDir);

        // Check thresholds
        if (cli.hasOption("check-thresholds")) {
            boolean passed = checkThresholds(config, finalReport);
            if (!passed) {
                return ExitCode.THRESHOLD_FAILED.code();
            }
        }

        // Diff against baseline
        if (cli.hasOption("diff-baseline")) {
            String baselineRef = cli.getOptionValue("diff-baseline");
            Path refDir = paths.resolveRef(baselineRef);
            Path baselineSummary = resolveSummaryJson(refDir.toString());
            if (Files.exists(baselineSummary)) {
                PerfReport baselineReport =
                        MAPPER.readValue(baselineSummary.toFile(), PerfReport.class);
                DiffResult diff = ReportDiffer.diff(baselineReport, finalReport);
                System.out.println(MAPPER.writeValueAsString(diff));
            } else {
                System.err.println("Baseline summary not found: " + baselineSummary);
            }
        }

        // Diff against previous run
        if (cli.hasOption("diff-previous")) {
            Path previousLink = paths.previousLink(scenario);
            if (Files.exists(previousLink)) {
                Path previousSummary = previousLink.resolve("summary.json");
                if (Files.exists(previousSummary)) {
                    PerfReport previousReport =
                            MAPPER.readValue(previousSummary.toFile(), PerfReport.class);
                    DiffResult diff = ReportDiffer.diff(previousReport, finalReport);
                    String diffJson = MAPPER.writeValueAsString(diff);
                    System.out.println(diffJson);
                    Files.writeString(runDir.resolve("diff-vs-previous.json"), diffJson);
                } else {
                    System.err.println("Previous summary not found: " + previousSummary);
                }
            } else {
                System.err.println("No previous run found for scenario: " + scenario);
            }
        }

        return "partial".equals(finalReport.status())
                ? ExitCode.EXECUTION_ERROR.code()
                : ExitCode.SUCCESS.code();
    }

    private static Options buildRunOptions() {
        Options options = new Options();
        options.addOption(
                Option.builder()
                        .longOpt("scenario-file")
                        .hasArg()
                        .desc("Scenario YAML file or preset name")
                        .build());
        options.addOption(
                Option.builder()
                        .longOpt("timeout")
                        .hasArg()
                        .desc(
                                "Global timeout, default 3600s. Do not set too small"
                                        + " — insufficient time produces unreliable results.")
                        .build());
        options.addOption(
                Option.builder()
                        .longOpt("cluster-config")
                        .hasArg()
                        .desc("Cluster config file")
                        .build());
        options.addOption(
                Option.builder()
                        .longOpt("client-config")
                        .hasArg()
                        .desc("Client config file")
                        .build());
        options.addOption(
                Option.builder().longOpt("jvm-args").hasArg().desc("Extra JVM arguments").build());
        options.addOption(
                Option.builder()
                        .longOpt("report-format")
                        .hasArg()
                        .desc("Report formats (comma-separated)")
                        .build());
        options.addOption(Option.builder().longOpt("seed").hasArg().desc("Random seed").build());
        options.addOption(
                Option.builder()
                        .longOpt("check-thresholds")
                        .desc("Check thresholds after run")
                        .build());
        options.addOption(
                Option.builder()
                        .longOpt("diff-baseline")
                        .hasArg()
                        .desc("Diff against a named baseline or path")
                        .build());
        options.addOption(
                Option.builder()
                        .longOpt("diff-previous")
                        .desc("Diff against the previous run of the same scenario")
                        .build());
        return options;
    }

    // -------------------------------------------------------------------------
    //  generate command
    // -------------------------------------------------------------------------

    private static int generateCommand(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(
                Option.builder()
                        .longOpt("scenario-file")
                        .hasArg()
                        .required()
                        .desc("Scenario YAML file")
                        .build());
        options.addOption(
                Option.builder()
                        .longOpt("output")
                        .hasArg()
                        .desc("Output dataset file path")
                        .build());
        options.addOption(
                Option.builder().longOpt("records").hasArg().desc("Number of records").build());
        options.addOption(
                Option.builder().longOpt("seed").hasArg().desc("Random seed override").build());

        CommandLine cli;
        try {
            cli = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println("Invalid options: " + e.getMessage());
            return ExitCode.CONFIG_ERROR.code();
        }

        String scenarioFile = cli.getOptionValue("scenario-file");
        String yaml = PerfConfig.loadYaml(scenarioFile);
        ScenarioConfig config = ScenarioConfig.parse(yaml);

        long records = 100_000;
        if (cli.hasOption("records")) {
            records = Long.parseLong(cli.getOptionValue("records"));
        } else if (config.workload() != null && !config.workload().isEmpty()) {
            Long configRecords = config.workload().get(0).records();
            if (configRecords != null) {
                records = configRecords;
            }
        }

        long seed = 42;
        if (cli.hasOption("seed")) {
            seed = Long.parseLong(cli.getOptionValue("seed"));
        } else if (config.data() != null && config.data().seed() != null) {
            seed = config.data().seed();
        }

        MicrobenchPaths paths = MicrobenchPaths.fromSystemProperty();
        Path datasetsDir = paths.datasetsDir();
        Files.createDirectories(datasetsDir);
        String fileName =
                cli.hasOption("output")
                        ? cli.getOptionValue("output")
                        : scenarioFile.replace(".yaml", "") + "-" + records + ".dat";
        Path outputPath = datasetsDir.resolve(fileName);

        if (config.table() == null
                || config.table().columns() == null
                || config.table().columns().isEmpty()) {
            System.err.println("Scenario file must define table.columns for dataset generation");
            return ExitCode.CONFIG_ERROR.code();
        }

        List<ColumnConfig> columns = config.table().columns();
        FieldGenerator[] generators = ExecutorUtils.buildGenerators(columns, config.data());

        // Build schema JSON for header
        StringBuilder schemaJson = new StringBuilder("[");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                schemaJson.append(',');
            }
            schemaJson
                    .append("{\"name\":\"")
                    .append(columns.get(i).name())
                    .append("\",\"type\":\"")
                    .append(columns.get(i).type())
                    .append("\"}");
        }
        schemaJson.append(']');

        System.out.printf("Generating %d records to %s (seed=%d)%n", records, outputPath, seed);

        try (DatasetWriter writer = new DatasetWriter(outputPath)) {
            writer.writeHeader(records, schemaJson.toString(), yaml, seed);
            for (long i = 0; i < records; i++) {
                Object[] fields = new Object[columns.size()];
                for (int c = 0; c < columns.size(); c++) {
                    fields[c] = generators[c].generate(i);
                }
                byte[] recordBytes = MAPPER.writeValueAsBytes(fields);
                writer.writeRecord(recordBytes);
                if ((i + 1) % 100_000 == 0) {
                    System.out.printf("  %d / %d records written%n", i + 1, records);
                }
            }
        }

        System.out.printf("Dataset generated: %s (%d records)%n", outputPath, records);
        return ExitCode.SUCCESS.code();
    }

    // -------------------------------------------------------------------------
    //  validate command
    // -------------------------------------------------------------------------

    private static int validateCommand(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(
                Option.builder()
                        .longOpt("scenario-file")
                        .hasArg()
                        .required()
                        .desc("Scenario YAML file")
                        .build());

        CommandLine cli;
        try {
            cli = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println("Invalid options: " + e.getMessage());
            return ExitCode.CONFIG_ERROR.code();
        }

        String yaml = PerfConfig.loadYaml(cli.getOptionValue("scenario-file"));
        ScenarioConfig config = ScenarioConfig.parse(yaml);
        List<String> errors = ScenarioValidator.validate(config);

        Map<String, Object> result = new LinkedHashMap<>();
        if (errors.isEmpty()) {
            result.put("valid", true);
        } else {
            result.put("valid", false);
            result.put("errors", errors);
        }
        if (!config.parseWarnings().isEmpty()) {
            result.put("warnings", config.parseWarnings());
        }
        System.out.println(MAPPER.writeValueAsString(result));
        return errors.isEmpty() ? ExitCode.SUCCESS.code() : ExitCode.CONFIG_ERROR.code();
    }

    // -------------------------------------------------------------------------
    //  diff command
    // -------------------------------------------------------------------------

    private static int diffCommand(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(
                Option.builder()
                        .longOpt("baseline")
                        .hasArg()
                        .required()
                        .desc("Baseline: name, path, or summary.json")
                        .build());
        options.addOption(
                Option.builder()
                        .longOpt("target")
                        .hasArg()
                        .required()
                        .desc("Target: name, path, or summary.json")
                        .build());

        CommandLine cli;
        try {
            cli = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println("Invalid options: " + e.getMessage());
            return ExitCode.CONFIG_ERROR.code();
        }

        MicrobenchPaths paths = MicrobenchPaths.fromSystemProperty();
        Path baselinePath = resolveRefToSummary(paths, cli.getOptionValue("baseline"));
        Path targetPath = resolveRefToSummary(paths, cli.getOptionValue("target"));

        if (!Files.exists(baselinePath)) {
            System.err.println("Baseline not found: " + baselinePath);
            return ExitCode.CONFIG_ERROR.code();
        }
        if (!Files.exists(targetPath)) {
            System.err.println("Target not found: " + targetPath);
            return ExitCode.CONFIG_ERROR.code();
        }

        PerfReport baseline = MAPPER.readValue(baselinePath.toFile(), PerfReport.class);
        PerfReport target = MAPPER.readValue(targetPath.toFile(), PerfReport.class);
        DiffResult diff = ReportDiffer.diff(baseline, target);
        System.out.println(MAPPER.writeValueAsString(diff));
        return ExitCode.SUCCESS.code();
    }

    // -------------------------------------------------------------------------
    //  baseline command
    // -------------------------------------------------------------------------

    private static int baselineCommand(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: baseline <save|list|delete> [options]");
            return ExitCode.CONFIG_ERROR.code();
        }

        String subCommand = args[0];
        String[] subArgs = Arrays.copyOfRange(args, 1, args.length);

        if ("save".equals(subCommand)) {
            return baselineSave(subArgs);
        } else if ("list".equals(subCommand)) {
            return baselineList(subArgs);
        } else if ("delete".equals(subCommand)) {
            return baselineDelete(subArgs);
        } else {
            System.err.println("Unknown baseline subcommand: " + subCommand);
            return ExitCode.CONFIG_ERROR.code();
        }
    }

    private static int baselineSave(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(
                Option.builder().longOpt("name").hasArg().required().desc("Baseline name").build());
        options.addOption(
                Option.builder()
                        .longOpt("run")
                        .hasArg()
                        .required()
                        .desc("Run reference (e.g. log-append/latest)")
                        .build());

        CommandLine cli;
        try {
            cli = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println("Invalid options: " + e.getMessage());
            return ExitCode.CONFIG_ERROR.code();
        }

        MicrobenchPaths paths = MicrobenchPaths.fromSystemProperty();
        Path runDir = paths.runsDir().resolve(cli.getOptionValue("run"));
        if (Files.isSymbolicLink(runDir)) {
            runDir = runDir.getParent().resolve(Files.readSymbolicLink(runDir));
        }
        BaselineManager bm = new BaselineManager(paths);
        bm.save(cli.getOptionValue("name"), runDir);
        System.out.println("Baseline saved: " + cli.getOptionValue("name"));
        return ExitCode.SUCCESS.code();
    }

    private static int baselineList(String[] args) throws Exception {
        Options options = new Options();
        CommandLine cli = new DefaultParser().parse(options, args);

        MicrobenchPaths paths = MicrobenchPaths.fromSystemProperty();
        BaselineManager bm = new BaselineManager(paths);
        List<String> baselines = bm.list();
        System.out.println(MAPPER.writeValueAsString(baselines));
        return ExitCode.SUCCESS.code();
    }

    private static int baselineDelete(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(
                Option.builder().longOpt("name").hasArg().required().desc("Baseline name").build());

        CommandLine cli;
        try {
            cli = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println("Invalid options: " + e.getMessage());
            return ExitCode.CONFIG_ERROR.code();
        }

        MicrobenchPaths paths = MicrobenchPaths.fromSystemProperty();
        BaselineManager bm = new BaselineManager(paths);
        bm.delete(cli.getOptionValue("name"));
        System.out.println("Baseline deleted: " + cli.getOptionValue("name"));
        return ExitCode.SUCCESS.code();
    }

    // -------------------------------------------------------------------------
    //  list command
    // -------------------------------------------------------------------------

    private static int listCommand(String[] args) throws Exception {
        List<Map<String, String>> presets = new ArrayList<>();
        try {
            // Scan classpath presets/ directory
            ClassLoader cl = PerfRunner.class.getClassLoader();
            // Try to list known presets by reading an index, or scan the resource directory
            try (InputStream indexStream = cl.getResourceAsStream("presets/")) {
                if (indexStream != null) {
                    String content = new String(indexStream.readAllBytes(), StandardCharsets.UTF_8);
                    // Directory listing from classpath (works in exploded dirs)
                    for (String line : content.split("\n")) {
                        String name = line.trim();
                        if (name.endsWith(".yaml")) {
                            Map<String, String> entry = new LinkedHashMap<>();
                            entry.put("name", name.replace(".yaml", ""));
                            entry.put("file", "presets/" + name);
                            presets.add(entry);
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Classpath scanning may not work in all environments
            System.err.println("Warning: could not scan classpath presets: " + e.getMessage());
        }
        System.out.println(MAPPER.writeValueAsString(presets));
        return ExitCode.SUCCESS.code();
    }

    // -------------------------------------------------------------------------
    //  clean command
    // -------------------------------------------------------------------------

    private static int cleanCommand(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(
                Option.builder()
                        .longOpt("before")
                        .hasArg()
                        .desc("Delete items older than duration (e.g. 7d)")
                        .build());
        options.addOption(Option.builder().longOpt("runs").desc("Clean runs only").build());
        options.addOption(
                Option.builder().longOpt("baselines").desc("Clean baselines only").build());
        options.addOption(Option.builder().longOpt("datasets").desc("Clean datasets only").build());

        CommandLine cli;
        try {
            cli = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println("Invalid options: " + e.getMessage());
            return ExitCode.CONFIG_ERROR.code();
        }

        MicrobenchPaths paths = MicrobenchPaths.fromSystemProperty();
        Path root = paths.root();

        if (!Files.isDirectory(root)) {
            System.out.println("Nothing to clean: " + root);
            return ExitCode.SUCCESS.code();
        }

        Instant cutoff = null;
        if (cli.hasOption("before")) {
            long beforeMs = parseDurationMs(cli.getOptionValue("before"));
            cutoff = Instant.now().minus(beforeMs, ChronoUnit.MILLIS);
        }

        boolean all =
                !cli.hasOption("runs") && !cli.hasOption("baselines") && !cli.hasOption("datasets");

        int deleted = 0;
        if (all || cli.hasOption("runs")) {
            deleted += cleanDir(paths.runsDir(), cutoff);
        }
        if (all || cli.hasOption("baselines")) {
            deleted += cleanDir(paths.baselinesDir(), cutoff);
        }
        if (all || cli.hasOption("datasets")) {
            deleted += cleanDir(paths.datasetsDir(), cutoff);
        }

        System.out.println("Cleaned " + deleted + " items from " + root);
        return ExitCode.SUCCESS.code();
    }

    private static int cleanDir(Path dir, Instant cutoff) throws IOException {
        if (!Files.exists(dir)) {
            return 0;
        }
        if (cutoff == null) {
            // Delete everything, then recreate the directory
            int[] count = {0};
            try (Stream<Path> paths = Files.walk(dir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(
                                p -> {
                                    try {
                                        Files.delete(p);
                                        count[0]++;
                                    } catch (IOException e) {
                                        System.err.println(
                                                "Warning: failed to delete "
                                                        + p
                                                        + ": "
                                                        + e.getMessage());
                                    }
                                });
            }
            Files.createDirectories(dir);
            // Don't count the directory itself
            return Math.max(0, count[0] - 1);
        } else {
            int deleted = 0;
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
                for (Path entry : stream) {
                    Instant modified = Files.getLastModifiedTime(entry).toInstant();
                    if (modified.isAfter(cutoff)) {
                        continue;
                    }
                    if (Files.isDirectory(entry)) {
                        deleted += cleanDir(entry, null);
                        Files.deleteIfExists(entry);
                    } else {
                        Files.deleteIfExists(entry);
                    }
                    deleted++;
                }
            }
            return deleted;
        }
    }

    // -------------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------------

    private static void printUsage() {
        System.err.println("Usage: fluss-microbench <command> [options]");
        System.err.println();
        System.err.println("Commands:");
        System.err.println("  run        Run a benchmark scenario");
        System.err.println("  generate   Generate a pre-built dataset file");
        System.err.println("  validate   Validate a scenario YAML file");
        System.err.println("  diff       Compare two run or baseline summaries");
        System.err.println("  baseline   Manage baselines (save, list, delete)");
        System.err.println("  list       List available preset scenarios");
        System.err.println("  clean      Clean output directories");
        System.err.println();
        System.err.println("Output root: $MICROBENCH_ROOT (default: .microbench/)");
        System.err.println();
        System.err.println("Workload parameters (records, threads, warmup, duration) are defined");
        System.err.println("in the scenario YAML file and cannot be overridden via CLI.");
        System.err.println("This ensures every benchmark report is statistically reliable.");
    }

    /** Spawns a daemon watchdog thread that exits the process after the given timeout. */
    private static void startTimeoutWatchdog(long timeoutMs) {
        Thread watchdog =
                new Thread(
                        () -> {
                            try {
                                Thread.sleep(timeoutMs);
                                System.err.println(
                                        "Timeout reached after " + timeoutMs + "ms, forcing exit.");
                                System.err.flush();
                                System.out.flush();
                                System.exit(ExitCode.TIMEOUT.code());
                            } catch (InterruptedException e) {
                                // Watchdog cancelled — normal shutdown
                            }
                        },
                        "perf-timeout-watchdog");
        watchdog.setDaemon(true);
        watchdog.start();
    }

    /** Checks configured thresholds against the report. Returns true if all pass. */
    private static boolean checkThresholds(ScenarioConfig config, PerfReport report) {
        ThresholdsConfig thresholds = config.thresholds();
        if (thresholds == null) {
            return true;
        }

        List<String> failures = new ArrayList<>();

        // Check heap-mb max
        if (thresholds.heapMb() != null && thresholds.heapMb().max() != null) {
            long maxHeapBytes = thresholds.heapMb().max() * 1024L * 1024L;
            for (ResourceSnapshot snap : report.snapshots()) {
                if (snap.clientHeapUsed > maxHeapBytes) {
                    failures.add(
                            "heap-mb max exceeded: "
                                    + (snap.clientHeapUsed / (1024 * 1024))
                                    + "MB > "
                                    + thresholds.heapMb().max()
                                    + "MB");
                    break;
                }
            }
        }

        // Check rss-peak-mb max
        if (thresholds.rssPeakMb() != null && thresholds.rssPeakMb().max() != null) {
            long maxRssBytes = thresholds.rssPeakMb().max() * 1024L * 1024L;
            for (ResourceSnapshot snap : report.snapshots()) {
                if (snap.client != null && snap.client.rssBytes > maxRssBytes) {
                    failures.add(
                            "rss-peak-mb max exceeded: "
                                    + (snap.client.rssBytes / (1024 * 1024))
                                    + "MB > "
                                    + thresholds.rssPeakMb().max()
                                    + "MB");
                    break;
                }
            }
        }

        // Check write-tps and p99-ms across non-warmup phases
        for (PhaseResult phase : report.phaseResults()) {
            if (phase.warmupOnly()) {
                continue;
            }
            if (thresholds.writeTps() != null && thresholds.writeTps().min() != null) {
                if (phase.opsPerSecond() < thresholds.writeTps().min()) {
                    failures.add(
                            phase.phaseName()
                                    + " TPS below threshold: "
                                    + String.format("%.0f", phase.opsPerSecond())
                                    + " < "
                                    + thresholds.writeTps().min());
                }
            }
            if (thresholds.p99Ms() != null && thresholds.p99Ms().max() != null) {
                long p99Ms = TimeUnit.NANOSECONDS.toMillis(phase.p99Nanos());
                if (p99Ms > thresholds.p99Ms().max()) {
                    failures.add(
                            phase.phaseName()
                                    + " P99 exceeded: "
                                    + p99Ms
                                    + "ms > "
                                    + thresholds.p99Ms().max()
                                    + "ms");
                }
            }
        }

        if (!failures.isEmpty()) {
            System.err.println("Threshold check FAILED:");
            for (String f : failures) {
                System.err.println("  - " + f);
            }
            return false;
        }
        return true;
    }

    /**
     * Resolves a path argument to a summary.json file. If the path points to a directory, appends
     * {@code summary.json}.
     */
    private static Path resolveSummaryJson(String pathStr) {
        Path path = Path.of(pathStr);
        if (Files.isDirectory(path)) {
            return path.resolve("summary.json");
        }
        return path;
    }

    /**
     * Resolves a ref (baseline name, run path, or direct file) to a summary.json path using {@link
     * MicrobenchPaths#resolveRef(String)} first, then falling back to direct path resolution.
     */
    private static Path resolveRefToSummary(MicrobenchPaths paths, String ref) {
        // If it looks like a direct file path to summary.json, use it as-is
        if (ref.endsWith(".json")) {
            return Path.of(ref);
        }
        // Try resolving via MicrobenchPaths (baselines/{ref} then runs/{ref})
        Path resolved = paths.resolveRef(ref);
        if (Files.isDirectory(resolved)) {
            return resolved.resolve("summary.json");
        }
        // Fall back to treating it as a direct filesystem path
        Path direct = Path.of(ref);
        if (Files.isDirectory(direct)) {
            return direct.resolve("summary.json");
        }
        return direct;
    }

    /**
     * Derives the scenario name from the config's {@code meta.name} or from the scenario file
     * argument (stripping path and extension).
     */
    private static String resolveScenarioName(ScenarioConfig config, String scenarioFileArg) {
        if (config.meta() != null
                && config.meta().name() != null
                && !config.meta().name().isEmpty()) {
            return config.meta().name();
        }
        // Strip directory and .yaml extension from the file argument
        String name = Path.of(scenarioFileArg).getFileName().toString();
        if (name.endsWith(".yaml") || name.endsWith(".yml")) {
            name = name.substring(0, name.lastIndexOf('.'));
        }
        return name;
    }

    /**
     * Updates the {@code latest} and {@code previous} symlinks for a scenario. The current {@code
     * latest} is moved to {@code previous} before pointing {@code latest} at the new run directory.
     */
    private static void updateSymlinks(MicrobenchPaths paths, String scenario, Path newRunDir)
            throws IOException {
        Path latestLink = paths.latestLink(scenario);
        Path previousLink = paths.previousLink(scenario);

        // Ensure parent directory exists
        Files.createDirectories(latestLink.getParent());

        // If latest already exists, move it to previous
        if (Files.isSymbolicLink(latestLink)) {
            Path oldTarget = Files.readSymbolicLink(latestLink);
            Files.deleteIfExists(previousLink);
            Files.createSymbolicLink(previousLink, oldTarget);
        }

        // Point latest at the new run directory
        Files.deleteIfExists(latestLink);
        Files.createSymbolicLink(latestLink, newRunDir.toAbsolutePath());
    }

    /**
     * Parses a duration string like "10m", "1h", "30s", "7d" into milliseconds. Supports suffixes:
     * s (seconds), m (minutes), h (hours), d (days).
     */
    static long parseDurationMs(String duration) {
        if (duration == null || duration.isEmpty()) {
            throw new IllegalArgumentException("Duration cannot be empty");
        }
        char unit = duration.charAt(duration.length() - 1);
        long value;
        if (Character.isDigit(unit)) {
            // No unit suffix — assume seconds
            return Long.parseLong(duration) * 1000;
        }
        value = Long.parseLong(duration.substring(0, duration.length() - 1));
        if (unit == 's') {
            return value * 1000;
        } else if (unit == 'm') {
            return value * 60 * 1000;
        } else if (unit == 'h') {
            return value * 60 * 60 * 1000;
        } else if (unit == 'd') {
            return value * 24 * 60 * 60 * 1000;
        } else {
            throw new IllegalArgumentException("Unknown duration unit: " + unit);
        }
    }
}
