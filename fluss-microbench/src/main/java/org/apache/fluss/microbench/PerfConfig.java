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

import org.apache.fluss.microbench.config.ClientConfig;
import org.apache.fluss.microbench.config.ClusterConfig;
import org.apache.fluss.microbench.config.DataConfig;
import org.apache.fluss.microbench.config.ReportConfig;
import org.apache.fluss.microbench.config.ScenarioConfig;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads a {@link ScenarioConfig} from a YAML file or classpath preset and applies CLI overrides.
 */
public class PerfConfig {

    private PerfConfig() {}

    /**
     * Loads a scenario configuration from the given file path or classpath preset name, then
     * applies any CLI overrides.
     *
     * @param presetOrFile a file path or classpath preset name (without {@code .yaml} suffix)
     * @param cli parsed command-line options
     * @return the merged configuration
     */
    public static ScenarioConfig load(String presetOrFile, CommandLine cli) throws IOException {
        String yaml = loadYaml(presetOrFile);
        ScenarioConfig config = ScenarioConfig.parse(yaml);
        applyOverrides(config, cli);
        return config;
    }

    /** Loads raw YAML content from a file path or classpath preset. */
    static String loadYaml(String presetOrFile) throws IOException {
        Path filePath = Path.of(presetOrFile);
        if (Files.exists(filePath)) {
            return Files.readString(filePath, StandardCharsets.UTF_8);
        }

        // Try classpath preset
        String resourcePath = "presets/" + presetOrFile + ".yaml";
        try (InputStream in = PerfConfig.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in != null) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }

        throw new IOException(
                "Scenario not found: '"
                        + presetOrFile
                        + "' (not a file and not a classpath preset)");
    }

    /** Applies CLI overrides to the parsed config. */
    private static void applyOverrides(ScenarioConfig config, CommandLine cli) {
        // --report-format override
        if (cli.hasOption("report-format")) {
            ReportConfig reportConfig = config.report();
            if (reportConfig == null) {
                reportConfig = new ReportConfig();
                config.setReport(reportConfig);
            }
            String[] formats = cli.getOptionValue("report-format").split(",");
            reportConfig.setFormats(Arrays.asList(formats));
        }

        // --seed override
        if (cli.hasOption("seed")) {
            DataConfig dataConfig = config.data();
            if (dataConfig == null) {
                dataConfig = new DataConfig();
                config.setData(dataConfig);
            }
            dataConfig.setSeed(parseLongOption(cli, "seed"));
        }

        // --cluster-config override (key=value pairs merged into cluster.config)
        if (cli.hasOption("cluster-config")) {
            ClusterConfig clusterConfig = config.cluster();
            if (clusterConfig == null) {
                clusterConfig = new ClusterConfig();
                config.setCluster(clusterConfig);
            }
            clusterConfig.setConfig(
                    mergeKeyValueOverrides(
                            clusterConfig.configOverrides(),
                            cli.getOptionValues("cluster-config")));
        }

        // --client-config override (key=value pairs merged into client.properties)
        if (cli.hasOption("client-config")) {
            ClientConfig clientConfig = config.client();
            if (clientConfig == null) {
                clientConfig = new ClientConfig();
                config.setClient(clientConfig);
            }
            clientConfig.setProperties(
                    mergeKeyValueOverrides(
                            clientConfig.properties(), cli.getOptionValues("client-config")));
        }

        // --jvm-args override
        if (cli.hasOption("jvm-args")) {
            ClusterConfig clusterConfig = config.cluster();
            if (clusterConfig == null) {
                clusterConfig = new ClusterConfig();
                config.setCluster(clusterConfig);
            }
            clusterConfig.setJvmArgs(
                    new ArrayList<>(Arrays.asList(cli.getOptionValue("jvm-args").split("\\s+"))));
        }
    }

    /**
     * Returns the effective report formats, defaulting to {@code ["json", "csv"]} if not
     * configured.
     */
    public static List<String> getReportFormats(ScenarioConfig config) {
        if (config.report() != null && config.report().formats() != null) {
            return config.report().formats();
        }
        return Arrays.asList("json", "csv");
    }

    private static Map<String, String> mergeKeyValueOverrides(
            Map<String, String> existing, String[] cliValues) {
        Map<String, String> merged = existing != null ? new HashMap<>(existing) : new HashMap<>();
        for (String kv : cliValues) {
            String[] parts = kv.split("=", 2);
            if (parts.length == 2) {
                merged.put(parts[0].trim(), parts[1].trim());
            }
        }
        return merged;
    }

    private static long parseLongOption(CommandLine cli, String option) {
        String value = cli.getOptionValue(option);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid value for --" + option + ": '" + value + "' (expected a number)", e);
        }
    }
}
