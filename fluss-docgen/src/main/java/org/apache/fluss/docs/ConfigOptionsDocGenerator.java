/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.docs;

import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Generator for configuration documentation.
 *
 * <p>This tool uses reflection to scan {@link ConfigOptions} and generates a categorized Markdown
 * representation of all available configurations, injecting it into the documentation file.
 */
public class ConfigOptionsDocGenerator {

    private static final String BEGIN_MARKER = "";
    private static final String END_MARKER = "";

    public static void main(String[] args) throws Exception {
        // Resolve project root directory reliably.
        Path projectRoot = Paths.get(System.getProperty("user.dir"));

        // If we are deep in fluss-docgen, go up to the project root.
        while (projectRoot != null && !Files.exists(projectRoot.resolve("pom.xml"))) {
            projectRoot = projectRoot.getParent();
        }

        if (projectRoot == null) {
            throw new RuntimeException(
                    "Could not find project root (pom.xml not found in parent hierarchy)");
        }

        File configFile = projectRoot.resolve("website/docs/configuration.md").toFile();
        Path configPath = configFile.toPath();

        System.out.println("Processing documentation file: " + configFile.getAbsolutePath());

        // Create directory structure if missing.
        if (!configFile.getParentFile().exists()) {
            configFile.getParentFile().mkdirs();
        }

        // Initialize markers if file is missing.
        if (!configFile.exists()) {
            List<String> initialLines =
                    Arrays.asList("# Configuration Reference", "", BEGIN_MARKER, END_MARKER);
            Files.write(configPath, initialLines, StandardCharsets.UTF_8);
        }

        String generatedContent = generateContent();
        updateFile(configPath, generatedContent);

        System.out.println("SUCCESS: Configuration documentation updated successfully.");
    }

    /** Scans ConfigOptions and groups them by their key prefix. */
    private static String generateContent() throws IllegalAccessException {
        StringBuilder builder = new StringBuilder();
        Field[] fields = ConfigOptions.class.getDeclaredFields();
        Map<String, List<ConfigOption<?>>> groupedOptions = new TreeMap<>();

        for (Field field : fields) {
            if (field.getType().equals(ConfigOption.class)) {
                ConfigOption<?> option = (ConfigOption<?>) field.get(null);
                String key = option.key();
                // Categorize based on the first part of the configuration key.
                String category = key.contains(".") ? key.split("\\.")[0] : "common";
                groupedOptions.computeIfAbsent(category, k -> new ArrayList<>()).add(option);
            }
        }

        for (Map.Entry<String, List<ConfigOption<?>>> entry : groupedOptions.entrySet()) {
            builder.append("\n## ")
                    .append(capitalize(entry.getKey()))
                    .append(" Configurations\n\n");
            for (ConfigOption<?> option : entry.getValue()) {
                builder.append("### ")
                        .append(option.key())
                        .append("\n")
                        .append("- **Default:** ")
                        .append(ConfigDocUtils.formatDefaultValue(option))
                        .append("\n")
                        .append("- **Description:** ")
                        .append(option.description())
                        .append("\n\n");
            }
        }
        return builder.toString();
    }

    /** Injects the generated content between the defined HTML markers in the target file. */
    private static void updateFile(Path path, String content) throws IOException {
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        List<String> resultLines = new ArrayList<>();
        boolean inGeneratedSection = false;
        boolean markersFound = false;

        for (String line : lines) {
            String trimmedLine = line.trim();
            if (trimmedLine.equals(BEGIN_MARKER)) {
                resultLines.add(line);
                resultLines.add(content);
                inGeneratedSection = true;
                markersFound = true;
            } else if (trimmedLine.equals(END_MARKER)) {
                resultLines.add(line);
                inGeneratedSection = false;
            } else if (!inGeneratedSection) {
                resultLines.add(line);
            }
        }

        if (!markersFound) {
            resultLines.clear();
            resultLines.add("# Configuration Reference");
            resultLines.add("");
            resultLines.add(BEGIN_MARKER);
            resultLines.add(content);
            resultLines.add(END_MARKER);
        }

        Files.write(path, resultLines, StandardCharsets.UTF_8);
    }

    private static String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}
