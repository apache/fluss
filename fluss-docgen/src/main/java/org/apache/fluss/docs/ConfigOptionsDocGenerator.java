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

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Generator for configuration documentation that injects categorized content into MD files.
 */
public class ConfigOptionsDocGenerator {

    private static final String BEGIN_MARKER = "";
    private static final String END_MARKER = "";

    public static void main(String[] args) throws Exception {
        // 1. Get the current working directory
        String projectRoot = System.getProperty("user.dir");

        // 2. Target the Docusaurus content directory in the website module
        // Based on your screenshot, the docs live in 'website/docs'
        Path path = Paths.get(projectRoot, "website", "docs", "configuration.md");

        // 3. Generate content
        String newContent = generateContent();

        // 4. Update the file
        updateFile(path, newContent);

        System.out.println(" Step 2 Complete: Injected into Docusaurus docs at:");
        System.out.println(path.toAbsolutePath());
    }

    private static String generateContent() throws IllegalAccessException {
        StringBuilder builder = new StringBuilder();
        Field[] fields = ConfigOptions.class.getDeclaredFields();

        // Use a TreeMap to keep categories sorted alphabetically
        Map<String, List<ConfigOption<?>>> groups = new TreeMap<>();

        for (Field field : fields) {
            if (field.getType().equals(ConfigOption.class)) {
                ConfigOption<?> option = (ConfigOption<?>) field.get(null);
                String key = option.key();

                // Get the category from the first part of the key (e.g., "server" from "server.port")
                String category = key.contains(".") ? key.split("\\.")[0] : "common";
                groups.computeIfAbsent(category, k -> new ArrayList<>()).add(option);
            }
        }

        // Build the Markdown sections
        for (Map.Entry<String, List<ConfigOption<?>>> entry : groups.entrySet()) {
            builder.append("## ").append(capitalize(entry.getKey())).append(" Configurations\n\n");

            for (ConfigOption<?> option : entry.getValue()) {
                builder.append("### ").append(option.key()).append("\n")
                        .append("- **Default:** ").append(ConfigDocUtils.formatDefaultValue(option.defaultValue())).append("\n")
                        .append("- **Description:** ").append(option.description()).append("\n\n");
            }
        }
        return builder.toString();
    }

    private static void updateFile(Path path, String content) throws IOException {
        // Check if the file exists. If not, create it with markers.
        if (!Files.exists(path)) {
            System.out.println("File not found. Creating new file at: " + path.toAbsolutePath());
            List<String> initialLines = new ArrayList<>();
            initialLines.add("# Configuration Reference");
            initialLines.add("");
            initialLines.add(BEGIN_MARKER);
            initialLines.add(END_MARKER);

            // Ensure the directory exists
            Files.createDirectories(path.getParent());
            Files.write(path, initialLines, StandardCharsets.UTF_8);
        }

        // Now it's safe to read
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        List<String> newLines = new ArrayList<>();
        boolean insideGeneratedSection = false;

        for (String line : lines) {
            if (line.contains(BEGIN_MARKER)) {
                newLines.add(line);
                newLines.add(content);
                insideGeneratedSection = true;
            } else if (line.contains(END_MARKER)) {
                newLines.add(line);
                insideGeneratedSection = false;
            } else if (!insideGeneratedSection) {
                newLines.add(line);
            }
        }

        Files.write(path, newLines, StandardCharsets.UTF_8);
    }

    private static String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}