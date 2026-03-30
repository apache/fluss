/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.fluss.annotation.docs.ConfigOverrideDefault;
import org.apache.fluss.annotation.docs.ConfigSection;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Generator for configuration documentation. */
public class ConfigOptionsDocGenerator {

    public static void main(String[] args) throws Exception {
        Path root = findProjectRoot();
        String projectRoot = (root != null) ? root.toString() : System.getProperty("user.dir");
        String outputPath = projectRoot + "/website/docs/_configs/_partial_config.mdx";

        File outputFile = new File(outputPath);
        outputFile.getParentFile().mkdirs();

        System.out.println("Generating MDX partial: " + outputFile.getAbsolutePath());

        if (!outputFile.getParentFile().exists()) {
            outputFile.getParentFile().mkdirs();
        }

        String content = generateMDXContent();
        Files.write(
                outputFile.toPath(), Collections.singletonList(content), StandardCharsets.UTF_8);

        System.out.println("SUCCESS: MDX partial generated.");
    }

    private static String generateMDXContent() throws IllegalAccessException {
        StringBuilder builder = new StringBuilder();
        builder.append("{/* This file is auto-generated. Do not edit directly. */}\n\n");

        Field[] fields = ConfigOptions.class.getDeclaredFields();
        Map<String, List<Field>> sections = new TreeMap<>();

        for (Field field : fields) {
            if (field.getType().equals(ConfigOption.class)) {
                String section = "Common";
                if (field.isAnnotationPresent(ConfigSection.class)) {
                    section = field.getAnnotation(ConfigSection.class).value();
                } else {
                    ConfigOption<?> option = (ConfigOption<?>) field.get(null);
                    String key = option.key();
                    if (key != null && key.contains(".")) {
                        section = capitalize(key.split("\\.")[0]);
                    }
                }
                sections.computeIfAbsent(section, k -> new ArrayList<>()).add(field);
            }
        }

        for (Map.Entry<String, List<Field>> entry : sections.entrySet()) {
            builder.append("## ").append(entry.getKey()).append(" Configurations\n\n");

            builder.append("| Key | Default | Type | Description |\n");
            builder.append("| :--- | :--- | :--- | :--- |\n");

            for (Field field : entry.getValue()) {
                ConfigOption<?> option = (ConfigOption<?>) field.get(null);

                String defaultValue = ConfigDocUtils.formatDefaultValue(option);
                if (field.isAnnotationPresent(ConfigOverrideDefault.class)) {
                    defaultValue = field.getAnnotation(ConfigOverrideDefault.class).value();
                }

                // IMPORTANT: MDX hates unescaped < or { symbols in text
                String description =
                        option.description()
                                .replace("\n", " ")
                                .replace("\r", " ")
                                .replace("|", "\\|")
                                .replace("<", "&lt;") // Escape for MDX
                                .replace("{", "&#123;") // Escape for MDX
                                .replace("}", "&#125;") // Escape for MDX
                                .replace("%s", "");

                builder.append("| `")
                        .append(option.key())
                        .append("` | `")
                        .append(defaultValue.replace("<", "&lt;"))
                        .append("` | ")
                        .append(getType(option))
                        .append(" | ")
                        .append(description)
                        .append(" |\n");
            }
            builder.append("\n");
        }
        return builder.toString();
    }

    private static String getType(ConfigOption<?> option) {
        Object def = option.defaultValue();
        if (def != null) {
            return def.getClass().getSimpleName();
        }
        return "String";
    }

    private static String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private static Path findProjectRoot() {
        Path root = Paths.get(System.getProperty("user.dir"));
        while (root != null && !Files.exists(root.resolve("pom.xml"))) {
            root = root.getParent();
        }
        return root;
    }
}
