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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.docs.ConfigOverrideDefault;
import org.apache.fluss.annotation.docs.ConfigSection;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Generator for configuration documentation. Produces a list-based MDX file grouped by sections
 * with support for default overrides and proper type formatting.
 */
public class ConfigOptionsDocGenerator {

    public static void main(String[] args) throws Exception {
        Path root = findProjectRoot();
        String projectRoot = (root != null) ? root.toString() : System.getProperty("user.dir");
        String outputPath = projectRoot + "/website/docs/_configs/_partial_config.mdx";

        File outputFile = new File(outputPath);
        File parent = outputFile.getParentFile();
        if (parent != null && !parent.exists()) {
            if (!parent.mkdirs()) {
                throw new RuntimeException("Failed to create directory: " + parent);
            }
        }

        String content = generateMDXContent();
        Files.write(
                outputFile.toPath(), Collections.singletonList(content), StandardCharsets.UTF_8);
        System.out.println("SUCCESS: Generated " + outputFile.getAbsolutePath());
    }

    private static String generateMDXContent() throws IllegalAccessException {
        StringBuilder builder = new StringBuilder();
        builder.append("{/* This file is auto-generated. Do not edit directly. */}\n\n");

        Field[] fields = ConfigOptions.class.getDeclaredFields();
        Map<String, List<Field>> sections = new TreeMap<>();

        for (Field field : fields) {
            // Using name check to avoid retention issues with Internal.class if necessary
            if (field.isAnnotationPresent(Internal.class)) {
                continue;
            }

            if (field.getType().equals(ConfigOption.class)) {
                ConfigOption<?> option = (ConfigOption<?>) field.get(null);
                String sectionName = getSectionName(field, option);
                sections.computeIfAbsent(sectionName, k -> new ArrayList<>()).add(field);
            }
        }

        for (Map.Entry<String, List<Field>> entry : sections.entrySet()) {
            builder.append("## ").append(entry.getKey()).append(" Configurations\n\n");

            List<Field> sectionFields = entry.getValue();
            sectionFields.sort(Comparator.comparing(f -> getOptionKey(f)));

            for (Field field : sectionFields) {
                ConfigOption<?> option = (ConfigOption<?>) field.get(null);

                String defaultValue = getFormattedDefaultValue(field, option);
                String scope = getScope(option.key());
                String description = cleanDescription(option.description());
                boolean isDeprecated = field.isAnnotationPresent(Deprecated.class);

                builder.append("### `")
                        .append(option.key())
                        .append("` {#")
                        .append(option.key().replace(".", "-"))
                        .append("}\n\n");

                if (isDeprecated) {
                    builder.append("> **Warning**: This configuration is **Deprecated**.\n\n");
                }
                builder.append("* **Default**: `").append(defaultValue).append("`\n");
                builder.append("* **Type**: ").append(getType(field)).append("\n");
                builder.append("* **Scope**: ").append(scope).append("\n\n");

                builder.append(description).append("\n\n");
                builder.append("---\n\n");
            }
        }
        return builder.toString();
    }

    private static String getOptionKey(Field field) {
        try {
            return ((ConfigOption<?>) field.get(null)).key();
        } catch (IllegalAccessException e) {
            return "";
        }
    }

    private static String getFormattedDefaultValue(Field field, ConfigOption<?> option) {
        if (field.isAnnotationPresent(ConfigOverrideDefault.class)) {
            return field.getAnnotation(ConfigOverrideDefault.class).value();
        }
        Object defaultValue = option.defaultValue();
        if (defaultValue == null) {
            return "(none)";
        }
        return defaultValue.toString();
    }

    private static String getSectionName(Field field, ConfigOption<?> option) {
        if (field.isAnnotationPresent(ConfigSection.class)) {
            return field.getAnnotation(ConfigSection.class).value();
        }
        String key = option.key();
        if (key.contains(".")) {
            return capitalize(key.split("\\.")[0]);
        }
        return "Common";
    }

    private static String getScope(String key) {
        if (key.startsWith("table.")) {
            return "Table";
        } else if (key.startsWith("client.")) {
            return "Client";
        }
        return "Server";
    }

    private static String cleanDescription(String desc) {
        if (desc == null) {
            return "";
        }
        return desc.replace("\n", " ")
                .replace("\r", " ")
                .replace("<", "&lt;")
                .replace("{", "&#123;")
                .replace("}", "&#125;")
                .replace("%s", "true");
    }

    private static String getType(Field field) {
        String typeName = "String";
        try {
            Type genericType = field.getGenericType();
            if (genericType instanceof ParameterizedType) {
                Type[] typeArgs = ((ParameterizedType) genericType).getActualTypeArguments();
                if (typeArgs.length > 0) {
                    String fullTypeName = typeArgs[0].getTypeName();
                    typeName = extractSimpleName(fullTypeName);
                }
            }
        } catch (Exception ignored) {
            // fallback
        }

        switch (typeName.toLowerCase()) {
            case "integer":
                return "Int";
            case "long":
                return "Long";
            case "boolean":
                return "Boolean";
            case "duration":
                return "Duration";
            case "memorysize":
                return "MemorySize";
            case "double":
                return "Double";
            case "list":
                return "List";
            case "string":
                return "String";
            default:
                return typeName;
        }
    }

    private static String extractSimpleName(String fullTypeName) {
        String simple = fullTypeName.substring(fullTypeName.lastIndexOf(".") + 1);
        return simple.replace("$", "").replace(">", "");
    }

    private static String capitalize(String str) {
        return (str == null || str.isEmpty())
                ? str
                : str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private static Path findProjectRoot() {
        Path root = Paths.get(System.getProperty("user.dir"));
        while (root != null && !Files.exists(root.resolve("pom.xml"))) {
            root = root.getParent();
        }
        return root;
    }
}
