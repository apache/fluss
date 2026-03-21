package org.apache.fluss.docs;

import org.apache.fluss.annotation.Documentation;
import org.apache.fluss.annotation.Internal;
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

    static String generateMDXContent() throws IllegalAccessException {
        StringBuilder builder = new StringBuilder();
        builder.append("{/* This file is auto-generated. Do not edit directly. */}\n\n");

        Field[] fields = ConfigOptions.class.getDeclaredFields();
        Map<String, List<Field>> sections = new TreeMap<>();

        for (Field field : fields) {
            if (field.isAnnotationPresent(Internal.class)) {
                continue;
            }
            if (!field.getType().equals(ConfigOption.class)) {
                continue;
            }

            // Skip fields with no @Documentation.Section — they are intentionally internal
            if (!field.isAnnotationPresent(Documentation.Section.class)) {
                continue;
            }

            sections.computeIfAbsent(
                            field.getAnnotation(Documentation.Section.class).value(),
                            k -> new ArrayList<>())
                    .add(field);
        }

        for (Map.Entry<String, List<Field>> entry : sections.entrySet()) {
            builder.append("## ").append(entry.getKey()).append("\n\n");

            List<Field> sectionFields = entry.getValue();
            sectionFields.sort(Comparator.comparing(ConfigOptionsDocGenerator::getOptionKey));

            for (Field field : sectionFields) {
                ConfigOption<?> option = (ConfigOption<?>) field.get(null);
                appendOptionEntry(builder, field, option);
            }
        }
        return builder.toString();
    }

    private static void appendOptionEntry(
            StringBuilder builder, Field field, ConfigOption<?> option) {
        String defaultValue = getFormattedDefaultValue(field, option);
        String description = cleanDescription(option.description());
        boolean isDeprecated = field.isAnnotationPresent(Deprecated.class);

        builder.append("### `")
                .append(option.key())
                .append("` {#")
                .append(option.key().replace(".", "-"))
                .append("}\n\n");

        if (isDeprecated) {
            builder.append("> **Deprecated**: ");
            // Extract the deprecation note from description if present
            if (description.contains("deprecated")) {
                builder.append("Use the replacement option described below.\n\n");
            } else {
                builder.append("This configuration option is deprecated.\n\n");
            }
        }

        builder.append("* **Default**: `").append(defaultValue).append("`\n");
        builder.append("* **Type**: ").append(getType(field)).append("\n\n");
        builder.append(description).append("\n\n");
        builder.append("---\n\n");
    }

    private static String getOptionKey(Field field) {
        try {
            return ((ConfigOption<?>) field.get(null)).key();
        } catch (IllegalAccessException e) {
            return "";
        }
    }

    static String getFormattedDefaultValue(Field field, ConfigOption<?> option) {
        // @Documentation.OverrideDefault takes priority
        if (field.isAnnotationPresent(Documentation.OverrideDefault.class)) {
            return field.getAnnotation(Documentation.OverrideDefault.class).value();
        }
        return ConfigDocUtils.formatDefaultValue(option);
    }

    static String cleanDescription(String desc) {
        if (desc == null || desc.isEmpty()) {
            return "(No description provided.)";
        }
        return desc.replace("\n", " ")
                .replace("\r", " ")
                .replace("<", "&lt;")
                .replace("{", "&#123;")
                .replace("}", "&#125;")
                // Remove markdown link syntax [text](url) -> text
                .replaceAll("\\[(.*?)\\]\\s*\\(.*?\\)", "$1")
                .trim();
    }

    static String getType(Field field) {
        try {
            Type genericType = field.getGenericType();
            if (genericType instanceof ParameterizedType) {
                Type[] typeArgs = ((ParameterizedType) genericType).getActualTypeArguments();
                if (typeArgs.length > 0) {
                    String fullTypeName = typeArgs[0].getTypeName();
                    return mapTypeName(extractSimpleName(fullTypeName));
                }
            }
        } catch (Exception ignored) {
            // fallback to String
        }
        return "String";
    }

    private static String mapTypeName(String typeName) {
        switch (typeName.toLowerCase()) {
            case "integer":
                return "Integer";
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
                return "List<String>";
            case "map":
                return "Map<String,String>";
            case "string":
                return "String";
            default:
                return typeName;
        }
    }

    private static String extractSimpleName(String fullTypeName) {
        // Get simple name after last dot
        String simple = fullTypeName.substring(fullTypeName.lastIndexOf(".") + 1);
        if (simple.contains("$")) {
            simple = simple.substring(simple.lastIndexOf("$") + 1);
        }
        simple = simple.replace(">", "");
        return simple;
    }

    private static Path findProjectRoot() {
        Path root = Paths.get(System.getProperty("user.dir"));
        while (root != null && !Files.exists(root.resolve("pom.xml"))) {
            root = root.getParent();
        }
        return root;
    }
}
