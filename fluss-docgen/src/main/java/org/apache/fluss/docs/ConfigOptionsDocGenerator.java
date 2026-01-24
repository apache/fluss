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
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

/**
 * Generator for configuration documentation.
 */
public class ConfigOptionsDocGenerator {

    public static void main(String[] args) {
        // Define the output path (puts it in the fluss-docgen/target folder)
        String outputPath = "fluss-docgen/target/config_reference.md";

        try (PrintWriter writer = new PrintWriter(outputPath, StandardCharsets.UTF_8.name())) {
            writer.println("# Configuration Reference");
            writer.println();
            generateList(ConfigOptions.class, writer);
            System.out.println("Success! Documentation generated at: " + outputPath);
        } catch (IOException e) {
            System.err.println("Failed to write documentation file: " + e.getMessage());
        }
    }

    private static void generateList(Class<?> clazz, PrintWriter writer) {
        Field[] fields = clazz.getDeclaredFields();

        for (Field field : fields) {
            if (field.getType().equals(ConfigOption.class)) {
                try {
                    ConfigOption<?> option = (ConfigOption<?>) field.get(null);

                    writer.println("### " + option.key());
                    writer.println("- **Default:** " + ConfigDocUtils.formatDefaultValue(option.defaultValue()));
                    writer.println("- **Description:** " + option.description());
                    writer.println();

                } catch (IllegalAccessException e) {
                    System.err.println("Could not access field: " + field.getName());
                }
            }
        }
    }
}
