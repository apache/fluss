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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.FLUSS_CONF_PREFIX;

/** Utils to verify whether the existing Paimon table is compatible with the table to be created. */
public class PaimonTableValidation {

    private static final Map<String, ConfigOption<?>> PAIMON_CONFIGS = extractPaimonConfigs();

    public static void validatePaimonSchemaCompatible(
            Identifier tablePath, Schema existingSchema, Schema newSchema) {
        // Adjust options for comparison
        Map<String, String> existingOptions = existingSchema.options();
        Map<String, String> newOptions = newSchema.options();

        // when enable datalake with an existing table, `table.datalake.enabled` will be `false`
        // in existing options, but `true` in new options.
        String datalakeConfigKey =
                FLUSS_CONF_PREFIX
                        + org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED.key();
        if (Boolean.FALSE.toString().equalsIgnoreCase(existingOptions.get(datalakeConfigKey))) {
            existingOptions.remove(datalakeConfigKey);
            newOptions.remove(datalakeConfigKey);
        }

        // remove changeable options
        removeChangeableOptions(existingOptions);
        removeChangeableOptions(newOptions);

        // ignore the existing options that are not in new options
        existingOptions.entrySet().removeIf(entry -> !newOptions.containsKey(entry.getKey()));

        if (!existingSchema.equals(newSchema)) {
            throw new TableAlreadyExistException(
                    String.format(
                            "The table %s already exists in Paimon catalog, but the table schema is not compatible. "
                                    + "Existing schema: %s, new schema: %s. "
                                    + "Please first drop the table in Paimon catalog or use a new table name.",
                            tablePath.getEscapedFullName(), existingSchema, newSchema));
        }
    }

    private static void removeChangeableOptions(Map<String, String> options) {
        options.entrySet()
                .removeIf(
                        entry ->
                                // currently we take all Paimon options and Fluss option as
                                // unchangeable.
                                !PAIMON_CONFIGS.containsKey(entry.getKey())
                                        && !entry.getKey().startsWith(FLUSS_CONF_PREFIX));
    }

    public static void checkTableIsEmpty(TablePath tablePath, FileStoreTable table) {
        if (table.latestSnapshot().isPresent()) {
            throw new TableAlreadyExistException(
                    String.format(
                            "The table %s already exists in Paimon catalog, and the table is not empty. "
                                    + "Please first drop the table in Paimon catalog or use a new table name.",
                            tablePath));
        }
    }

    private static Map<String, ConfigOption<?>> extractPaimonConfigs() {
        Map<String, ConfigOption<?>> options = new HashMap<>();

        Field[] fields = CoreOptions.class.getFields();
        for (Field field : fields) {
            if (!ConfigOption.class.isAssignableFrom(field.getType())) {
                continue;
            }

            try {
                ConfigOption<?> configOption = (ConfigOption<?>) field.get(null);
                options.put(configOption.key(), configOption);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(
                        "Unable to extract ConfigOption fields from CoreOptions class.", e);
            }
        }

        return options;
    }
}
