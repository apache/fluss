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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.paimon.PaimonLakeCatalog.SYSTEM_COLUMNS;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.FLUSS_CONF_PREFIX;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

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
            // Check if the existing schema is a legacy schema with system columns.
            // When re-enabling datalake for an existing legacy table, the existing Paimon
            // table may have system columns (__bucket, __offset, __timestamp), while the
            // new expected schema doesn't include them. In this case, we need to adjust the
            // expected schema to include these legacy system columns for backward compatibility.
            Schema adjustedNewSchema =
                    adjustSchemaForLegacySystemColumns(existingSchema, newSchema);
            if (existingSchema.equals(adjustedNewSchema)
                    || equalIgnoreSystemColumnTimestampPrecision(
                            existingSchema, adjustedNewSchema)) {
                return;
            }

            // Allow different precisions for __timestamp column for backward compatibility,
            // old cluster will use precision 6, but new cluster will use precision 3,
            // we allow such precision difference
            if (equalIgnoreSystemColumnTimestampPrecision(existingSchema, newSchema)) {
                return;
            }

            throw new TableAlreadyExistException(
                    String.format(
                            "The table %s already exists in Paimon catalog, but the table schema is not compatible. "
                                    + "Existing schema: %s, new schema: %s. "
                                    + "Please first drop the table in Paimon catalog or use a new table name.",
                            tablePath.getEscapedFullName(), existingSchema, newSchema));
        }
    }

    /**
     * Adjust the new schema to include legacy system columns if the existing schema has them.
     *
     * <p>This handles the case where a legacy table (created before the schema optimization) has
     * system columns in its Paimon table. When re-enabling datalake for such a table, we need to
     * adjust the expected schema to include these system columns for backward compatibility.
     *
     * @param existingSchema the schema currently persisted in the Paimon catalog
     * @param newSchema the new schema descriptor generated by the current Fluss cluster
     * @return adjusted schema with legacy system columns if applicable, otherwise the original new
     *     schema
     */
    private static Schema adjustSchemaForLegacySystemColumns(
            Schema existingSchema, Schema newSchema) {
        // Check if the existing schema has the __timestamp system column (indicator of legacy mode)
        List<DataField> existingFields = existingSchema.fields();
        boolean hasLegacySystemColumns =
                existingFields.stream()
                        .anyMatch(field -> field.name().equals(TIMESTAMP_COLUMN_NAME));

        if (!hasLegacySystemColumns) {
            return newSchema;
        }

        // Build a new schema with the legacy system columns appended
        Schema.Builder schemaBuilder = Schema.newBuilder();

        // Add all fields from new schema
        for (DataField field : newSchema.fields()) {
            schemaBuilder.column(field.name(), field.type(), field.description());
        }

        // Add legacy system columns
        for (Map.Entry<String, DataType> systemColumn : SYSTEM_COLUMNS.entrySet()) {
            schemaBuilder.column(systemColumn.getKey(), systemColumn.getValue());
        }

        // Copy other schema properties
        schemaBuilder.primaryKey(newSchema.primaryKeys());
        schemaBuilder.partitionKeys(newSchema.partitionKeys());
        schemaBuilder.options(newSchema.options());
        schemaBuilder.comment(newSchema.comment());

        return schemaBuilder.build();
    }

    /**
     * Check if the {@code existingSchema} is compatible with {@code newSchema} by ignoring the
     * precision difference of the system column {@code __timestamp}.
     *
     * <p>This is crucial for backward compatibility during cluster upgrades or configuration
     * changes (e.g., transitioning from precision 6 to 3). Without this relaxed check, users would
     * be unable to re-enable lake synchronization for existing tables if the cluster-wide default
     * timestamp precision has evolved.
     *
     * @param existingSchema the schema currently persisted in the Paimon catalog
     * @param newSchema the new schema descriptor generated by the current Fluss cluster
     * @return true if the schemas are identical, disregarding the precision of the system timestamp
     */
    private static boolean equalIgnoreSystemColumnTimestampPrecision(
            Schema existingSchema, Schema newSchema) {
        List<DataField> existingFields = new ArrayList<>(existingSchema.fields());
        DataField systemTimestampField = existingFields.get(existingFields.size() - 1);
        if (systemTimestampField.name().equals(TIMESTAMP_COLUMN_NAME)
                && systemTimestampField
                        .type()
                        .equalsIgnoreFieldId(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())) {
            existingFields.set(
                    existingFields.size() - 1,
                    new DataField(
                            systemTimestampField.id(),
                            systemTimestampField.name(),
                            DataTypes.TIMESTAMP_LTZ_MILLIS(),
                            systemTimestampField.description()));
        }
        existingSchema = existingSchema.copy(RowType.of(existingFields.toArray(new DataField[0])));
        return existingSchema.equals(newSchema);
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
