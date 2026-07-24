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

package org.apache.fluss.lake.iceberg;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.iceberg.utils.IcebergCatalogUtils;
import org.apache.fluss.lake.iceberg.utils.IcebergPartitionSpecUtils;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.lake.iceberg.IcebergSchemaUtils.SYSTEM_COLUMNS;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** An Iceberg implementation of {@link LakeCatalog}. */
public class IcebergLakeCatalog implements LakeCatalog {
    @VisibleForTesting
    static final Set<String> RESERVED_PROPERTIES =
            Set.of(
                    TableProperties.MERGE_MODE,
                    TableProperties.UPDATE_MODE,
                    TableProperties.DELETE_MODE);

    private final Catalog icebergCatalog;

    // for fluss config
    private static final String FLUSS_CONF_PREFIX = "fluss.";
    // for iceberg config
    private static final String ICEBERG_CONF_PREFIX = "iceberg.";

    public IcebergLakeCatalog(Configuration configuration) {
        this.icebergCatalog = IcebergCatalogUtils.createIcebergCatalog(configuration);
    }

    @VisibleForTesting
    protected Catalog getIcebergCatalog() {
        return icebergCatalog;
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor, Context context)
            throws TableAlreadyExistException {
        // validate at most one key field requirement
        List<String> keys = tableDescriptor.getBucketKeys();
        checkArgument(
                keys.size() <= 1,
                "Iceberg format supports at most one bucket key, but got: %s",
                keys);

        // convert Fluss table path to iceberg table
        boolean isPkTable = tableDescriptor.hasPrimaryKey();
        TableIdentifier icebergId = toIcebergTableIdentifier(tablePath);
        Schema icebergSchema = IcebergSchemaUtils.createIcebergSchema(tableDescriptor, isPkTable);
        Catalog.TableBuilder tableBuilder = icebergCatalog.buildTable(icebergId, icebergSchema);

        PartitionSpec partitionSpec =
                IcebergPartitionSpecUtils.createPartitionSpec(tableDescriptor, icebergSchema);
        SortOrder sortOrder = createSortOrder(icebergSchema);
        Map<String, String> expectedProperties = buildTableProperties(tableDescriptor, isPkTable);
        tableBuilder.withProperties(expectedProperties);
        tableBuilder.withPartitionSpec(partitionSpec);
        tableBuilder.withSortOrder(sortOrder);
        try {
            createTable(
                    tablePath,
                    tableBuilder,
                    icebergSchema,
                    partitionSpec,
                    sortOrder,
                    expectedProperties,
                    context.isCreatingFlussTable());
        } catch (NoSuchNamespaceException e) {
            createDatabase(tablePath.getDatabaseName());
            try {
                createTable(
                        tablePath,
                        tableBuilder,
                        icebergSchema,
                        partitionSpec,
                        sortOrder,
                        expectedProperties,
                        context.isCreatingFlussTable());
            } catch (NoSuchNamespaceException t) {
                // shouldn't happen in normal cases
                throw new RuntimeException(
                        String.format(
                                "Fail to create table %s in Iceberg, because "
                                        + "Namespace %s still doesn't exist although create namespace "
                                        + "successfully, please try again.",
                                tablePath, tablePath.getDatabaseName()));
            }
        }
    }

    @Override
    public void alterTable(TablePath tablePath, List<TableChange> tableChanges, Context context)
            throws TableNotExistException {
        try {
            Table table = icebergCatalog.loadTable(toIcebergTableIdentifier(tablePath));

            List<TableChange> schemaChanges = new ArrayList<>();
            List<TableChange> propertyChanges = new ArrayList<>();
            for (TableChange change : tableChanges) {
                if (change instanceof TableChange.SchemaChange) {
                    schemaChanges.add(change);
                } else {
                    propertyChanges.add(change);
                }
            }

            if (!schemaChanges.isEmpty()) {
                applySchemaChanges(table, schemaChanges, context);
            }

            if (!propertyChanges.isEmpty()) {
                applyPropertyChanges(table, propertyChanges);
            }
        } catch (NoSuchTableException e) {
            throw new TableNotExistException("Table " + tablePath + " does not exist.");
        }
    }

    private void applyPropertyChanges(Table table, List<TableChange> propertyChanges) {
        UpdateProperties updateProperties = table.updateProperties();
        for (TableChange tableChange : propertyChanges) {
            if (tableChange instanceof TableChange.SetOption) {
                TableChange.SetOption option = (TableChange.SetOption) tableChange;
                checkArgument(
                        !RESERVED_PROPERTIES.contains(option.getKey()),
                        "Cannot set table property '%s'",
                        option.getKey());
                updateProperties.set(
                        convertFlussPropertyKeyToIceberg(option.getKey()), option.getValue());
            } else if (tableChange instanceof TableChange.ResetOption) {
                TableChange.ResetOption option = (TableChange.ResetOption) tableChange;
                checkArgument(
                        !RESERVED_PROPERTIES.contains(option.getKey()),
                        "Cannot reset table property '%s'",
                        option.getKey());
                updateProperties.remove(convertFlussPropertyKeyToIceberg(option.getKey()));
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported table change: " + tableChange.getClass());
            }
        }
        updateProperties.commit();
    }

    private void applySchemaChanges(Table table, List<TableChange> schemaChanges, Context context) {
        Schema currentIcebergSchema = table.schema();

        // Check schema compatibility to handle crash recovery idempotency.
        boolean skipAddColumns;
        if (isIcebergSchemaCompatible(currentIcebergSchema, context.getCurrentTable())) {
            // Iceberg schema matches current Fluss schema, apply all changes.
            skipAddColumns = false;
        } else if (isIcebergSchemaCompatible(currentIcebergSchema, context.getExpectedTable())) {
            // Iceberg schema already matches expected (post-alter) schema,
            // skip AddColumn changes since they were already applied.
            skipAddColumns = true;
        } else {
            throw new InvalidAlterTableException(
                    String.format(
                            "Iceberg schema is not compatible with Fluss schema: "
                                    + "Iceberg schema: %s, Fluss schema: %s. "
                                    + "therefore you need to add the diff columns all at once, "
                                    + "rather than applying other table changes: %s.",
                            currentIcebergSchema,
                            context.getCurrentTable().getSchema(),
                            schemaChanges));
        }

        UpdateSchema updateSchema = table.updateSchema();
        String firstSystemColumnName = SYSTEM_COLUMNS.keySet().iterator().next();
        boolean hasChanges = false;

        for (TableChange tableChange : schemaChanges) {
            if (tableChange instanceof TableChange.AddColumn) {
                if (skipAddColumns) {
                    continue;
                }
                TableChange.AddColumn addColumn = (TableChange.AddColumn) tableChange;

                if (!(addColumn.getPosition() instanceof TableChange.Last)) {
                    throw new UnsupportedOperationException(
                            "Only support to add column at last for iceberg table.");
                }

                org.apache.fluss.types.DataType flussDataType = addColumn.getDataType();
                if (!flussDataType.isNullable()) {
                    throw new UnsupportedOperationException(
                            "Only support to add nullable column for iceberg table.");
                }

                Type icebergType = flussDataType.accept(new FlussDataTypeToIcebergDataType());
                updateSchema.addColumn(addColumn.getName(), icebergType, addColumn.getComment());
                updateSchema.moveBefore(addColumn.getName(), firstSystemColumnName);
                hasChanges = true;
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported table change: " + tableChange.getClass());
            }
        }

        if (hasChanges) {
            updateSchema.commit();
        }
    }

    /**
     * Checks whether the current Iceberg schema is compatible with the given Fluss table
     * descriptor. Compatibility means the user columns and system columns match in name, type, and
     * nullability (ignoring Iceberg-assigned field IDs).
     *
     * <p>Iceberg reassigns field IDs during table creation, so field IDs are ignored by this
     * comparison.
     */
    @VisibleForTesting
    boolean isIcebergSchemaCompatible(
            Schema icebergSchema, @Nullable TableDescriptor flussTableDescriptor) {
        if (flussTableDescriptor == null) {
            return false;
        }
        // Identifier fields don't affect the comparison.
        Schema expectedSchema = IcebergSchemaUtils.createIcebergSchema(flussTableDescriptor, false);
        return IcebergSchemaUtils.compatibleWith(icebergSchema, expectedSchema);
    }

    private TableIdentifier toIcebergTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private void createTable(
            TablePath tablePath,
            Catalog.TableBuilder tableBuilder,
            Schema newIcebergSchema,
            PartitionSpec expectedSpec,
            SortOrder expectedSortOrder,
            Map<String, String> expectedProperties,
            boolean isCreatingFlussTable) {
        try {
            tableBuilder.create();
        } catch (AlreadyExistsException e) {
            // Table already exists in Iceberg. Check schema compatibility for idempotency,
            TableIdentifier icebergId = toIcebergTableIdentifier(tablePath);
            Table existingTable = icebergCatalog.loadTable(icebergId);
            Schema existingSchema = existingTable.schema();
            if (!isIcebergSchemaCompatibleWithSchema(existingSchema, newIcebergSchema)) {
                throw new TableAlreadyExistException(
                        String.format(
                                "The table %s already exists in Iceberg catalog, but the table schema is not compatible. "
                                        + "Existing schema: %s, new schema: %s. "
                                        + "Please first drop the table in Iceberg catalog or use a new table name.",
                                tablePath, existingSchema, newIcebergSchema),
                        e);
            }

            if (!isIcebergPartitionSpecCompatible(
                    existingTable.spec(), existingSchema, expectedSpec, newIcebergSchema)) {
                throw new TableAlreadyExistException(
                        String.format(
                                "The table %s already exists in Iceberg catalog, but the partition spec is not compatible. "
                                        + "Existing spec: %s, new spec: %s. "
                                        + "Please first drop the table in Iceberg catalog or use a new table name.",
                                tablePath, existingTable.spec(), expectedSpec),
                        e);
            }

            if (!isIcebergSortOrderCompatible(
                    existingTable.sortOrder(),
                    existingSchema,
                    expectedSortOrder,
                    newIcebergSchema)) {
                throw new TableAlreadyExistException(
                        String.format(
                                "The table %s already exists in Iceberg catalog, but the sort order is not compatible. "
                                        + "Existing sort order: %s, new sort order: %s. "
                                        + "Please first drop the table in Iceberg catalog, or pre-create the "
                                        + "Iceberg table without a custom sort order, or with ASC(%s) set explicitly, "
                                        + "or use a new table name.",
                                tablePath,
                                existingTable.sortOrder(),
                                expectedSortOrder,
                                OFFSET_COLUMN_NAME),
                        e);
            }

            if (!isIcebergPropertiesCompatible(existingTable.properties(), expectedProperties)) {
                throw new TableAlreadyExistException(
                        String.format(
                                "The table %s already exists in Iceberg catalog, but the table properties are not compatible. "
                                        + "Existing properties: %s, new properties: %s. "
                                        + "Please first drop the table in Iceberg catalog or use a new table name.",
                                tablePath, existingTable.properties(), expectedProperties),
                        e);
            }

            if (isCreatingFlussTable && existingTable.currentSnapshot() != null) {
                throw new TableAlreadyExistException(
                        String.format(
                                "The table %s already exists in Iceberg catalog, and the table is not empty. "
                                        + "Please first drop the table in Iceberg catalog or use a new table name.",
                                tablePath),
                        e);
            }
        }
    }

    @VisibleForTesting
    boolean isIcebergSchemaCompatibleWithSchema(Schema existingSchema, Schema newSchema) {
        Schema normalizedExisting = TypeUtil.assignIncreasingFreshIds(existingSchema);
        Schema normalizedNew = TypeUtil.assignIncreasingFreshIds(newSchema);

        List<Types.NestedField> existingFields = normalizedExisting.columns();
        List<Types.NestedField> newFields = normalizedNew.columns();

        if (existingFields.size() != newFields.size()) {
            return false;
        }

        for (int i = 0; i < existingFields.size(); i++) {
            Types.NestedField existing = existingFields.get(i);
            Types.NestedField expected = newFields.get(i);
            if (!existing.name().equals(expected.name())
                    || !existing.type().equals(expected.type())
                    || existing.isOptional() != expected.isOptional()) {
                return false;
            }
        }

        return identifierColumnNames(existingSchema).equals(identifierColumnNames(newSchema));
    }

    private static Set<String> identifierColumnNames(Schema schema) {
        Set<String> names = new HashSet<>();
        for (Integer fieldId : schema.identifierFieldIds()) {
            String columnName = schema.findColumnName(fieldId);
            if (columnName != null) {
                names.add(columnName);
            }
        }

        return names;
    }

    /** Checks whether the existing Iceberg partition spec is compatible with the expected one. */
    @VisibleForTesting
    boolean isIcebergPartitionSpecCompatible(
            PartitionSpec existingSpec,
            Schema existingSchema,
            PartitionSpec expectedSpec,
            Schema expectedSchema) {
        if (existingSpec.fields().size() != expectedSpec.fields().size()) {
            return false;
        }

        for (int i = 0; i < existingSpec.fields().size(); i++) {
            PartitionField existing = existingSpec.fields().get(i);
            PartitionField expected = expectedSpec.fields().get(i);

            String existingSource = existingSchema.findColumnName(existing.sourceId());
            String expectedSource = expectedSchema.findColumnName(expected.sourceId());
            if (existingSource == null || !existingSource.equals(expectedSource)) {
                return false;
            }

            if (!existing.name().equals(expected.name())) {
                return false;
            }

            if (!existing.transform().equals(expected.transform())) {
                return false;
            }
        }

        return true;
    }

    @VisibleForTesting
    boolean isIcebergSortOrderCompatible(
            SortOrder existingOrder,
            Schema existingSchema,
            SortOrder expectedOrder,
            Schema expectedSchema) {
        if (existingOrder.fields().size() != expectedOrder.fields().size()) {
            return false;
        }

        for (int i = 0; i < existingOrder.fields().size(); i++) {
            SortField existing = existingOrder.fields().get(i);
            SortField expected = expectedOrder.fields().get(i);

            String existingSource = existingSchema.findColumnName(existing.sourceId());
            String expectedSource = expectedSchema.findColumnName(expected.sourceId());
            if (existingSource == null || !existingSource.equals(expectedSource)) {
                return false;
            }

            if (!existing.transform().equals(expected.transform())) {
                return false;
            }

            if (existing.direction() != expected.direction()) {
                return false;
            }

            if (existing.nullOrder() != expected.nullOrder()) {
                return false;
            }
        }

        return true;
    }

    @VisibleForTesting
    boolean isIcebergPropertiesCompatible(
            Map<String, String> existingProperties, Map<String, String> expectedProperties) {
        for (Map.Entry<String, String> entry : expectedProperties.entrySet()) {
            String actual = existingProperties.get(entry.getKey());
            if (actual == null || !actual.equals(entry.getValue())) {
                return false;
            }
        }

        return true;
    }

    private void setFlussPropertyToIceberg(
            String key, String value, Map<String, String> icebergProperties) {
        if (key.startsWith(ICEBERG_CONF_PREFIX)) {
            icebergProperties.put(key.substring(ICEBERG_CONF_PREFIX.length()), value);
        } else {
            icebergProperties.put(FLUSS_CONF_PREFIX + key, value);
        }
    }

    private static String convertFlussPropertyKeyToIceberg(String key) {
        if (key.startsWith(ICEBERG_CONF_PREFIX)) {
            return key.substring(ICEBERG_CONF_PREFIX.length());
        } else {
            return FLUSS_CONF_PREFIX + key;
        }
    }

    private void createDatabase(String databaseName) {
        Namespace namespace = Namespace.of(databaseName);
        if (icebergCatalog instanceof SupportsNamespaces) {
            SupportsNamespaces supportsNamespaces = (SupportsNamespaces) icebergCatalog;
            if (!supportsNamespaces.namespaceExists(namespace)) {
                supportsNamespaces.createNamespace(namespace);
            }
        } else {
            throw new UnsupportedOperationException(
                    "The underlying Iceberg catalog does not support namespace operations.");
        }
    }

    private SortOrder createSortOrder(Schema icebergSchema) {
        // Sort by __offset system column for deterministic ordering
        SortOrder.Builder builder = SortOrder.builderFor(icebergSchema);
        builder.asc(OFFSET_COLUMN_NAME);
        return builder.build();
    }

    private Map<String, String> buildTableProperties(
            TableDescriptor tableDescriptor, boolean isPkTable) {
        Map<String, String> icebergProperties = new HashMap<>();

        if (isPkTable) {
            // MOR table properties for streaming workloads
            icebergProperties.put(
                    TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
            icebergProperties.put(
                    TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
            icebergProperties.put(
                    TableProperties.MERGE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        }

        tableDescriptor
                .getProperties()
                .forEach((k, v) -> setFlussPropertyToIceberg(k, v, icebergProperties));
        tableDescriptor
                .getCustomProperties()
                .forEach((k, v) -> setFlussPropertyToIceberg(k, v, icebergProperties));

        return icebergProperties;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly((AutoCloseable) icebergCatalog, "fluss-iceberg-catalog");
    }
}
