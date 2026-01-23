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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.annotation.PublicStable;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.ReassignFieldId;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.EncodingUtils;
import org.apache.fluss.utils.StringUtils;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.SchemaJsonSerde;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A schema represents the schema part of a {@code CREATE TABLE} DDL statement in SQL. It defines
 * columns of different kind, constraints.
 *
 * @since 0.1
 */
@PublicEvolving
public final class Schema implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Schema EMPTY = Schema.newBuilder().build();

    private final List<Column> columns;
    private final @Nullable PrimaryKey primaryKey;
    private final List<String> autoIncrementColumnNames;
    private final RowType rowType;

    /**
     * The highest field ID in this schema, this can only increase during the life cycle of the
     * schema. Otherwise, the removed columns will influence it.
     */
    private final int highestFieldId;

    private Schema(
            List<Column> columns,
            @Nullable PrimaryKey primaryKey,
            int highestFieldId,
            List<String> autoIncrementColumnNames) {
        this.columns =
                normalizeColumns(columns, primaryKey, autoIncrementColumnNames, highestFieldId);
        this.primaryKey = primaryKey;
        this.autoIncrementColumnNames = autoIncrementColumnNames;
        // pre-create the row type as it is the most frequently used part of the schema
        this.rowType =
                new RowType(
                        this.columns.stream()
                                .map(
                                        column ->
                                                new DataField(
                                                        column.getName(),
                                                        column.getDataType(),
                                                        column.columnId))
                                .collect(Collectors.toList()));
        this.highestFieldId = highestFieldId;
    }

    public List<Column> getColumns() {
        return columns;
    }

    /**
     * Gets a column by its name.
     *
     * @param columnName the column name
     * @return the column with the given name
     * @throws IllegalArgumentException if the column does not exist
     */
    public Column getColumn(String columnName) {
        for (Column column : columns) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        throw new IllegalArgumentException(
                String.format("Column %s does not exist in schema.", columnName));
    }

    public Optional<PrimaryKey> getPrimaryKey() {
        return Optional.ofNullable(primaryKey);
    }

    public List<String> getAutoIncrementColumnNames() {
        return autoIncrementColumnNames;
    }

    public RowType getRowType() {
        return rowType;
    }

    /**
     * Gets the aggregation function for a specific column.
     *
     * @param columnName the column name
     * @return the aggregation function, or empty if not configured
     */
    public Optional<AggFunction> getAggFunction(String columnName) {
        return columns.stream()
                .filter(col -> col.getName().equals(columnName))
                .findFirst()
                .flatMap(Column::getAggFunction);
    }

    /** Returns the primary key indexes, if any, otherwise returns an empty array. */
    public int[] getPrimaryKeyIndexes() {
        final List<String> columns = getColumnNames();
        return getPrimaryKey()
                .map(pk -> pk.columnNames)
                .map(pkColumns -> pkColumns.stream().mapToInt(columns::indexOf).toArray())
                .orElseGet(() -> new int[0]);
    }

    /** Returns the auto-increment columnIds, if any, otherwise returns an empty array. */
    public int[] getAutoIncrementColumnIds() {
        if (autoIncrementColumnNames.isEmpty()) {
            return new int[0];
        } else {
            return getColumns().stream()
                    .filter(column -> autoIncrementColumnNames.contains(column.getName()))
                    .mapToInt(Column::getColumnId)
                    .toArray();
        }
    }

    /** Returns the primary key column names, if any, otherwise returns an empty array. */
    public List<String> getPrimaryKeyColumnNames() {
        return getPrimaryKey().map(PrimaryKey::getColumnNames).orElse(Collections.emptyList());
    }

    /**
     * Serialize the schema to a JSON byte array.
     *
     * @see SchemaJsonSerde
     */
    public byte[] toJsonBytes() {
        return JsonSerdeUtils.writeValueAsBytes(this, SchemaJsonSerde.INSTANCE);
    }

    /**
     * Deserialize from JSON byte array to an instance of {@link Schema}.
     *
     * @see SchemaJsonSerde
     */
    public static Schema fromJsonBytes(byte[] json) {
        return JsonSerdeUtils.readValue(json, SchemaJsonSerde.INSTANCE);
    }

    /** Returns all column names. It does not distinguish between different kinds of columns. */
    public List<String> getColumnNames() {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    /** Returns all column ids for top-level columns, the nested field ids are not included. */
    public List<Integer> getColumnIds() {
        return columns.stream().map(Column::getColumnId).collect(Collectors.toList());
    }

    /** Returns the column names in given column indexes. */
    public List<String> getColumnNames(int[] columnIndexes) {
        List<String> columnNames = new ArrayList<>();
        for (int columnIndex : columnIndexes) {
            columnNames.add(columns.get(columnIndex).columnName);
        }
        return columnNames;
    }

    /** Returns the column name in given column index. */
    public String getColumnName(int columnIndex) {
        return columns.get(columnIndex).columnName;
    }

    /** Returns the indexes of the fields in the schema. */
    public int[] getColumnIndexes(List<String> keyNames) {
        int[] keyIndexes = new int[keyNames.size()];
        for (int i = 0; i < keyNames.size(); i++) {
            keyIndexes[i] = rowType.getFieldIndex(keyNames.get(i));
        }
        return keyIndexes;
    }

    /** Returns the highest field ID in this schema. */
    public int getHighestFieldId() {
        return highestFieldId;
    }

    @Override
    public String toString() {
        return "Schema{"
                + "columns="
                + columns
                + ", primaryKey="
                + primaryKey
                + ", autoIncrementColumnNames="
                + autoIncrementColumnNames
                + ", highestFieldId="
                + highestFieldId
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Schema schema = (Schema) o;
        return Objects.equals(columns, schema.columns)
                && Objects.equals(autoIncrementColumnNames, schema.autoIncrementColumnNames)
                && Objects.equals(primaryKey, schema.primaryKey)
                && highestFieldId == schema.highestFieldId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, primaryKey, autoIncrementColumnNames, highestFieldId);
    }

    // --------------------------------------------------------------------------------------------

    /** Builder for configuring and creating instances of {@link Schema}. */
    public static Schema.Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for constructing an immutable {@link Schema}.
     *
     * @since 0.1
     */
    @PublicStable
    public static final class Builder {
        private final List<Column> columns;
        private @Nullable PrimaryKey primaryKey;
        private final List<String> autoIncrementColumnNames;
        private AtomicInteger highestFieldId;

        private Builder() {
            columns = new ArrayList<>();
            autoIncrementColumnNames = new ArrayList<>();
            highestFieldId = new AtomicInteger(-1);
        }

        /** Adopts all members from the given schema. */
        public Builder fromSchema(Schema schema) {
            // Check that the builder is empty before adopting from an existing schema
            checkState(
                    columns.isEmpty() && autoIncrementColumnNames.isEmpty() && primaryKey == null,
                    "Schema.Builder#fromSchema should be the first API to be called on the builder.");

            // Adopt columns while preserving their original IDs
            this.fromColumns(schema.getColumns());

            // Sync the highest field ID counter to prevent ID conflicts
            this.highestFieldId.set(schema.getHighestFieldId());

            // Copy the metadata members
            this.autoIncrementColumnNames.addAll(schema.getAutoIncrementColumnNames());
            schema.getPrimaryKey().ifPresent(pk -> this.primaryKey = pk);

            return this;
        }

        /**
         * Adopts all columns from the given list.
         *
         * <p>This method directly uses the columns as-is, preserving their existing column IDs and
         * all nested field IDs within their data types (e.g., field IDs in {@link RowType}, {@link
         * ArrayType}, {@link MapType}). No field ID reassignment will occur.
         *
         * <p>This behavior is different from {@link #column(String, DataType)}, which automatically
         * assigns new column IDs and reassigns all nested field IDs to ensure global uniqueness.
         *
         * <p>Use this method when:
         *
         * <ul>
         *   <li>Loading existing schema from storage where IDs are already assigned
         *   <li>Preserving schema identity during schema evolution
         *   <li>Reconstructing schema from serialized format
         * </ul>
         *
         * <p>Note: All input columns must either have column IDs set or none of them should have
         * column IDs. Mixed states are not allowed.
         *
         * @param inputColumns the list of columns to adopt
         * @return this builder for fluent API
         * @throws IllegalStateException if columns have inconsistent column ID states (some set,
         *     some not set)
         */
        public Builder fromColumns(List<Column> inputColumns) {

            boolean nonSetColumnId =
                    inputColumns.stream()
                            .noneMatch(column -> column.columnId != Column.UNKNOWN_COLUMN_ID);
            boolean allSetColumnId =
                    inputColumns.stream()
                            .allMatch(column -> column.columnId != Column.UNKNOWN_COLUMN_ID);
            // REFINED CHECK:
            // Only throw if we are adopting a full schema (allSetColumnId)
            // AND the builder already has columns assigned.
            // We use !columns.isEmpty() as the primary signal of a "dirty" builder.
            if (allSetColumnId && !inputColumns.isEmpty() && !this.columns.isEmpty()) {
                throw new IllegalStateException(
                        "Schema.Builder#fromColumns (or fromSchema) should be the first API to be called on the builder when adopting columns with IDs.");
            }

            checkState(
                    nonSetColumnId || allSetColumnId,
                    "All columns must have columnId or none of them must have columnId.");
            if (allSetColumnId) {
                columns.addAll(inputColumns);
                List<Integer> allFieldIds = collectAllFieldIds(inputColumns);
                highestFieldId =
                        new AtomicInteger(allFieldIds.stream().max(Integer::compareTo).orElse(-1));
            } else {
                // if all columnId is not set, this maybe from old version schema. Just use its
                // position as columnId.
                for (Column column : inputColumns) {
                    int newColumnId = highestFieldId.incrementAndGet();
                    columns.add(
                            new Column(
                                    column.columnName,
                                    column.dataType,
                                    column.comment,
                                    newColumnId,
                                    column.aggFunction));
                }
            }

            return this;
        }

        public Builder highestFieldId(int highestFieldId) {
            this.highestFieldId = new AtomicInteger(highestFieldId);
            return this;
        }

        /**
         * Adopts the field names and data types from the given {@link RowType} as physical columns
         * of the schema.
         *
         * <p>This method internally calls {@link #column(String, DataType)} for each field, which
         * means: The original field IDs in the RowType will be ignored and replaced with new ones.
         * If you need to preserve existing field IDs, use {@link #fromColumns(List)} or {@link
         * #fromSchema(Schema)} instead.
         *
         * @param rowType the row type to adopt fields from
         * @return this builder for fluent API
         */
        public Builder fromRowType(RowType rowType) {
            checkNotNull(rowType, "rowType must not be null.");
            final List<DataType> fieldDataTypes = rowType.getChildren();
            final List<String> fieldNames = rowType.getFieldNames();
            IntStream.range(0, fieldDataTypes.size())
                    .forEach(i -> column(fieldNames.get(i), fieldDataTypes.get(i)));
            return this;
        }

        /**
         * Adopts the given field names and field data types as physical columns of the schema.
         *
         * <p>This method internally calls {@link #column(String, DataType)} for each field, which
         * means: The original field IDs in the RowType will be ignored and replaced with new ones.
         * If you need to preserve existing field IDs, use {@link #fromColumns(List)} or {@link
         * #fromSchema(Schema)} instead.
         *
         * @param fieldNames the list of field names
         * @param fieldDataTypes the list of field data types
         * @return this builder for fluent API
         */
        public Builder fromFields(
                List<String> fieldNames, List<? extends DataType> fieldDataTypes) {
            checkNotNull(fieldNames, "Field names must not be null.");
            checkNotNull(fieldDataTypes, "Field data types must not be null.");
            checkArgument(
                    fieldNames.size() == fieldDataTypes.size(),
                    "Field names and field data types must have the same length.");
            IntStream.range(0, fieldNames.size())
                    .forEach(i -> column(fieldNames.get(i), fieldDataTypes.get(i)));
            return this;
        }

        /**
         * Declares a column that is appended to this schema.
         *
         * <p>Columns are regular columns known from databases. They define the names, the types,
         * and the order of fields in the data. Thus, columns represent the payload that is read
         * from and written to an external system.
         *
         * <p>Note: If the data type contains nested types (e.g., {@link RowType}, {@link
         * ArrayType}, {@link MapType}), all nested field IDs will be automatically reassigned to
         * ensure global uniqueness. This is essential for schema evolution support. If you need to
         * preserve existing field IDs, use {@link #fromColumns(List)} or {@link
         * #fromSchema(Schema)} instead.
         *
         * @param columnName column name
         * @param dataType column data type
         * @return this builder for fluent API
         */
        public Builder column(String columnName, DataType dataType) {
            checkNotNull(columnName, "Column name must not be null.");
            checkNotNull(dataType, "Data type must not be null.");
            int id = highestFieldId.incrementAndGet();
            // Reassign field id especially for nested types.
            DataType reassignDataType = ReassignFieldId.reassign(dataType, highestFieldId);
            columns.add(new Column(columnName, reassignDataType, null, id, null));
            return this;
        }

        /**
         * Declares a column with aggregation function that is appended to this schema.
         *
         * <p>This method associates an aggregation function with a non-primary key column. It is
         * only applicable when the table uses aggregation merge engine.
         *
         * <p>If aggregation function is not specified for a non-primary key column, it defaults to
         * {@link AggFunctions#LAST_VALUE_IGNORE_NULLS}.
         *
         * @param columnName the name of the column
         * @param dataType the data type of the column
         * @param aggFunction the aggregation function to apply
         * @return the builder instance
         */
        public Builder column(String columnName, DataType dataType, AggFunction aggFunction) {
            checkNotNull(columnName, "Column name must not be null.");
            checkNotNull(dataType, "Data type must not be null.");
            checkNotNull(aggFunction, "Aggregation function must not be null.");

            int id = highestFieldId.incrementAndGet();
            // Reassign field id especially for nested types.
            DataType reassignDataType = ReassignFieldId.reassign(dataType, highestFieldId);
            columns.add(new Column(columnName, reassignDataType, null, id, aggFunction));
            return this;
        }

        /** Apply comment to the previous column. */
        public Builder withComment(@Nullable String comment) {
            if (!columns.isEmpty()) {
                columns.set(
                        columns.size() - 1, columns.get(columns.size() - 1).withComment(comment));
            } else {
                throw new IllegalArgumentException(
                        "Method 'withComment(...)' must be called after a column definition, "
                                + "but there is no preceding column defined.");
            }
            return this;
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable. Adding a
         * primary key will force the column(s) to be marked {@code NOT NULL}. A table can have at
         * most one primary key.
         *
         * <p>The primary key will be assigned a generated name in the format {@code PK_col1_col2}.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(String... columnNames) {
            checkNotNull(columnNames, "Primary key column names must not be null.");
            return primaryKey(Arrays.asList(columnNames));
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable. Adding a
         * primary key will force the column(s) to be marked {@code NOT NULL}. A table can have at
         * most one primary key.
         *
         * <p>The primary key will be assigned a generated name in the format {@code PK_col1_col2}.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(List<String> columnNames) {
            checkNotNull(columnNames, "Primary key column names must not be null.");
            final String generatedConstraintName =
                    columnNames.stream().collect(Collectors.joining("_", "PK_", ""));
            return primaryKeyNamed(generatedConstraintName, columnNames);
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable. Adding a
         * primary key will force the column(s) to be marked {@code NOT NULL}. A table can have at
         * most one primary key.
         *
         * @param constraintName name for the primary key, can be used to reference the constraint
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKeyNamed(String constraintName, List<String> columnNames) {
            checkState(primaryKey == null, "Multiple primary keys are not supported.");
            checkArgument(
                    columnNames != null && !columnNames.isEmpty(),
                    "Primary key constraint must be defined for at least a single column.");
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(constraintName),
                    "Primary key constraint name must not be empty.");
            primaryKey = new PrimaryKey(constraintName, columnNames);
            return this;
        }

        /**
         * Declares a column to be auto-incremented. With an auto-increment column in the table,
         * whenever a new row is inserted into the table, the new row will be assigned with the next
         * available value from the auto-increment sequence. A table can have at most one auto
         * increment column.
         *
         * @param columnName the auto increment column name
         */
        public Builder enableAutoIncrement(String columnName) {
            checkState(
                    autoIncrementColumnNames.isEmpty(),
                    "Multiple auto increment columns are not supported yet.");
            checkArgument(columnName != null, "Auto increment column name must not be null.");
            autoIncrementColumnNames.add(columnName);
            return this;
        }

        /** Returns the column with the given name, if it exists. */
        public Optional<Column> getColumn(String columnName) {
            return columns.stream()
                    .filter(column -> column.getName().equals(columnName))
                    .findFirst();
        }

        /** Returns an instance of an {@link Schema}. */
        public Schema build() {
            return new Schema(columns, primaryKey, highestFieldId.get(), autoIncrementColumnNames);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes for representing the schema
    // --------------------------------------------------------------------------------------------

    /**
     * column in a schema.
     *
     * @since 0.1
     */
    @PublicStable
    public static final class Column implements Serializable {
        public static final int UNKNOWN_COLUMN_ID = -1;
        private static final long serialVersionUID = 1L;
        private final int columnId;
        private final String columnName;
        private final DataType dataType;
        private final @Nullable String comment;
        private final @Nullable AggFunction aggFunction;

        public Column(String columnName, DataType dataType) {
            this(columnName, dataType, null, UNKNOWN_COLUMN_ID, null);
        }

        public Column(String columnName, DataType dataType, @Nullable String comment) {
            this(columnName, dataType, comment, UNKNOWN_COLUMN_ID, null);
        }

        public Column(
                String columnName, DataType dataType, @Nullable String comment, int columnId) {
            this(columnName, dataType, comment, columnId, null);
        }

        public Column(
                String columnName,
                DataType dataType,
                @Nullable String comment,
                int columnId,
                @Nullable AggFunction aggFunction) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.comment = comment;
            this.columnId = columnId;
            this.aggFunction = aggFunction;
        }

        public String getName() {
            return columnName;
        }

        public Optional<String> getComment() {
            return Optional.ofNullable(comment);
        }

        public int getColumnId() {
            return columnId;
        }

        public DataType getDataType() {
            return dataType;
        }

        /**
         * Gets the aggregation function for this column.
         *
         * @return the aggregation function, or empty if not configured
         */
        public Optional<AggFunction> getAggFunction() {
            return Optional.ofNullable(aggFunction);
        }

        public Column withComment(String comment) {
            return new Column(columnName, dataType, comment, columnId, aggFunction);
        }

        public Column withAggFunction(@Nullable AggFunction aggFunction) {
            return new Column(columnName, dataType, comment, columnId, aggFunction);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(columnName).append(" ").append(dataType.toString());
            getComment()
                    .ifPresent(
                            c -> {
                                sb.append(" COMMENT '");
                                sb.append(EncodingUtils.escapeSingleQuotes(c));
                                sb.append("'");
                            });
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Column that = (Column) o;
            return Objects.equals(columnName, that.columnName)
                    && Objects.equals(dataType, that.dataType)
                    && Objects.equals(comment, that.comment)
                    && Objects.equals(columnId, that.columnId)
                    && Objects.equals(aggFunction, that.aggFunction);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, dataType, comment, columnId, aggFunction);
        }
    }

    /**
     * Primary key in a schema.
     *
     * @since 0.1
     */
    @PublicStable
    public static final class PrimaryKey implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String constraintName;
        private final List<String> columnNames;

        public PrimaryKey(String constraintName, List<String> columnNames) {
            this.constraintName = constraintName;
            this.columnNames = columnNames;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        @Override
        public String toString() {
            return String.format(
                    "CONSTRAINT %s PRIMARY KEY (%s)",
                    constraintName, String.join(", ", columnNames));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PrimaryKey that = (PrimaryKey) o;
            return Objects.equals(columnNames, that.columnNames);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), columnNames);
        }
    }

    // ----------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------

    /** Normalize columns and primary key. */
    private static List<Column> normalizeColumns(
            List<Column> columns,
            @Nullable PrimaryKey primaryKey,
            List<String> autoIncrementColumnNames,
            int highestFieldId) {

        checkFieldIds(columns, highestFieldId);
        List<String> columnNames =
                columns.stream().map(Column::getName).collect(Collectors.toList());

        Set<String> duplicateColumns = duplicate(columnNames);
        checkState(
                duplicateColumns.isEmpty(),
                "Table column %s must not contain duplicate fields. Found: %s",
                columnNames,
                duplicateColumns);
        Set<String> allFields = new HashSet<>(columnNames);

        if (primaryKey == null) {
            checkState(
                    autoIncrementColumnNames.isEmpty(),
                    "Auto increment column can only be used in primary-key table.");
            return Collections.unmodifiableList(columns);
        }

        List<String> primaryKeyNames = primaryKey.getColumnNames();
        duplicateColumns = duplicate(primaryKeyNames);
        checkState(
                duplicateColumns.isEmpty(),
                "Primary key constraint %s must not contain duplicate columns. Found: %s",
                primaryKey,
                duplicateColumns);
        checkState(
                allFields.containsAll(primaryKeyNames),
                "Table column %s should include all primary key constraint %s",
                columnNames,
                primaryKeyNames);

        Set<String> pkSet = new HashSet<>(primaryKeyNames);
        for (String autoIncrementColumn : autoIncrementColumnNames) {
            checkState(
                    allFields.contains(autoIncrementColumn),
                    "Auto increment column %s does not exist in table columns %s.",
                    autoIncrementColumn,
                    columnNames);
            checkState(
                    !pkSet.contains(autoIncrementColumn),
                    "Auto increment column can not be used as the primary key.");
        }

        // Validate that aggregation functions are only set for non-primary key columns
        for (Column column : columns) {
            // check presentation first for better performance
            if (column.getAggFunction().isPresent() && pkSet.contains(column.getName())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot set aggregation function for primary key column '%s'. "
                                        + "Primary key columns automatically use 'primary-key' aggregation.",
                                column.getName()));
            }
        }

        List<Column> newColumns = new ArrayList<>();
        for (Column column : columns) {
            if (autoIncrementColumnNames.contains(column.getName())) {
                checkState(
                        column.getDataType().is(DataTypeRoot.INTEGER)
                                || column.getDataType().is(DataTypeRoot.BIGINT),
                        "The data type of auto increment column must be INT or BIGINT.");
            }

            // primary key and auto increment column should not nullable
            if (pkSet.contains(column.getName()) && column.getDataType().isNullable()) {
                newColumns.add(
                        new Column(
                                column.getName(),
                                column.getDataType().copy(false),
                                column.getComment().isPresent() ? column.getComment().get() : null,
                                column.getColumnId(),
                                column.getAggFunction().orElse(null)));
            } else {
                newColumns.add(column);
            }
        }

        return Collections.unmodifiableList(newColumns);
    }

    private static Set<String> duplicate(List<String> names) {
        return names.stream()
                .filter(name -> Collections.frequency(names, name) > 1)
                .collect(Collectors.toSet());
    }

    public static RowType getKeyRowType(Schema schema, int[] keyIndexes) {
        List<DataField> keyRowFields = new ArrayList<>(keyIndexes.length);
        List<DataField> rowFields = schema.getRowType().getFields();
        for (int index : keyIndexes) {
            keyRowFields.add(rowFields.get(index));
        }
        return new RowType(keyRowFields);
    }

    /**
     * Validates field IDs in the schema, including both top-level column IDs and nested field IDs.
     *
     * <p>This method performs the following checks:
     *
     * <ul>
     *   <li>Ensures all top-level column IDs are unique
     *   <li>Ensures all field IDs (including nested fields in ROW, ARRAY, MAP types) are globally
     *       unique
     *   <li>Verifies that the highest field ID is greater than or equal to all existing field IDs
     * </ul>
     *
     * @param columns the list of columns to validate
     * @param highestFieldId the highest field ID that should be greater than or equal to all field
     *     IDs
     * @throws IllegalStateException if any validation fails
     */
    private static void checkFieldIds(List<Column> columns, int highestFieldId) {

        // Collect all field IDs (including nested fields) for validation
        List<Integer> allFieldIds = collectAllFieldIds(columns);

        // Validate all field IDs (including nested fields) are unique
        long uniqueFieldIdsCount = allFieldIds.stream().distinct().count();
        checkState(
                uniqueFieldIdsCount == allFieldIds.size(),
                "All field IDs (including nested fields) must be unique. Found %s unique IDs but expected %s.",
                uniqueFieldIdsCount,
                allFieldIds.size());

        // Validate the highest field ID is greater than or equal to all field IDs
        Integer maximumFieldId = allFieldIds.stream().max(Integer::compareTo).orElse(-1);
        checkState(
                columns.isEmpty() || highestFieldId >= maximumFieldId,
                "Highest field ID (%s) must be greater than or equal to the maximum field ID (%s) including nested fields. Current columns is %s",
                highestFieldId,
                maximumFieldId,
                columns);
    }

    /**
     * Recursively collects all field IDs from a data type, including nested fields in ROW, ARRAY,
     * and MAP types.
     */
    private static List<Integer> collectAllFieldIds(List<Column> columns) {
        List<Integer> allFieldIds = new ArrayList<>();
        for (Column column : columns) {
            allFieldIds.add(column.getColumnId());
            collectAllFieldIds(column.getDataType(), allFieldIds);
        }
        return allFieldIds;
    }

    private static void collectAllFieldIds(DataType dataType, List<Integer> fieldIds) {
        if (dataType instanceof RowType) {
            RowType rowType = (RowType) dataType;
            for (DataField field : rowType.getFields()) {
                fieldIds.add(field.getFieldId());
                collectAllFieldIds(field.getType(), fieldIds);
            }
        } else if (dataType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) dataType;
            collectAllFieldIds(arrayType.getElementType(), fieldIds);
        } else if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            collectAllFieldIds(mapType.getKeyType(), fieldIds);
            collectAllFieldIds(mapType.getValueType(), fieldIds);
        }
    }
}
