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
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
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
    private final RowType rowType;

    /**
     * The highest field ID in this schema, this can only increase during the life cycle of the
     * schema. Otherwise, the removed columns will influence it.
     */
    private final int highestFieldId;

    private Schema(List<Column> columns, @Nullable PrimaryKey primaryKey, int highestFieldId) {
        this.columns = normalizeColumns(columns, primaryKey);
        this.primaryKey = primaryKey;
        // pre-create the row type as it is the most frequently used part of the schema
        this.rowType =
                new RowType(
                        this.columns.stream()
                                .map(
                                        column ->
                                                new DataField(
                                                        column.getName(), column.getDataType()))
                                .collect(Collectors.toList()));
        this.highestFieldId = highestFieldId;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Optional<PrimaryKey> getPrimaryKey() {
        return Optional.ofNullable(primaryKey);
    }

    public RowType getRowType() {
        return rowType;
    }

    /** Returns the primary key indexes, if any, otherwise returns an empty array. */
    public int[] getPrimaryKeyIndexes() {
        final List<String> columns = getColumnNames();
        return getPrimaryKey()
                .map(pk -> pk.columnNames)
                .map(pkColumns -> pkColumns.stream().mapToInt(columns::indexOf).toArray())
                .orElseGet(() -> new int[0]);
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

    /** Returns the indexes of the fields in the schema. */
    public int[] getColumnIndexes(List<String> keyNames) {
        int[] keyIndexes = new int[keyNames.size()];
        for (int i = 0; i < keyNames.size(); i++) {
            keyIndexes[i] = rowType.getFieldIndex(keyNames.get(i));
        }
        return keyIndexes;
    }

    public int getHighestFieldId() {
        return highestFieldId;
    }

    @Override
    public String toString() {
        final List<Object> components = new ArrayList<>(columns);
        if (primaryKey != null) {
            components.add(primaryKey);
        }
        return components.stream()
                .map(Objects::toString)
                .collect(Collectors.joining(",", "(", ")"));
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
                && Objects.equals(primaryKey, schema.primaryKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, primaryKey);
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
        private AtomicInteger highestFieldId;

        private Builder() {
            columns = new ArrayList<>();
            highestFieldId = new AtomicInteger(-1);
        }

        /** Adopts all members from the given schema. */
        public Builder fromSchema(Schema schema) {
            columns.addAll(schema.columns);
            if (schema.primaryKey != null) {
                primaryKeyNamed(schema.primaryKey.constraintName, schema.primaryKey.columnNames);
            }
            this.highestFieldId = new AtomicInteger(schema.highestFieldId);
            return this;
        }

        public Builder highestFieldId(int highestFieldId) {
            this.highestFieldId = new AtomicInteger(highestFieldId);
            return this;
        }

        public Builder fromRowType(RowType rowType) {
            checkNotNull(rowType, "rowType must not be null.");
            final List<DataType> fieldDataTypes = rowType.getChildren();
            final List<String> fieldNames = rowType.getFieldNames();
            IntStream.range(0, fieldDataTypes.size())
                    .forEach(i -> column(fieldNames.get(i), fieldDataTypes.get(i)));
            return this;
        }

        /** Adopts the given field names and field data types as physical columns of the schema. */
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

        /** Adopts all columns from the given list. */
        public Builder fromColumns(List<Column> inputColumns) {
            boolean nonMatchColumnId =
                    inputColumns.stream()
                            .noneMatch(column -> column.columnId != Column.UNKNOWN_COLUMN_ID);
            boolean allMatchColumnId =
                    inputColumns.stream()
                            .allMatch(column -> column.columnId != Column.UNKNOWN_COLUMN_ID);
            checkState(
                    nonMatchColumnId || allMatchColumnId,
                    "All columns must have columnId or none of them must have columnId.");

            if (allMatchColumnId) {
                columns.addAll(inputColumns);
                highestFieldId =
                        new AtomicInteger(
                                inputColumns.stream()
                                        .mapToInt(Column::getColumnId)
                                        .max()
                                        .orElse(-1));
            } else {
                // if all columnId is not set, this maybe from old version schema. Just use its
                // position as columnId.
                inputColumns.forEach(
                        column ->
                                this.column(column.columnName, column.dataType)
                                        .withComment(column.comment));
            }

            return this;
        }

        /**
         * Declares a column that is appended to this schema.
         *
         * <p>Columns are regular columns known from databases. They define the names, the types,
         * and the order of fields in the data. Thus, columns represent the payload that is read
         * from and written to an external system.
         *
         * @param columnName column name
         */
        public Builder column(String columnName, DataType dataType) {
            checkNotNull(columnName, "Column name must not be null.");
            checkNotNull(dataType, "Data type must not be null.");
            columns.add(new Column(columnName, dataType, null, highestFieldId.incrementAndGet()));
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

        /** Returns an instance of an {@link Schema}. */
        public Schema build() {
            Integer maximumColumnId =
                    columns.stream().map(Column::getColumnId).max(Integer::compareTo).orElse(0);

            checkState(
                    columns.isEmpty() || highestFieldId.get() >= maximumColumnId,
                    "Highest field id must be greater than or equal to the maximum column id.");

            checkState(
                    columns.stream().map(Column::getColumnId).distinct().count() == columns.size(),
                    "Column ids must be unique.");
            return new Schema(columns, primaryKey, highestFieldId.get());
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

        public Column(String columnName, DataType dataType) {
            this(columnName, dataType, null, UNKNOWN_COLUMN_ID);
        }

        public Column(String columnName, DataType dataType, @Nullable String comment) {
            this(columnName, dataType, comment, UNKNOWN_COLUMN_ID);
        }

        public Column(
                String columnName, DataType dataType, @Nullable String comment, int columnId) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.comment = comment;
            this.columnId = columnId;
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

        public Column withComment(String comment) {
            return new Column(columnName, dataType, comment, columnId);
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
                    && Objects.equals(columnId, that.columnId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, dataType, comment, columnId);
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
            List<Column> columns, @Nullable PrimaryKey primaryKey) {

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

        // primary key should not nullable
        Set<String> pkSet = new HashSet<>(primaryKeyNames);
        List<Column> newColumns = new ArrayList<>();
        for (Column column : columns) {
            if (pkSet.contains(column.getName()) && column.getDataType().isNullable()) {
                newColumns.add(
                        new Column(
                                column.getName(),
                                column.getDataType().copy(false),
                                column.getComment().isPresent() ? column.getComment().get() : null,
                                column.getColumnId()));
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
}
