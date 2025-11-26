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

import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;

import java.util.Objects;

/** {@link TableChange} represents the modification of the Fluss Table. */
public interface TableChange {

    /**
     * A table change toadd the column with specified position.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; ADD &lt;column_definition&gt; &lt;column_position&gt;
     * </pre>
     *
     * @return a TableChange represents the modification.
     */
    static AddColumn addColumn(
            String columnName,
            DataType dataType,
            @Nullable String comment,
            ColumnPosition position) {
        return new AddColumn(columnName, dataType, comment, position);
    }

    /**
     * A table change to modify a column. The modification includes:
     *
     * <ul>
     *   <li>change column data type
     *   <li>reorder column position
     *   <li>modify column comment
     *   <li>rename column name
     *   <li>change the computed expression
     *   <li>change the metadata column expression
     * </ul>
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY &lt;column_definition&gt; COMMENT '&lt;column_comment&gt;' &lt;column_position&gt;
     * </pre>
     *
     * @param oldColumn the definition of the old column.
     * @param newColumn the definition of the new column.
     * @param columnPosition the new position of the column.
     * @return a TableChange represents the modification.
     */
    static ModifyColumn modifyColumn(
            String columnName,
            DataType dataType,
            @Nullable String comment,
            @Nullable ColumnPosition columnPosition) {
        return new ModifyColumn(columnName, dataType, comment, columnPosition);
    }

    /**
     * A table change to modify the column name.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; RENAME &lt;old_column_name&gt; TO &lt;new_column_name&gt;
     * </pre>
     *
     * @param oldColumn the definition of the old column.
     * @param newName the name of the new column.
     * @return a TableChange represents the modification.
     */
    static RenameColumn renameColumn(String oldColumnName, String newColumnName) {
        return new RenameColumn(oldColumnName, newColumnName);
    }

    /**
     * A table change to drop column.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; DROP COLUMN &lt;column_name&gt;
     * </pre>
     *
     * @param columnName the column to drop.
     * @return a TableChange represents the modification.
     */
    static DropColumn dropColumn(String columnName) {
        return new DropColumn(columnName);
    }

    static SetOption set(String key, String value) {
        return new SetOption(key, value);
    }

    static ResetOption reset(String key) {
        return new ResetOption(key);
    }

    /**
     * A table change to set the table option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; SET '&lt;key&gt;' = '&lt;value&gt;';
     * </pre>
     */
    class SetOption implements TableChange {

        private final String key;
        private final String value;

        private SetOption(String key, String value) {
            this.key = key;
            this.value = value;
        }

        /** Returns the Option key to set. */
        public String getKey() {
            return key;
        }

        /** Returns the Option value to set. */
        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SetOption)) {
                return false;
            }
            SetOption setOption = (SetOption) o;
            return Objects.equals(key, setOption.key) && Objects.equals(value, setOption.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "SetOption{" + "key='" + key + '\'' + ", value='" + value + '\'' + '}';
        }
    }

    /**
     * A table change to reset the table option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; RESET '&lt;key&gt;'
     * </pre>
     */
    class ResetOption implements TableChange {

        private final String key;

        public ResetOption(String key) {
            this.key = key;
        }

        /** Returns the Option key to reset. */
        public String getKey() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ResetOption)) {
                return false;
            }
            ResetOption that = (ResetOption) o;
            return Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }

        @Override
        public String toString() {
            return "ResetOption{" + "key='" + key + '\'' + '}';
        }
    }

    /** A table change to modify the table schema. */
    interface SchemaChange extends TableChange {}

    /**
     * A table change to add a column.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; ADD &lt;column_definition&gt; &lt;column_position&gt;
     * </pre>
     */
    class AddColumn implements SchemaChange {
        private final String name;
        private final DataType dataType;
        private final @Nullable String comment;

        private final ColumnPosition position;

        private AddColumn(
                String name, DataType dataType, @Nullable String comment, ColumnPosition position) {
            this.name = name;
            this.dataType = dataType;
            this.comment = comment;
            this.position = position;
        }

        public String getName() {
            return name;
        }

        public DataType getDataType() {
            return dataType;
        }

        @Nullable
        public String getComment() {
            return comment;
        }

        public ColumnPosition getPosition() {
            return position;
        }
    }

    /** A table change to drop a column. */
    class DropColumn implements SchemaChange {
        private final String name;

        private DropColumn(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    /** A table change to modify a column. */
    class ModifyColumn implements SchemaChange {
        private final String name;
        private final DataType dataType;
        private final @Nullable String comment;

        private final @Nullable ColumnPosition newPosition;

        private ModifyColumn(
                String name,
                DataType dataType,
                @Nullable String comment,
                @Nullable ColumnPosition newPosition) {
            this.name = name;
            this.dataType = dataType;
            this.comment = comment;
            this.newPosition = newPosition;
        }

        public String getName() {
            return name;
        }

        public DataType getDataType() {
            return dataType;
        }

        @Nullable
        public String getComment() {
            return comment;
        }

        @Nullable
        public ColumnPosition getNewPosition() {
            return newPosition;
        }
    }

    /** A table change to modify a column's name. */
    class RenameColumn implements SchemaChange {
        private final String oldColumnName;
        private final String newColumnName;

        private RenameColumn(String oldColumnName, String newColumnName) {
            this.oldColumnName = oldColumnName;
            this.newColumnName = newColumnName;
        }

        public String getOldColumnName() {
            return oldColumnName;
        }

        public String getNewColumnName() {
            return newColumnName;
        }
    }

    /** The position of the modified or added column. */
    interface ColumnPosition {
        /** Get the position to place the column at the first. */
        static ColumnPosition last() {
            return Last.INSTANCE;
        }

        /** Get the position to place the column at the first. */
        static ColumnPosition first() {
            return First.INSTANCE;
        }

        /** Get the position to place the column after the specified column. */
        static ColumnPosition after(String column) {
            return new After(column);
        }
    }

    /** Column position FIRST means the specified column should be the first column. */
    final class First implements ColumnPosition {
        private static final First INSTANCE = new First();

        private First() {}

        @Override
        public String toString() {
            return "FIRST";
        }
    }

    /** Column position Last means the specified column should be the last column. */
    final class Last implements ColumnPosition {
        private static final Last INSTANCE = new Last();

        private Last() {}

        @Override
        public String toString() {
            return "LAST";
        }
    }

    /** Column position AFTER means the specified column should be put after the given `column`. */
    final class After implements ColumnPosition {
        private final String columnName;

        private After(String columnName) {
            this.columnName = columnName;
        }

        public String columnName() {
            return columnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof After)) {
                return false;
            }
            After after = (After) o;
            return Objects.equals(columnName, after.columnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName);
        }

        @Override
        public String toString() {
            return String.format("AFTER %s", columnName);
        }
    }
}
