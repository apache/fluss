/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.exception.InvalidDatabaseException;
import com.alibaba.fluss.exception.InvalidTableException;

import java.io.Serializable;
import java.util.Objects;

/**
 * A database name and table name combo. A table path is unique in a Fluss cluster and can be used
 * to identify a table at one moment.
 *
 * <p>Note: table name can be modified by {@code RENAME TABLE} statement, so a table path may point
 * to different table at different time. Fluss assigns a unique tableId (i.e., {@link
 * TableInfo#getTableId()}) to each table when table is created. The tableId is used to identify a
 * table at any time.
 *
 * @since 0.1
 */
@PublicEvolving
public class TablePath implements Serializable {
    private static final long serialVersionUID = 1L;

    // database name and table name are used as local folder names in Fluss. The name of such
    // folders consists of "{database_name}/{table_name}-{table_id}/log-{bucket_id}/xxx.log". Since
    // a typical folder name can not be over 255 characters long, there will be a limitation on the
    // length of table names. Since the max length of table_id is 10 characters, the length of
    // table_name shouldn't exceed 244 characters. However, We set the max length of both table name
    // and database name to 200 to leave enough room for the future.
    private static final int MAX_NAME_LENGTH = 200;

    private final String databaseName;
    private final String tableName;

    // Cache hashCode as it is called in performance sensitive parts of the code (e.g.
    // RecordAccumulator.ready)
    private Integer hash;

    public TablePath(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static TablePath of(String db, String table) {
        return new TablePath(db, table);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * Returns true if the table path is valid. A table path is valid if both the database and table
     * names are valid.
     */
    public boolean isValid() {
        return detectInvalidName(databaseName) == null && detectInvalidName(tableName) == null;
    }

    /**
     * Validate the table path. A table path is valid if both the database and table names are
     * valid.
     *
     * @throws InvalidTableException if the table name is invalid.
     * @throws InvalidDatabaseException if the database name is invalid.
     */
    public void validate() throws InvalidTableException, InvalidDatabaseException {
        validateDatabaseName(databaseName);
        validateTableName(tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TablePath that = (TablePath) o;
        return Objects.equals(databaseName, that.databaseName)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        Integer h = this.hash;
        if (h == null) {
            int result = Objects.hash(databaseName, tableName);
            this.hash = result;
            return result;
        } else {
            return h;
        }
    }

    @Override
    public String toString() {
        return databaseName + "." + tableName;
    }

    // -------------------------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------------------------

    public static void validateDatabaseName(String databaseName) throws InvalidDatabaseException {
        String dbError = detectInvalidName(databaseName);
        if (dbError != null) {
            throw new InvalidDatabaseException(
                    "Database name " + databaseName + " is invalid: " + dbError);
        }
    }

    public static void validateTableName(String tableName) throws InvalidTableException {
        String tableError = detectInvalidName(tableName);
        if (tableError != null) {
            throw new InvalidTableException(
                    "Table name " + tableName + " is invalid: " + tableError);
        }
    }

    static String detectInvalidName(String identifier) {
        if (identifier == null) {
            return "null string is not allowed";
        }
        if (identifier.isEmpty()) {
            return "the empty string is not allowed";
        }
        if (".".equals(identifier)) {
            return "'.' is not allowed";
        }
        if ("..".equals(identifier)) {
            return "'..' is not allowed";
        }
        if (identifier.length() > MAX_NAME_LENGTH) {
            return "the length of '"
                    + identifier
                    + "' is longer than the max allowed length "
                    + MAX_NAME_LENGTH;
        }
        if (containsInvalidPattern(identifier)) {
            return "'"
                    + identifier
                    + "' contains one or more characters other than "
                    + "ASCII alphanumerics, '_' and '-'";
        }
        return null;
    }

    /** Valid characters for Fluss table names are the ASCII alphanumerics, '_', and '-'. */
    private static boolean containsInvalidPattern(String identifier) {
        for (int i = 0; i < identifier.length(); ++i) {
            char c = identifier.charAt(i);

            // We don't use Character.isLetterOrDigit(c) because it's slower
            boolean validChar =
                    (c >= 'a' && c <= 'z')
                            || (c >= '0' && c <= '9')
                            || (c >= 'A' && c <= 'Z')
                            || c == '_'
                            || c == '-';
            if (!validChar) {
                return true;
            }
        }
        return false;
    }
}
