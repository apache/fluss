/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.spark.procedure;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.spark.SparkTable;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** A base class for procedure. */
public abstract class BaseProcedure implements Procedure {

    private final TableCatalog tableCatalog;

    protected BaseProcedure(TableCatalog tableCatalog) {
        this.tableCatalog = tableCatalog;
    }

    protected Identifier toIdentifier(String identifierAsString, String argName) {
        if (identifierAsString == null || identifierAsString.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot handle an empty identifier for argument " + argName);
        }

        SparkSession spark = SparkSession.active();
        String[] multipartIdentifier = identifierAsString.split("\\.");

        if (multipartIdentifier.length == 1) {
            String[] defaultNamespace = spark.sessionState().catalogManager().currentNamespace();
            return Identifier.of(defaultNamespace, multipartIdentifier[0]);
        } else if (multipartIdentifier.length == 2) {
            return Identifier.of(new String[] {multipartIdentifier[0]}, multipartIdentifier[1]);
        } else {
            throw new IllegalArgumentException(
                    "Invalid identifier format for argument "
                            + argName
                            + ": "
                            + identifierAsString);
        }
    }

    protected SparkTable loadSparkTable(Identifier ident) {
        try {
            Table table = tableCatalog.loadTable(ident);
            if (!(table instanceof SparkTable)) {
                throw new IllegalArgumentException(
                        ident + " is not a Fluss table: " + table.getClass().getName());
            }
            return (SparkTable) table;
        } catch (Exception e) {
            String errMsg =
                    String.format(
                            "Couldn't load table '%s' in catalog '%s'", ident, tableCatalog.name());
            throw new RuntimeException(errMsg, e);
        }
    }

    protected Admin getAdmin(SparkTable table) {
        if (table instanceof org.apache.fluss.spark.catalog.AbstractSparkTable) {
            return ((org.apache.fluss.spark.catalog.AbstractSparkTable) table).admin();
        }
        throw new IllegalArgumentException(
                "Table is not an AbstractSparkTable: " + table.getClass().getName());
    }

    protected InternalRow newInternalRow(Object... values) {
        return new GenericInternalRow(values);
    }

    protected TableCatalog tableCatalog() {
        return tableCatalog;
    }

    protected TablePath toTablePath(Identifier ident) {
        if (ident.namespace().length != 1) {
            throw new IllegalArgumentException("Only single namespace is supported");
        }
        return TablePath.of(ident.namespace()[0], ident.name());
    }

    protected abstract static class Builder<T extends BaseProcedure> implements ProcedureBuilder {
        private TableCatalog tableCatalog;

        @Override
        public Builder<T> withTableCatalog(TableCatalog newTableCatalog) {
            this.tableCatalog = newTableCatalog;
            return this;
        }

        @Override
        public T build() {
            return doBuild();
        }

        protected abstract T doBuild();

        protected TableCatalog tableCatalog() {
            return tableCatalog;
        }
    }
}
