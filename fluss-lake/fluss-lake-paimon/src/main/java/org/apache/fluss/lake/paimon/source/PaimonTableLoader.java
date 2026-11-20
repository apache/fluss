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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import java.io.Serializable;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Loads Paimon tables and manages owned resources. */
interface PaimonTableLoader extends Serializable {

    PaimonTableHandle load() throws Exception;

    /** Closing the handle releases only resources created by the loader. */
    interface PaimonTableHandle extends AutoCloseable {

        FileStoreTable table();
    }

    final class CatalogTableLoader implements PaimonTableLoader {
        private static final long serialVersionUID = 1L;

        private final Configuration paimonConfig;
        private final TablePath tablePath;

        CatalogTableLoader(Configuration paimonConfig, TablePath tablePath) {
            this.paimonConfig = new Configuration(paimonConfig);
            this.tablePath = tablePath;
        }

        @Override
        public PaimonTableHandle load() throws Exception {
            Catalog catalog =
                    CatalogFactory.createCatalog(
                            CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
            try {
                FileStoreTable table = (FileStoreTable) catalog.getTable(toPaimon(tablePath));
                return new PaimonTableHandle() {
                    private boolean closed;

                    @Override
                    public FileStoreTable table() {
                        return table;
                    }

                    @Override
                    public void close() throws Exception {
                        if (!closed) {
                            closed = true;
                            catalog.close();
                        }
                    }
                };
            } catch (Exception | Error failure) {
                try {
                    catalog.close();
                } catch (Exception | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }
    }

    final class ProvidedTableLoader implements PaimonTableLoader {
        private static final long serialVersionUID = 1L;

        private final FileStoreTable table;

        ProvidedTableLoader(FileStoreTable table) {
            this.table = checkNotNull(table, "table must not be null.");
        }

        @Override
        public PaimonTableHandle load() {
            return new PaimonTableHandle() {
                @Override
                public FileStoreTable table() {
                    return table;
                }

                @Override
                public void close() {}
            };
        }
    }
}
