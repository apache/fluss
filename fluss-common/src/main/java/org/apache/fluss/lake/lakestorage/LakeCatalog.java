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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.FlussPrincipal;

import java.util.List;

/**
 * A catalog interface to modify metadata in external datalake.
 *
 * @since 0.7
 */
@PublicEvolving
public interface LakeCatalog extends AutoCloseable {

    /**
     * Create a new table in lake.
     *
     * @param tablePath path of the table to be created
     * @param tableDescriptor The descriptor of the table to be created
     * @param context contextual information needed for create table
     * @throws TableAlreadyExistException if the table already exists
     */
    void createTable(TablePath tablePath, TableDescriptor tableDescriptor, Context context)
            throws TableAlreadyExistException;

    /**
     * Alter a table in lake.
     *
     * @param tablePath path of the table to be altered
     * @param tableChanges The changes to be applied to the table
     * @param context contextual information needed for alter table
     * @throws TableNotExistException if the table not exists
     */
    void alterTable(TablePath tablePath, List<TableChange> tableChanges, Context context)
            throws TableNotExistException;

    @Override
    default void close() throws Exception {
        // default do nothing
    }

    /**
     * Contextual information for lake catalog methods that modify metadata in an external data
     * lake. It can be used to:
     *
     * <ul>
     *   <li>Access the fluss principal currently accessing the catalog.
     * </ul>
     *
     * @since 0.9
     */
    @PublicEvolving
    interface Context {

        /**
         * Whether the current operation is creating a fluss table.
         *
         * @return true if the current operation is creating a fluss table
         */
        boolean isCreatingFlussTable();

        /** Get the fluss principal currently accessing the catalog. */
        FlussPrincipal getFlussPrincipal();
    }
}
