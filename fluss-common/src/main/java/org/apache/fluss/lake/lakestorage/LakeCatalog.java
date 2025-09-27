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
     * @throws TableAlreadyExistException if the table already exists
     */
    void createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws TableAlreadyExistException;

    /**
     * Alter a table in lake.
     *
     * @param tablePath path of the table to be altered
     * @param tableChanges The changes to be applied to the table
     * @throws TableNotExistException if the table not exists
     */
    void alterTable(TablePath tablePath, List<TableChange> tableChanges)
            throws TableNotExistException;

    @Override
    default void close() throws Exception {
        // default do nothing
    }
}
