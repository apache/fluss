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
import org.apache.fluss.exception.InsufficientLakeSnapshotHistoryException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.FlussPrincipal;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * A catalog interface to modify metadata in external datalake.
 *
 * @since 0.7
 */
@PublicEvolving
public interface LakeCatalog extends AutoCloseable {

    /**
     * Gets the Fluss {@link TableDescriptor} mapped from the metadata of an existing lake table.
     *
     * @param tablePath path of the lake table
     * @return the Fluss table descriptor mapped from the lake table
     * @throws TableNotExistException if the table does not exist in the lake
     */
    default TableDescriptor getTableDescriptor(TablePath tablePath) throws TableNotExistException {
        throw new UnsupportedOperationException(
                "This lake catalog does not support mapping lake table metadata to a Fluss table descriptor.");
    }

    /**
     * Gets the latest snapshot ID that changed the logical data of a lake table.
     *
     * <p>Snapshots that only rewrite physical files or update statistics are ignored. The result
     * must distinguish a table known to have no data-change snapshot from one whose earlier
     * data-change snapshots may have expired. Return empty only when that absence is known; fail
     * when the available history cannot establish it.
     *
     * @param tablePath path of the lake table
     * @return the latest data-change snapshot ID, or empty if the table is known to have no
     *     data-change snapshot
     * @throws TableNotExistException if the lake table does not exist
     * @throws InsufficientLakeSnapshotHistoryException if the available snapshot history cannot
     *     establish whether a data-change snapshot exists
     * @throws UnsupportedOperationException if reading snapshots is not supported
     */
    default Optional<Long> getLatestDataChangeSnapshotId(TablePath tablePath)
            throws TableNotExistException {
        throw new UnsupportedOperationException(
                "Reading data-change snapshots is not supported by this lake catalog.");
    }

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
     * <p>A {@link TableChange.ModifyBucketCount} is Fluss's coordinator-orchestrated bucket count
     * rescale: implementations supporting rescale must apply it to their bucket layout option,
     * others should throw {@link UnsupportedOperationException}. User-facing changes to the
     * lake-native bucket option (e.g. Paimon {@code bucket}) keep being rejected to prevent the two
     * systems from diverging.
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

        /**
         * Get the current table info of fluss.
         *
         * @return the current table info of fluss. Null if the table does not exist.
         * @since 0.10
         */
        @Nullable
        TableDescriptor getCurrentTable();

        /**
         * Get the lake table path currently associated with the Fluss table.
         *
         * @return the current lake table path, or null when there is no current Fluss table
         */
        @Nullable
        default TablePath getCurrentLakeTablePath() {
            return null;
        }

        /**
         * Gets the lake snapshot ID currently registered by Fluss for the table.
         *
         * @return the registered snapshot ID, or empty if Fluss has not registered a snapshot
         */
        default Optional<Long> getCurrentLakeSnapshotId() {
            return Optional.empty();
        }

        /** Get the expected table info of fluss. */
        TableDescriptor getExpectedTable();
    }
}
