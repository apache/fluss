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

package org.apache.fluss.server.zk.data.producer;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.server.zk.data.ZkData;

import java.util.List;
import java.util.Objects;

/**
 * Represents producer offset snapshot metadata stored in {@link ZkData.ProducerIdZNode}.
 *
 * <p>This class stores metadata about producer offset snapshots, including:
 *
 * <ul>
 *   <li>Expiration time for TTL management
 *   <li>List of table offset metadata, each containing table ID and path to offset file
 * </ul>
 *
 * <p>The actual offset data is stored in remote storage (e.g., OSS/S3) and referenced by the file
 * paths in {@link TableOffsetMetadata}.
 *
 * @see ProducerOffsetsJsonSerde for JSON serialization and deserialization
 */
public class ProducerOffsets {

    /** The expiration time in milliseconds since epoch for TTL management. */
    private final long expirationTime;

    /** List of table offset metadata, each containing table ID and offset file path. */
    private final List<TableOffsetMetadata> tableOffsets;

    /**
     * Creates a new ProducerOffsets.
     *
     * @param expirationTime the expiration time in milliseconds since epoch
     * @param tableOffsets list of table offset metadata
     */
    public ProducerOffsets(long expirationTime, List<TableOffsetMetadata> tableOffsets) {
        this.expirationTime = expirationTime;
        this.tableOffsets = tableOffsets;
    }

    /**
     * Returns the expiration time in milliseconds since epoch.
     *
     * @return the expiration time
     */
    public long getExpirationTime() {
        return expirationTime;
    }

    /**
     * Returns the list of table offset metadata.
     *
     * @return list of table offset metadata
     */
    public List<TableOffsetMetadata> getTableOffsets() {
        return tableOffsets;
    }

    /**
     * Checks if this snapshot has expired.
     *
     * @param currentTimeMs the current time in milliseconds since epoch
     * @return true if the snapshot has expired, false otherwise
     */
    public boolean isExpired(long currentTimeMs) {
        return currentTimeMs > expirationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProducerOffsets)) {
            return false;
        }
        ProducerOffsets that = (ProducerOffsets) o;
        return expirationTime == that.expirationTime
                && Objects.equals(tableOffsets, that.tableOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expirationTime, tableOffsets);
    }

    @Override
    public String toString() {
        return "ProducerOffsets{"
                + "expirationTime="
                + expirationTime
                + ", tableOffsets="
                + tableOffsets
                + '}';
    }

    /** Metadata for a single table's offset snapshot. */
    public static class TableOffsetMetadata {

        /** The table ID. */
        private final long tableId;

        /** The path to the offset file in remote storage. */
        private final FsPath offsetsPath;

        /**
         * Creates a new TableOffsetMetadata.
         *
         * @param tableId the table ID
         * @param offsetsPath the path to the offset file
         */
        public TableOffsetMetadata(long tableId, FsPath offsetsPath) {
            this.tableId = tableId;
            this.offsetsPath = offsetsPath;
        }

        /**
         * Returns the table ID.
         *
         * @return the table ID
         */
        public long getTableId() {
            return tableId;
        }

        /**
         * Returns the path to the offset file.
         *
         * @return the offset file path
         */
        public FsPath getOffsetsPath() {
            return offsetsPath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableOffsetMetadata)) {
                return false;
            }
            TableOffsetMetadata that = (TableOffsetMetadata) o;
            return tableId == that.tableId && Objects.equals(offsetsPath, that.offsetsPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, offsetsPath);
        }

        @Override
        public String toString() {
            return "TableOffsetMetadata{"
                    + "tableId="
                    + tableId
                    + ", offsetsPath="
                    + offsetsPath
                    + '}';
        }
    }
}
