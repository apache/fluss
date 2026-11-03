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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Captured source boundaries, independent of an engine's task layout or transport format.
 *
 * <p>The context fixes source metadata and log offsets but provides no global transaction snapshot
 * or retention lease. Engines must preserve the captured schema, lake snapshot, partition scope and
 * log ranges when constructing distributed tasks. Credentials and query configuration are supplied
 * separately. The selected lake snapshot can include partitions absent from the log ranges.
 *
 * @since 1.1
 */
@PublicEvolving
public final class UnionReadContext {
    private final TablePath tablePath;
    private final long tableId;
    private final long tableModifiedTime;
    private final int schemaId;
    private final Schema schema;
    private final List<String> partitionKeys;
    private final List<String> bucketKeys;
    private final int numBuckets;
    private final Map<String, String> partitionFilter;
    @Nullable private final TablePath lakeTablePath;
    private final Optional<DataLakeFormat> lakeFormat;
    private final Optional<MergeEngineType> mergeEngine;
    private final OptionalLong lakeSnapshotId;
    private final List<LogRange> logRanges;

    UnionReadContext(
            TableInfo table,
            @Nullable LakeSnapshot snapshot,
            PartitionSpec scope,
            List<LogRange> ranges) {
        tablePath = table.getTablePath();
        tableId = table.getTableId();
        tableModifiedTime = table.getModifiedTime();
        schemaId = table.getSchemaId();
        schema = table.getSchema();
        partitionKeys = Collections.unmodifiableList(new ArrayList<>(table.getPartitionKeys()));
        bucketKeys = Collections.unmodifiableList(new ArrayList<>(table.getBucketKeys()));
        numBuckets = table.getNumBuckets();
        partitionFilter = Collections.unmodifiableMap(new HashMap<>(scope.getSpecMap()));
        boolean lakeEnabled = table.getTableConfig().isDataLakeEnabled();
        lakeTablePath = lakeEnabled ? table.getLakeTablePath() : null;
        lakeFormat = lakeEnabled ? table.getTableConfig().getDataLakeFormat() : Optional.empty();
        mergeEngine = table.getTableConfig().getMergeEngineType();
        lakeSnapshotId =
                snapshot == null ? OptionalLong.empty() : OptionalLong.of(snapshot.getSnapshotId());
        logRanges = Collections.unmodifiableList(new ArrayList<>(ranges));
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public long getTableId() {
        return tableId;
    }

    /** Source metadata modification time, not a query snapshot timestamp. */
    public long getTableModifiedTime() {
        return tableModifiedTime;
    }

    public int getSchemaId() {
        return schemaId;
    }

    /** Frozen, unprojected schema. */
    public Schema getSchema() {
        return schema;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public List<String> getBucketKeys() {
        return bucketKeys;
    }

    /** Table default; each log range carries its partition's actual bucket count. */
    public int getNumBuckets() {
        return numBuckets;
    }

    /** Exact scope for both lake and log inputs; empty means table-wide. */
    public Map<String, String> getPartitionFilter() {
        return partitionFilter;
    }

    public Optional<TablePath> getLakeTablePath() {
        return Optional.ofNullable(lakeTablePath);
    }

    public Optional<DataLakeFormat> getLakeFormat() {
        return lakeFormat;
    }

    public Optional<MergeEngineType> getMergeEngine() {
        return mergeEngine;
    }

    /** Readable lake snapshot, absent before initial tiering or for a non-lake table. */
    public OptionalLong getLakeSnapshotId() {
        return lakeSnapshotId;
    }

    /** Live log boundaries, not a complete inventory of lake partitions or engine tasks. */
    public List<LogRange> getLogRanges() {
        return logRanges;
    }

    /**
     * One physical bucket's half-open log interval. Offsets are comparable only within that bucket.
     * A missing lake seam means reading starts at the captured earliest retained offset; for PK
     * tables this covers only keys reconstructible from retained logs.
     */
    @PublicEvolving
    public static final class LogRange {
        private final TableBucket tableBucket;
        private final Map<String, String> partitionValues;
        private final int bucketCount;
        private final OptionalLong lakeSnapshotOffset;
        private final long earliestOffset;
        private final long stopOffset;

        LogRange(
                TableBucket bucket,
                Map<String, String> partitionValues,
                int bucketCount,
                @Nullable Long seam,
                long earliest,
                long stop) {
            this.tableBucket = bucket;
            this.partitionValues = Collections.unmodifiableMap(new HashMap<>(partitionValues));
            this.bucketCount = bucketCount;
            this.lakeSnapshotOffset = seam == null ? OptionalLong.empty() : OptionalLong.of(seam);
            this.earliestOffset = earliest;
            this.stopOffset = stop;
        }

        public TableBucket getTableBucket() {
            return tableBucket;
        }

        /**
         * Business partition values in Fluss metadata encoding; empty for an unpartitioned table.
         */
        public Map<String, String> getPartitionValues() {
            return partitionValues;
        }

        public int getBucketCount() {
            return bucketCount;
        }

        /** A present zero is a lake seam, not an absent baseline. */
        public OptionalLong getLakeSnapshotOffset() {
            return lakeSnapshotOffset;
        }

        public long getStartOffset() {
            return lakeSnapshotOffset.orElse(earliestOffset);
        }

        public long getStopOffset() {
            return stopOffset;
        }

        /** Retention observation, not a lease; execution must detect later data loss. */
        public long getEarliestOffset() {
            return earliestOffset;
        }
    }
}
