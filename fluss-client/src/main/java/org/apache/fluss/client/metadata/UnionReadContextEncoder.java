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

import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Encodes boundaries captured by {@link UnionReadBoundaryPlanner}. */
final class UnionReadContextEncoder {

    private static final int CONTEXT_VERSION = 1;

    private UnionReadContextEncoder() {}

    /**
     * Encodes a table-wide boundary using the Rust lake client's v1 JSON contract.
     *
     * @param table resolved source metadata
     * @param readableSnapshot matching readable snapshot and seams, or null for a non-lake table or
     *     before its first snapshot; capture it before stopping offsets
     * @param partitions captured live partitions, empty for an unpartitioned table
     * @param earliestOffsets concrete earliest offsets for exactly the captured buckets
     * @param stoppingOffsets concrete exclusive end offsets for exactly the captured buckets
     * @return UTF-8 JSON bytes independent of subsequent changes to the inputs
     * @throws IllegalArgumentException if metadata, bucket coverage or offsets disagree
     */
    static byte[] encode(
            TableInfo table,
            @Nullable LakeSnapshot readableSnapshot,
            List<PartitionInfo> partitions,
            Map<TableBucket, Long> earliestOffsets,
            Map<TableBucket, Long> stoppingOffsets) {
        return encode(
                table,
                readableSnapshot,
                partitions,
                earliestOffsets,
                stoppingOffsets,
                new PartitionSpec(Collections.emptyMap()));
    }

    /**
     * Encodes a boundary restricted by an exact, possibly partial partition spec.
     *
     * <p>Apply the spec before querying offsets. Unselected partition entries are ignored; offset
     * maps must cover exactly the selected buckets. Readers must also apply the spec to lake-only
     * partitions in the pinned snapshot. Inputs must not be modified during this synchronous call.
     * Receivers must validate the transport version and contents and trust the coordinator.
     *
     * @param table resolved source metadata
     * @param readableSnapshot matching readable snapshot and seams, or null when unavailable
     * @param partitions captured live partitions
     * @param earliestOffsets earliest retained offsets for selected buckets
     * @param stoppingOffsets exclusive end offsets for selected buckets
     * @param partitionFilter business partition scope; an empty spec selects the whole table
     * @return credential-free UTF-8 JSON in the v1 context format
     * @throws IllegalArgumentException if the filter, metadata or offset coverage is invalid
     */
    static byte[] encode(
            TableInfo table,
            @Nullable LakeSnapshot readableSnapshot,
            List<PartitionInfo> partitions,
            Map<TableBucket, Long> earliestOffsets,
            Map<TableBucket, Long> stoppingOffsets,
            PartitionSpec partitionFilter) {
        checkNotNull(table, "table");
        checkNotNull(partitions, "partitions");
        checkNotNull(partitionFilter, "partitionFilter");
        checkArgument(
                table.getPartitionKeys().containsAll(partitionFilter.getSpecMap().keySet())
                        && !partitionFilter.getSpecMap().containsValue(null),
                "Partition filter must reference only partition keys with non-null encoded values.");
        checkNotNull(earliestOffsets, "earliestOffsets");
        checkNotNull(stoppingOffsets, "stoppingOffsets");
        Map<TableBucket, Long> seams =
                readableSnapshot == null
                        ? Collections.emptyMap()
                        : readableSnapshot.getTableBucketsOffset();
        for (Map.Entry<TableBucket, Long> seam : seams.entrySet()) {
            checkArgument(
                    seam.getKey().getTableId() == table.getTableId()
                            && seam.getValue() != null
                            && seam.getValue() >= 0,
                    "Snapshot contains an invalid bucket seam: %s",
                    seam.getKey());
        }

        boolean lakeEnabled = table.getTableConfig().isDataLakeEnabled();
        checkArgument(
                lakeEnabled || readableSnapshot == null,
                "A non-lake table cannot have a lake snapshot.");
        checkArgument(
                table.getTableId() >= 0
                        && table.getSchemaId() >= 0
                        && table.getModifiedTime() >= 0
                        && table.getNumBuckets() > 0,
                "Invalid table identity or bucket count.");
        checkArgument(
                readableSnapshot == null || readableSnapshot.getSnapshotId() >= 0,
                "Invalid lake snapshot ID.");
        return JsonSerdeUtils.writeValueAsBytes(
                table,
                (metadata, generator) -> {
                    writeTable(generator, metadata, readableSnapshot, partitionFilter);
                    generator.writeArrayFieldStart("log_ranges");
                    int rangeCount = 0;
                    if (table.isPartitioned()) {
                        List<PartitionInfo> ordered = new ArrayList<>();
                        for (PartitionInfo partition : partitions) {
                            if (partition
                                    .getResolvedPartitionSpec()
                                    .toPartitionSpec()
                                    .getSpecMap()
                                    .entrySet()
                                    .containsAll(partitionFilter.getSpecMap().entrySet())) {
                                ordered.add(partition);
                            }
                        }
                        ordered.sort(
                                Comparator.comparing(PartitionInfo::getPartitionName)
                                        .thenComparingLong(PartitionInfo::getPartitionId));
                        Set<Long> ids = new HashSet<>();
                        Set<List<String>> values = new HashSet<>();
                        for (PartitionInfo partition : ordered) {
                            ResolvedPartitionSpec spec = partition.getResolvedPartitionSpec();
                            checkArgument(
                                    partition.getPartitionId() >= 0
                                            && spec.getPartitionKeys()
                                                    .equals(table.getPartitionKeys())
                                            && ids.add(partition.getPartitionId())
                                            && values.add(spec.getPartitionValues()),
                                    "Duplicate or mismatched partition: %s",
                                    partition.getPartitionName());
                            rangeCount +=
                                    writeRanges(
                                            generator,
                                            table.getTableId(),
                                            partition.getPartitionId(),
                                            table.getPartitionKeys(),
                                            spec.getPartitionValues(),
                                            partition.getBucketCount(),
                                            seams,
                                            earliestOffsets,
                                            stoppingOffsets);
                        }
                    } else {
                        checkArgument(
                                partitions.isEmpty(), "Non-partitioned table has live partitions.");
                        rangeCount +=
                                writeRanges(
                                        generator,
                                        table.getTableId(),
                                        null,
                                        table.getPartitionKeys(),
                                        Collections.emptyList(),
                                        table.getNumBuckets(),
                                        seams,
                                        earliestOffsets,
                                        stoppingOffsets);
                    }
                    checkArgument(
                            rangeCount == earliestOffsets.size()
                                    && rangeCount == stoppingOffsets.size(),
                            "Offset maps must cover exactly the captured buckets.");
                    generator.writeEndArray();
                    generator.writeEndObject();
                });
    }

    private static void writeTable(
            JsonGenerator generator,
            TableInfo table,
            @Nullable LakeSnapshot snapshot,
            PartitionSpec partitionFilter)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField("context_version", CONTEXT_VERSION);
        writePath(generator, "table_path", table.getTablePath());
        generator.writeNumberField("table_id", table.getTableId());
        generator.writeNumberField("table_modified_time", table.getModifiedTime());
        generator.writeNumberField("schema_id", table.getSchemaId());
        generator.writeFieldName("schema");
        generator.writeRawValue(
                new String(table.getSchema().toJsonBytes(), StandardCharsets.UTF_8));
        writeStrings(generator, "partition_keys", table.getPartitionKeys());
        generator.writeObjectFieldStart("partition_filter");
        for (Map.Entry<String, String> entry :
                new TreeMap<>(partitionFilter.getSpecMap()).entrySet()) {
            generator.writeStringField(entry.getKey(), entry.getValue());
        }
        generator.writeEndObject();
        writeStrings(generator, "bucket_keys", table.getBucketKeys());
        generator.writeNumberField("num_buckets", table.getNumBuckets());
        generator.writeStringField(
                "merge_engine",
                table.getTableConfig()
                        .getMergeEngineType()
                        .map(engine -> engine.name().toLowerCase(Locale.ROOT))
                        .orElse(null));
        generator.writeFieldName("lake");
        if (!table.getTableConfig().isDataLakeEnabled()) {
            generator.writeNull();
        } else {
            generator.writeStartObject();
            writePath(generator, "table_path", table.getLakeTablePath());
            generator.writeStringField(
                    "format",
                    table.getTableConfig()
                            .getDataLakeFormat()
                            .orElseThrow(() -> new IllegalArgumentException("Missing lake format."))
                            .toString());
            generator.writeFieldName("snapshot_id");
            if (snapshot == null) {
                generator.writeNull();
            } else {
                generator.writeNumber(snapshot.getSnapshotId());
            }
            generator.writeEndObject();
        }
    }

    private static int writeRanges(
            JsonGenerator generator,
            long tableId,
            @Nullable Long partitionId,
            List<String> keys,
            List<String> values,
            int count,
            Map<TableBucket, Long> seams,
            Map<TableBucket, Long> earliest,
            Map<TableBucket, Long> stops)
            throws IOException {
        checkArgument(count > 0, "Invalid partition bucket count: %s", count);
        for (int bucketId = 0; bucketId < count; bucketId++) {
            TableBucket bucket = new TableBucket(tableId, partitionId, bucketId);
            Long begin = earliest.get(bucket);
            Long end = stops.get(bucket);
            checkArgument(begin != null && end != null, "Missing log offsets for %s", bucket);
            Long seam = seams.get(bucket);
            long start = seam == null ? begin : seam;
            checkArgument(
                    start >= 0 && begin >= 0 && end >= start && end >= begin,
                    "Invalid concrete log offsets for %s",
                    bucket);
            generator.writeStartObject();
            generator.writeObjectFieldStart("table_bucket");
            generator.writeNumberField("table_id", bucket.getTableId());
            generator.writeNumberField("bucket", bucket.getBucket());
            generator.writeFieldName("partition_id");
            if (bucket.getPartitionId() == null) {
                generator.writeNull();
            } else {
                generator.writeNumber(bucket.getPartitionId());
            }
            generator.writeEndObject();
            generator.writeObjectFieldStart("partition");
            generator.writeStringField("kind", "ordinary");
            generator.writeFieldName("identity");
            if (bucket.getPartitionId() == null) {
                generator.writeString("unpartitioned");
            } else {
                generator.writeStartObject();
                generator.writeArrayFieldStart("key_values");
                for (int i = 0; i < keys.size(); i++) {
                    generator.writeStartArray();
                    generator.writeString(keys.get(i));
                    generator.writeString(values.get(i));
                    generator.writeEndArray();
                }
                generator.writeEndArray();
                generator.writeEndObject();
            }
            generator.writeEndObject();
            generator.writeNumberField("bucket_count", count);
            generator.writeObjectFieldStart("baseline");
            generator.writeStringField("kind", seam == null ? "none" : "lake");
            generator.writeEndObject();
            generator.writeNumberField("start_offset", start);
            generator.writeNumberField("stop_offset", end);
            generator.writeNumberField("earliest_offset", begin);
            generator.writeEndObject();
        }
        return count;
    }

    private static void writePath(JsonGenerator generator, String field, TablePath path)
            throws IOException {
        generator.writeObjectFieldStart(field);
        generator.writeStringField("database", path.getDatabaseName());
        generator.writeStringField("table", path.getTableName());
        generator.writeEndObject();
    }

    private static void writeStrings(JsonGenerator generator, String field, List<String> values)
            throws IOException {
        generator.writeArrayFieldStart(field);
        for (String value : values) {
            generator.writeString(value);
        }
        generator.writeEndArray();
    }
}
