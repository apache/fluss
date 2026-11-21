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

package org.apache.fluss.client.lookup;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.exception.InvalidBucketRoutingException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link Lookuper} that lookups by prefix key. A prefix key is a prefix subset
 * of the primary key.
 */
@ThreadSafe
class PrefixKeyLookuper extends AbstractLookuper implements Lookuper {

    /** Encode bucket key from prefix lookup key row. */
    private final KeyEncoder bucketKeyEncoder;

    /** Encode prefix key for prefix lookup (follows primary key encoding rules). */
    private final KeyEncoder prefixKeyEncoder;

    private final BucketingFunction bucketingFunction;
    private final int numBuckets;

    /**
     * a getter to extract partition from prefix lookup key row, null when it's not a partitioned.
     */
    private @Nullable final PartitionGetter partitionGetter;

    /**
     * The lookup columns after the bucket key that the server can't match, because the table's key
     * encoding doesn't support them in the prefix: getters in the lookup row and in the results.
     */
    private final InternalRow.FieldGetter[] clientFilterKeyGetters;

    private final InternalRow.FieldGetter[] clientFilterResultGetters;

    public PrefixKeyLookuper(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            List<String> lookupColumnNames) {
        super(tableInfo, metadataUpdater, lookupClient, schemaGetter);
        // sanity check
        List<String> prefixKeys = validateAndGetPrefixKeys(tableInfo, lookupColumnNames);
        this.numBuckets = tableInfo.getNumBuckets();
        // the row type of the input lookup row
        RowType lookupRowType = tableInfo.getRowType().project(lookupColumnNames);
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);

        List<String> bucketKeys = tableInfo.getBucketKeys();
        // only the compacted encoding encodes every primary key prefix as a byte prefix of the key;
        // otherwise (e.g. Paimon on legacy tables) the keys after the bucket key are
        // client-filtered
        boolean compactedKey =
                KeyEncoder.ofPrimaryKeyEncoder(
                                lookupRowType,
                                bucketKeys,
                                tableInfo.getTableConfig(),
                                tableInfo.isDefaultBucketKey())
                        instanceof CompactedKeyEncoder;
        List<String> clientFilterKeys =
                compactedKey
                        ? Collections.emptyList()
                        : prefixKeys.subList(bucketKeys.size(), prefixKeys.size());
        prefixKeys = prefixKeys.subList(0, prefixKeys.size() - clientFilterKeys.size());
        this.clientFilterKeyGetters = createFieldGetters(lookupRowType, clientFilterKeys);
        this.clientFilterResultGetters =
                createFieldGetters(tableInfo.getRowType(), clientFilterKeys);

        // Use ofPrimaryKeyEncoder which follows primary key encoding rules
        this.prefixKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        lookupRowType,
                        prefixKeys,
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());

        // the prefix key bytes can be reused for bucketing only if they encode just the bucket key
        this.bucketKeyEncoder =
                lakeFormat == null && prefixKeys.equals(bucketKeys)
                        ? this.prefixKeyEncoder
                        : KeyEncoder.ofBucketKeyEncoder(lookupRowType, bucketKeys, lakeFormat);

        this.bucketingFunction = BucketingFunction.of(lakeFormat);
        this.partitionGetter =
                tableInfo.isPartitioned()
                        ? new PartitionGetter(lookupRowType, tableInfo.getPartitionKeys())
                        : null;
    }

    /** Validates the lookup columns and returns them without the partition fields. */
    private List<String> validateAndGetPrefixKeys(TableInfo tableInfo, List<String> lookupColumns) {
        // verify is primary key table
        if (!tableInfo.hasPrimaryKey()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Log table %s doesn't support prefix lookup",
                            tableInfo.getTablePath()));
        }

        // verify the bucket keys are the prefix subset of physical primary keys
        List<String> physicalPrimaryKeys = tableInfo.getPhysicalPrimaryKeys();
        List<String> bucketKeys = tableInfo.getBucketKeys();
        for (int i = 0; i < bucketKeys.size(); i++) {
            if (!bucketKeys.get(i).equals(physicalPrimaryKeys.get(i))) {
                throw new IllegalArgumentException(
                        String.format(
                                "Can not perform prefix lookup on table '%s', "
                                        + "because the bucket keys %s is not a prefix subset of the "
                                        + "physical primary keys %s (excluded partition fields if present).",
                                tableInfo.getTablePath(), bucketKeys, physicalPrimaryKeys));
            }
        }

        // verify the lookup columns must contain all partition fields if this is partitioned table
        if (tableInfo.isPartitioned()) {
            List<String> partitionKeys = tableInfo.getPartitionKeys();
            Set<String> lookupColumnsSet = new HashSet<>(lookupColumns);
            if (!lookupColumnsSet.containsAll(partitionKeys)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Can not perform prefix lookup on table '%s', "
                                        + "because the lookup columns %s must contain all partition fields %s.",
                                tableInfo.getTablePath(), lookupColumns, partitionKeys));
            }
        }

        // verify the lookup columns are the bucket keys, optionally followed by the next primary
        // keys
        List<String> physicalLookupColumns = new ArrayList<>(lookupColumns);
        physicalLookupColumns.removeAll(tableInfo.getPartitionKeys());
        if (physicalLookupColumns.size() < bucketKeys.size()
                || physicalLookupColumns.size() > physicalPrimaryKeys.size()
                || !physicalPrimaryKeys
                        .subList(0, physicalLookupColumns.size())
                        .equals(physicalLookupColumns)) {
            // all primary keys would be a primary key lookup
            List<String> extensionKeys =
                    bucketKeys.size() < physicalPrimaryKeys.size() - 1
                            ? physicalPrimaryKeys.subList(
                                    bucketKeys.size(), physicalPrimaryKeys.size() - 1)
                            : Collections.emptyList();
            throw new IllegalArgumentException(
                    String.format(
                            "Can not perform prefix lookup on table '%s', "
                                    + "because the lookup columns %s must contain all bucket keys %s in order%s.",
                            tableInfo.getTablePath(),
                            lookupColumns,
                            bucketKeys,
                            extensionKeys.isEmpty()
                                    ? ""
                                    : String.format(
                                            ", optionally followed by the next physical primary keys %s in order",
                                            extensionKeys)));
        }

        if (physicalLookupColumns.size() == physicalPrimaryKeys.size()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can not perform prefix lookup on table '%s', "
                                    + "because the lookup columns %s equals the physical primary keys %s. "
                                    + "Please use primary key lookup (Lookuper without lookupBy) instead.",
                            tableInfo.getTablePath(), lookupColumns, physicalPrimaryKeys));
        }
        return physicalLookupColumns;
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow prefixKey) {
        CompletableFuture<LookupResult> result = lookupPrefix(prefixKey);
        if (clientFilterKeyGetters.length == 0) {
            return result;
        }
        // copied: the caller may reuse the row's buffers before the lookup completes
        Object[] expected = new Object[clientFilterKeyGetters.length];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = copyIfMutable(clientFilterKeyGetters[i].getFieldOrNull(prefixKey));
        }
        return result.thenApply(
                lookupResult -> {
                    List<InternalRow> rows = new ArrayList<>();
                    for (InternalRow row : lookupResult.getRowList()) {
                        if (matches(row, expected)) {
                            rows.add(row);
                        }
                    }
                    return new LookupResult(rows);
                });
    }

    private boolean matches(InternalRow row, Object[] expected) {
        for (int i = 0; i < expected.length; i++) {
            if (!Objects.deepEquals(
                    clientFilterResultGetters[i].getFieldOrNull(row), expected[i])) {
                return false;
            }
        }
        return true;
    }

    private CompletableFuture<LookupResult> lookupPrefix(InternalRow prefixKey) {
        byte[] prefixKeyBytes = prefixKeyEncoder.encodeKey(prefixKey);
        byte[] bucketKeyBytes =
                prefixKeyEncoder == bucketKeyEncoder
                        ? prefixKeyBytes
                        : bucketKeyEncoder.encodeKey(prefixKey);

        Long partitionId = null;
        int bucketCount = numBuckets;
        if (partitionGetter != null) {
            try {
                PartitionRoutingInfo routing =
                        resolvePartitionRouting(partitionGetter.getPartition(prefixKey));
                partitionId = routing.getPartitionId();
                bucketCount = routing.getBucketCount();
            } catch (PartitionNotExistException e) {
                return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
            } catch (InvalidBucketRoutingException e) {
                CompletableFuture<LookupResult> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                return failed;
            }
        }

        // Compute bucket ID after partition resolution — needs per-partition bucket count
        int bucketId = bucketingFunction.bucketing(bucketKeyBytes, bucketCount);

        CompletableFuture<LookupResult> lookupFuture = new CompletableFuture<>();
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        lookupClient
                .prefixLookup(tableInfo.getTablePath(), tableBucket, prefixKeyBytes, bucketCount)
                .whenComplete(
                        (result, error) -> {
                            if (error != null) {
                                lookupFuture.completeExceptionally(
                                        new RuntimeException(
                                                "Failed to perform prefix lookup for table: "
                                                        + tableInfo.getTablePath(),
                                                error));
                            } else {
                                handleLookupResponse(result, lookupFuture);
                            }
                        });
        return lookupFuture;
    }

    /** Copies the key values that can be backed by mutable memory: strings and binaries. */
    private static Object copyIfMutable(@Nullable Object value) {
        if (value instanceof BinaryString) {
            return ((BinaryString) value).copy();
        } else if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        return value;
    }

    private static InternalRow.FieldGetter[] createFieldGetters(
            RowType rowType, List<String> columns) {
        InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[columns.size()];
        for (int i = 0; i < getters.length; i++) {
            int index = rowType.getFieldIndex(columns.get(i));
            getters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(index), index);
        }
        return getters;
    }
}
