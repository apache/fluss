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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.lake.paimon.source.FlussRowAsPaimonRow;
import org.apache.fluss.lake.paimon.utils.PaimonDataTypeToFlussDataType;
import org.apache.fluss.lake.paimon.utils.PaimonPartitionBucket;
import org.apache.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.PartitionUtils;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.lake.paimon.PaimonLakeCatalog.SYSTEM_COLUMNS;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;

/**
 * Paimon implementation of {@link LakeTableLookuper}. Uses Paimon's {@link LocalTableQuery} API for
 * efficient point lookups against lake storage.
 *
 * <p>Resources (Catalog, FileStoreTable, LocalTableQuery) are lazily initialized on first lookup.
 * File refresh is snapshot-based: each (partition, bucket) tracks its last refreshed snapshot ID,
 * and files are refreshed only when a new snapshot is detected.
 */
public class PaimonLakeTableLookuper implements LakeTableLookuper {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonLakeTableLookuper.class);

    private final Configuration paimonConfig;
    private final TablePath tablePath;

    // Lazily initialized resources
    private Catalog catalog;
    private FileStoreTable table;
    private org.apache.paimon.disk.IOManager ioManager;
    private LocalTableQuery tableQuery;
    private int currentSchemaId = -1;
    private int numPrimaryKeys;
    private int numUserColumns;
    private CompactedRowEncoder rowEncoder;
    private org.apache.fluss.row.InternalRow.FieldGetter[] fieldGetters;
    private PaimonRowAsFlussRow reusableFlussRow;

    // Partition resolution cache: partition name -> Paimon BinaryRow
    private final Map<String, BinaryRow> resolvedPartitions = new HashMap<>();

    // Snapshot-based refresh tracking
    private final Map<PaimonPartitionBucket, Long> refreshedSnapshots = new HashMap<>();
    private final Map<PaimonPartitionBucket, List<DataFileMeta>> loadedFiles = new HashMap<>();

    // Scan result cache: snapshot ID -> per-PB file lists
    private long lastScannedSnapshotId = -1;
    private Map<PaimonPartitionBucket, List<DataFileMeta>> scannedFilesByPb;

    public PaimonLakeTableLookuper(Configuration paimonConfig, TablePath tablePath) {
        this.paimonConfig = paimonConfig;
        this.tablePath = tablePath;
    }

    @Nullable
    @Override
    public byte[] lookup(byte[] key, LookupContext context) throws Exception {
        ensureInitialized(context.getSchemaId());

        BinaryRow partition = resolvePartition(context.getPartitionSpec());
        int bucket = context.getBucketId();

        refreshBucketIfNeeded(partition, bucket);

        // Key bytes are in Paimon BinaryRow format
        BinaryRow keyRow = new BinaryRow(numPrimaryKeys);
        keyRow.pointTo(org.apache.paimon.memory.MemorySegment.wrap(key), 0, key.length);

        // Lookup in Paimon
        org.apache.paimon.data.InternalRow result = tableQuery.lookup(partition, bucket, keyRow);
        if (result == null) {
            return null;
        }

        // Convert Paimon row -> Fluss row -> encoded value bytes
        reusableFlussRow.replaceRow(result);
        return encodeValue(context.getSchemaId(), reusableFlussRow);
    }

    @Override
    public void close() throws Exception {
        closeResources();
    }

    // -------------------------------------------------------------------------
    //  Lazy initialization
    // -------------------------------------------------------------------------

    private void ensureInitialized(int schemaId) throws Exception {
        if (tableQuery != null && schemaId == currentSchemaId) {
            return;
        }

        // Schema changed or first initialization — (re)create all resources
        if (tableQuery != null) {
            LOG.info(
                    "Schema evolved from {} to {} for table {}, reinitializing.",
                    currentSchemaId,
                    schemaId,
                    tablePath);
            closeResources();
        }

        catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
        table = (FileStoreTable) catalog.getTable(toPaimon(tablePath));
        ioManager = org.apache.paimon.disk.IOManager.create(System.getProperty("java.io.tmpdir"));
        tableQuery = table.newLocalTableQuery().withIOManager(ioManager);
        currentSchemaId = schemaId;

        RowType fullRowType = table.rowType();
        numUserColumns = fullRowType.getFieldCount() - SYSTEM_COLUMNS.size();
        numPrimaryKeys = table.primaryKeys().size();

        // Build Fluss DataType array for user columns (needed by CompactedRowEncoder)
        org.apache.fluss.types.DataType[] flussTypes =
                new org.apache.fluss.types.DataType[numUserColumns];
        for (int i = 0; i < numUserColumns; i++) {
            flussTypes[i] =
                    PaimonDataTypeToFlussDataType.INSTANCE.toFlussType(fullRowType.getTypeAt(i));
        }

        rowEncoder = new CompactedRowEncoder(flussTypes);
        fieldGetters = new org.apache.fluss.row.InternalRow.FieldGetter[numUserColumns];
        for (int i = 0; i < numUserColumns; i++) {
            fieldGetters[i] = org.apache.fluss.row.InternalRow.createFieldGetter(flussTypes[i], i);
        }
        reusableFlussRow = new PaimonRowAsFlussRow();
    }

    private void closeResources() {
        IOUtils.closeQuietly(tableQuery, "paimon local table query");
        IOUtils.closeQuietly(ioManager, "paimon io manager");
        IOUtils.closeQuietly(catalog, "paimon catalog");
        tableQuery = null;
        ioManager = null;
        catalog = null;
        table = null;
        resolvedPartitions.clear();
        refreshedSnapshots.clear();
        loadedFiles.clear();
        lastScannedSnapshotId = -1;
        scannedFilesByPb = null;
    }

    // -------------------------------------------------------------------------
    //  Snapshot-based file refresh
    // -------------------------------------------------------------------------

    private void refreshBucketIfNeeded(BinaryRow partition, int bucket) throws IOException {
        Optional<org.apache.paimon.Snapshot> latestSnapshot = table.latestSnapshot();
        if (!latestSnapshot.isPresent()) {
            return; // no data yet
        }
        long latestSnapshotId = latestSnapshot.get().id();

        PaimonPartitionBucket pb = new PaimonPartitionBucket(partition, bucket);
        Long lastRefreshed = refreshedSnapshots.get(pb);
        if (lastRefreshed != null && lastRefreshed == latestSnapshotId) {
            return; // already up-to-date
        }

        // Get file list for this partition/bucket from the latest snapshot
        Map<PaimonPartitionBucket, List<DataFileMeta>> filesByPb =
                getScannedFiles(latestSnapshotId);
        List<DataFileMeta> newFiles =
                filesByPb.getOrDefault(pb, Collections.<DataFileMeta>emptyList());
        List<DataFileMeta> oldFiles =
                loadedFiles.getOrDefault(pb, Collections.<DataFileMeta>emptyList());

        tableQuery.refreshFiles(partition, bucket, oldFiles, newFiles);
        loadedFiles.put(pb, newFiles);
        refreshedSnapshots.put(pb, latestSnapshotId);
    }

    /**
     * Scans the table and caches file lists grouped by (partition, bucket). Re-scans only when the
     * snapshot has changed.
     */
    private Map<PaimonPartitionBucket, List<DataFileMeta>> getScannedFiles(long snapshotId) {
        if (snapshotId != lastScannedSnapshotId || scannedFilesByPb == null) {
            scannedFilesByPb = new HashMap<>();
            List<Split> splits = table.newReadBuilder().newScan().plan().splits();
            for (Split split : splits) {
                DataSplit dataSplit = (DataSplit) split;
                PaimonPartitionBucket pb =
                        new PaimonPartitionBucket(dataSplit.partition(), dataSplit.bucket());
                scannedFilesByPb
                        .computeIfAbsent(pb, k -> new ArrayList<>())
                        .addAll(dataSplit.dataFiles());
            }
            lastScannedSnapshotId = snapshotId;
        }
        return scannedFilesByPb;
    }

    // -------------------------------------------------------------------------
    //  Partition resolution
    // -------------------------------------------------------------------------

    private BinaryRow resolvePartition(@Nullable ResolvedPartitionSpec spec) {
        if (spec == null) {
            return BinaryRow.EMPTY_ROW;
        }

        String partitionName = spec.getPartitionName();
        BinaryRow cached = resolvedPartitions.get(partitionName);
        if (cached != null) {
            return cached;
        }

        BinaryRow partitionRow = buildPartitionRow(spec);
        resolvedPartitions.put(partitionName, partitionRow);
        return partitionRow;
    }

    /**
     * Builds a Paimon partition {@link BinaryRow} from the given spec. Follows the same approach as
     * {@code RecordWriter}: parse string values to typed Fluss objects via {@link
     * PartitionUtils#parseValueOfType}, wrap in {@link FlussRowAsPaimonRow} for Fluss→Paimon value
     * conversion, then extract partition fields into a {@link BinaryRow}.
     */
    private BinaryRow buildPartitionRow(ResolvedPartitionSpec spec) {
        List<String> partitionKeys = table.partitionKeys();
        RowType fullRowType = table.rowType();
        List<String> values = spec.getPartitionValues();

        // Parse string partition values to typed Fluss objects and populate a GenericRow
        GenericRow flussRow = new GenericRow(fullRowType.getFieldCount());
        for (int i = 0; i < partitionKeys.size(); i++) {
            int fieldIndex = fullRowType.getFieldIndex(partitionKeys.get(i));
            org.apache.fluss.types.DataType flussType =
                    PaimonDataTypeToFlussDataType.INSTANCE.toFlussType(
                            fullRowType.getTypeAt(fieldIndex));
            Object typedValue =
                    PartitionUtils.parseValueOfType(values.get(i), flussType.getTypeRoot());
            flussRow.setField(fieldIndex, typedValue);
        }

        // Wrap as Paimon InternalRow for automatic Fluss→Paimon value conversion
        FlussRowAsPaimonRow paimonRow = new FlussRowAsPaimonRow(flussRow, fullRowType);

        // Extract partition fields into a BinaryRow
        BinaryRow partitionRow = new BinaryRow(partitionKeys.size());
        BinaryRowWriter writer = new BinaryRowWriter(partitionRow);
        for (int i = 0; i < partitionKeys.size(); i++) {
            int fieldIndex = fullRowType.getFieldIndex(partitionKeys.get(i));
            DataType fieldType = fullRowType.getTypeAt(fieldIndex);
            writePartitionField(writer, i, paimonRow, fieldIndex, fieldType);
        }
        writer.complete();
        return partitionRow;
    }

    /** Reads a field from a Paimon InternalRow and writes it to the BinaryRowWriter. */
    private static void writePartitionField(
            BinaryRowWriter writer,
            int pos,
            org.apache.paimon.data.InternalRow row,
            int fieldIndex,
            DataType type) {
        if (row.isNullAt(fieldIndex)) {
            writer.setNullAt(pos);
            return;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                writer.writeString(pos, row.getString(fieldIndex));
                break;
            case BOOLEAN:
                writer.writeBoolean(pos, row.getBoolean(fieldIndex));
                break;
            case BINARY:
            case VARBINARY:
                writer.writeBinary(pos, row.getBinary(fieldIndex));
                break;
            case TINYINT:
                writer.writeByte(pos, row.getByte(fieldIndex));
                break;
            case SMALLINT:
                writer.writeShort(pos, row.getShort(fieldIndex));
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                writer.writeInt(pos, row.getInt(fieldIndex));
                break;
            case BIGINT:
                writer.writeLong(pos, row.getLong(fieldIndex));
                break;
            case FLOAT:
                writer.writeFloat(pos, row.getFloat(fieldIndex));
                break;
            case DOUBLE:
                writer.writeDouble(pos, row.getDouble(fieldIndex));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int ntzPrecision = ((org.apache.paimon.types.TimestampType) type).getPrecision();
                writer.writeTimestamp(
                        pos, row.getTimestamp(fieldIndex, ntzPrecision), ntzPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int ltzPrecision =
                        ((org.apache.paimon.types.LocalZonedTimestampType) type).getPrecision();
                writer.writeTimestamp(
                        pos, row.getTimestamp(fieldIndex, ltzPrecision), ltzPrecision);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported partition column type: " + type);
        }
    }

    // -------------------------------------------------------------------------
    //  Value encoding
    // -------------------------------------------------------------------------

    private byte[] encodeValue(int schemaId, org.apache.fluss.row.InternalRow flussRow) {
        rowEncoder.startNewRow();
        for (int i = 0; i < numUserColumns; i++) {
            rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(flussRow));
        }
        org.apache.fluss.row.compacted.CompactedRow compactedRow = rowEncoder.finishRow();
        return ValueEncoder.encodeValue((short) schemaId, compactedRow);
    }
}
