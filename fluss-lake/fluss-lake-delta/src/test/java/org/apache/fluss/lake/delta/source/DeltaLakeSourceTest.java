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

package org.apache.fluss.lake.delta.source;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.LakeCommitter.CommitContext;
import org.apache.fluss.lake.delta.DeltaConfig;
import org.apache.fluss.lake.delta.DeltaLakeCatalog;
import org.apache.fluss.lake.delta.DeltaLakeStorage;
import org.apache.fluss.lake.delta.testutils.DeltaTestUtils;
import org.apache.fluss.lake.delta.tiering.DeltaCommittable;
import org.apache.fluss.lake.delta.tiering.DeltaLakeTieringFactory;
import org.apache.fluss.lake.delta.tiering.DeltaWriteResult;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.DataGetters;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.PartitionUtils;

import io.delta.kernel.internal.deletionvectors.Base85Codec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for reading tiered Delta tables back with {@link DeltaLakeSource}. */
class DeltaLakeSourceTest {

    private static final int BUCKET_NUM = 2;
    private static final int ROWS_PER_BUCKET = 5;
    private static final List<String> PARTITIONS = Arrays.asList("p1", "p2");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final RowType ROW_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("p", DataTypes.STRING()),
                    DataTypes.FIELD("flag", DataTypes.BOOLEAN()),
                    DataTypes.FIELD("tiny", DataTypes.TINYINT()),
                    DataTypes.FIELD("small", DataTypes.SMALLINT()),
                    DataTypes.FIELD("big", DataTypes.BIGINT()),
                    DataTypes.FIELD("f", DataTypes.FLOAT()),
                    DataTypes.FIELD("d", DataTypes.DOUBLE()),
                    DataTypes.FIELD("amount", DataTypes.DECIMAL(10, 2)),
                    DataTypes.FIELD("dt", DataTypes.DATE()),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(6)),
                    DataTypes.FIELD("ntz", DataTypes.TIMESTAMP(6)),
                    DataTypes.FIELD("bin", DataTypes.BYTES()),
                    DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())),
                    DataTypes.FIELD("attrs", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                    DataTypes.FIELD(
                            "point",
                            DataTypes.ROW(
                                    DataTypes.FIELD("x", DataTypes.INT()),
                                    DataTypes.FIELD("y", DataTypes.STRING()))));

    private @TempDir File tempWarehouseDir;
    private Configuration configuration;
    private DeltaLakeTieringFactory tieringFactory;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString(DeltaConfig.WAREHOUSE, tempWarehouseDir.toString());
        tieringFactory = new DeltaLakeTieringFactory(configuration);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReadLakeSnapshot(boolean isPartitioned) throws Exception {
        TablePath tablePath = TablePath.of("fluss", isPartitioned ? "partitioned" : "log");
        TableInfo tableInfo = createTable(tablePath, tableDescriptor(isPartitioned));
        List<String> partitions = isPartitioned ? PARTITIONS : Collections.singletonList(null);

        List<GenericRow> firstRound = new ArrayList<>();
        assertThat(tier(tablePath, tableInfo, partitions, 1, firstRound)).isEqualTo(1L);
        List<GenericRow> secondRound = new ArrayList<>();
        assertThat(tier(tablePath, tableInfo, partitions, 2, secondRound)).isEqualTo(2L);

        LakeSource<DeltaSplit> source = createLakeSource(tablePath);
        List<DeltaSplit> splits = plan(source, 1L);
        // one file per bucket and partition
        assertThat(splits).hasSize(BUCKET_NUM * partitions.size());
        for (DeltaSplit split : splits) {
            assertThat(split.bucket()).isEqualTo(-1);
            if (isPartitioned) {
                assertThat(split.partition()).hasSize(1);
                assertThat(PARTITIONS).contains(split.partition().get(0));
            } else {
                assertThat(split.partition()).isEmpty();
            }
        }

        // the snapshot recorded by Fluss pins the version, later rounds are not read
        assertThat(read(source, 1L, ROW_TYPE))
                .containsExactlyInAnyOrderElementsOf(render(firstRound, ROW_TYPE));
        List<GenericRow> allRows = new ArrayList<>(firstRound);
        allRows.addAll(secondRound);
        assertThat(read(source, 2L, ROW_TYPE))
                .containsExactlyInAnyOrderElementsOf(render(allRows, ROW_TYPE));
    }

    @Test
    void testProjection() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "projected");
        TableInfo tableInfo = createTable(tablePath, tableDescriptor(true));
        List<GenericRow> rows = new ArrayList<>();
        tier(tablePath, tableInfo, PARTITIONS, 1, rows);

        // reordered, with the partition column and nested types in between
        int[] projection = {16, 2, 9, 0, 14};
        LakeSource<DeltaSplit> source = createLakeSource(tablePath);
        source.withProject(
                Arrays.stream(projection).mapToObj(i -> new int[] {i}).toArray(int[][]::new));

        RowType projectedType = ROW_TYPE.project(projection);
        List<String> expected =
                rows.stream()
                        .map(
                                row ->
                                        render(
                                                ProjectedRow.from(projection).replaceRow(row),
                                                projectedType))
                        .collect(Collectors.toList());
        assertThat(read(source, 1L, projectedType)).containsExactlyInAnyOrderElementsOf(expected);

        // a copy keeps the projection, and later push downs don't leak between the copies
        LakeSource<DeltaSplit> copy = source.copy();
        source.withProject(new int[][] {{0}});
        assertThat(read(copy, 1L, projectedType)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @ParameterizedTest
    @MethodSource("partitionKeyTypes")
    void testTypedPartitionValues(DataType type, Object value) throws Exception {
        TablePath tablePath =
                TablePath.of("fluss", "partition_" + type.getTypeRoot().name().toLowerCase());
        RowType rowType =
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("p", type));
        TableInfo tableInfo =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(Schema.newBuilder().fromRowType(rowType).build())
                                .partitionedBy("p")
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .build());
        String partitionName = PartitionUtils.convertValueOfType(value, type.getTypeRoot());
        GenericRow row = GenericRow.of(1, value);
        List<DeltaWriteResult> writeResults = new ArrayList<>();
        try (LakeWriter<DeltaWriteResult> writer =
                tieringFactory.createLakeWriter(
                        DeltaTestUtils.writerContext(tablePath, 0, partitionName, tableInfo))) {
            writer.write(new GenericRecord(0, 0, ChangeType.APPEND_ONLY, row));
            writeResults.add(writer.complete());
        }
        commit(tablePath, tableInfo, writeResults, "offsets-1");

        LakeSource<DeltaSplit> source = createLakeSource(tablePath);
        List<DeltaSplit> splits = plan(source, 1L);
        // union read matches the splits to the Fluss partitions by name
        assertThat(splits)
                .extracting(DeltaSplit::partition)
                .containsExactly(Collections.singletonList(partitionName));
        assertThat(read(source, 1L, rowType)).containsExactly(render(row, rowType));
    }

    private static Stream<Arguments> partitionKeyTypes() {
        return Stream.of(
                Arguments.of(DataTypes.STRING(), BinaryString.fromString("a b/c")),
                Arguments.of(DataTypes.BOOLEAN(), false),
                Arguments.of(DataTypes.TINYINT(), (byte) -7),
                Arguments.of(DataTypes.SMALLINT(), (short) 300),
                Arguments.of(DataTypes.INT(), 42),
                Arguments.of(DataTypes.BIGINT(), -1234567890123L),
                Arguments.of(DataTypes.FLOAT(), 1.5f),
                Arguments.of(DataTypes.DOUBLE(), -2.25d),
                Arguments.of(DataTypes.DATE(), 19737),
                Arguments.of(DataTypes.TIMESTAMP(6), TimestampNtz.fromMicros(1700000000123456L)),
                Arguments.of(
                        DataTypes.TIMESTAMP_LTZ(6),
                        TimestampLtz.fromEpochMicros(1700000000123456L)));
    }

    @Test
    void testDeletionVectorDropsRows() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "deletion_vector");
        TableInfo tableInfo =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(Schema.newBuilder().fromRowType(ROW_TYPE).build())
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                .build());
        List<GenericRow> rows = new ArrayList<>();
        tier(tablePath, tableInfo, Collections.singletonList(null), 1, rows);
        assertThat(rows).hasSize(ROWS_PER_BUCKET);

        // like a DELETE of another engine, which marks rows 0 and 2 of the file as deleted
        commitDeletionVector(DeltaConfig.tablePath(configuration, tablePath), 0, 2);

        List<GenericRow> remaining = new ArrayList<>(rows);
        remaining.remove(2);
        remaining.remove(0);
        assertThat(read(createLakeSource(tablePath), 2L, ROW_TYPE))
                .containsExactlyInAnyOrderElementsOf(render(remaining, ROW_TYPE));
    }

    @Test
    void testFiltersAreNotPushedDown() throws Exception {
        LakeSource<DeltaSplit> source = createLakeSource(TablePath.of("fluss", "filters"));
        Predicate predicate = new PredicateBuilder(ROW_TYPE).equal(0, 1);
        LakeSource.FilterPushDownResult result =
                source.withFilters(Collections.singletonList(predicate));
        assertThat(result.acceptedPredicates()).isEmpty();
        assertThat(result.remainingPredicates()).containsExactly(predicate);
    }

    @Test
    void testSplitSerializerRejectsUnknownVersion() {
        DeltaSplitSerializer serializer = new DeltaSplitSerializer();
        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported DeltaSplit serializer version");
    }

    // ------------------------------------------------------------------------------------------

    private LakeSource<DeltaSplit> createLakeSource(TablePath tablePath) {
        return new DeltaLakeStorage(configuration).createLakeSource(tablePath);
    }

    private static List<DeltaSplit> plan(LakeSource<DeltaSplit> source, long snapshotId)
            throws IOException {
        return source.createPlanner((LakeSource.PlannerContext) () -> snapshotId).plan();
    }

    /** Plans and reads every split after shipping it through the split serializer. */
    private static List<String> read(
            LakeSource<DeltaSplit> source, long snapshotId, RowType rowType) throws IOException {
        SimpleVersionedSerializer<DeltaSplit> serializer = source.getSplitSerializer();
        List<String> rows = new ArrayList<>();
        for (DeltaSplit split : plan(source, snapshotId)) {
            DeltaSplit shipped =
                    serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
            assertThat(shipped).isEqualTo(split);
            try (CloseableIterator<LogRecord> records =
                    source.createRecordReader((LakeSource.ReaderContext<DeltaSplit>) () -> shipped)
                            .read()) {
                while (records.hasNext()) {
                    LogRecord record = records.next();
                    assertThat(record.getChangeType()).isEqualTo(ChangeType.INSERT);
                    assertThat(record.logOffset()).isEqualTo(-1L);
                    rows.add(render(record.getRow(), rowType));
                }
            }
        }
        return rows;
    }

    /** Tiers one round of rows into every bucket and partition and returns the Delta version. */
    private long tier(
            TablePath tablePath,
            TableInfo tableInfo,
            List<String> partitions,
            int round,
            List<GenericRow> written)
            throws Exception {
        List<DeltaWriteResult> writeResults = new ArrayList<>();
        for (int bucket = 0; bucket < tableInfo.getNumBuckets(); bucket++) {
            for (String partition : partitions) {
                try (LakeWriter<DeltaWriteResult> writer =
                        tieringFactory.createLakeWriter(
                                DeltaTestUtils.writerContext(
                                        tablePath, bucket, partition, tableInfo))) {
                    for (int i = 0; i < ROWS_PER_BUCKET; i++) {
                        GenericRow row = genRow(round, bucket, partition, i);
                        writer.write(new GenericRecord(i, i, ChangeType.APPEND_ONLY, row));
                        written.add(row);
                    }
                    writeResults.add(writer.complete());
                }
            }
        }
        return commit(tablePath, tableInfo, writeResults, "offsets-" + round);
    }

    private long commit(
            TablePath tablePath,
            TableInfo tableInfo,
            List<DeltaWriteResult> writeResults,
            String offsets)
            throws Exception {
        try (LakeCommitter<DeltaWriteResult, DeltaCommittable> committer =
                tieringFactory.createLakeCommitter(
                        DeltaTestUtils.committerContext(tablePath, tableInfo))) {
            return committer
                    .commit(
                            committer.toCommittable(writeResults),
                            new CommitContext(
                                    Collections.singletonMap(
                                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, offsets),
                                    Collections.emptyMap()))
                    .getCommittedSnapshotId();
        }
    }

    private TableInfo createTable(TablePath tablePath, TableDescriptor descriptor)
            throws Exception {
        new DeltaLakeCatalog(configuration)
                .createTable(tablePath, descriptor, new TestingLakeCatalogContext());
        return TableInfo.of(tablePath, 0, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    private static TableDescriptor tableDescriptor(boolean isPartitioned) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().fromRowType(ROW_TYPE).build())
                        .distributedBy(BUCKET_NUM)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true);
        if (isPartitioned) {
            builder.partitionedBy("p");
        }
        return builder.build();
    }

    private static GenericRow genRow(int round, int bucket, @Nullable String partition, int i) {
        int id = round * 1000 + bucket * 100 + i;
        Map<BinaryString, Integer> attrs = new HashMap<>();
        attrs.put(BinaryString.fromString("k" + i), i);
        return GenericRow.of(
                id,
                // every other row has nulls, also inside the nested values
                i % 2 == 0 ? null : BinaryString.fromString("name-" + id),
                BinaryString.fromString(partition == null ? "none" : partition),
                i % 2 == 0,
                (byte) i,
                (short) (i * 10),
                id * 1_000_000_000L,
                i + 0.5f,
                i - 0.25d,
                Decimal.fromUnscaledLong(id * 100L + 1, 10, 2),
                19000 + i,
                TimestampLtz.fromEpochMicros(1700000000000000L + id),
                TimestampNtz.fromMicros(1600000000000000L + id),
                new byte[] {(byte) i, (byte) -i},
                new GenericArray(
                        new Object[] {
                            BinaryString.fromString("t" + i),
                            i % 2 == 0 ? null : BinaryString.fromString("u")
                        }),
                new GenericMap(attrs),
                GenericRow.of(i, i % 2 == 0 ? null : BinaryString.fromString("y" + i)));
    }

    /**
     * Commits a protocol that enables deletion vectors, and replaces the only data file with itself
     * plus an inline deletion vector of the given row indexes. Delta Kernel can't write deletion
     * vectors, Spark writes the same actions for a DELETE.
     */
    private static void commitDeletionVector(String deltaPath, int... deletedRows)
            throws IOException {
        Path logDir = Paths.get(deltaPath, "_delta_log");
        ObjectNode protocol = null;
        ObjectNode add = null;
        for (String file :
                new String[] {"00000000000000000000.json", "00000000000000000001.json"}) {
            for (String line : Files.readAllLines(logDir.resolve(file))) {
                JsonNode action = OBJECT_MAPPER.readTree(line);
                if (action.has("protocol")) {
                    protocol = (ObjectNode) action.get("protocol");
                } else if (action.has("add")) {
                    add = (ObjectNode) action.get("add");
                }
            }
        }
        assertThat(protocol).isNotNull();
        assertThat(add).isNotNull();

        protocol.put("minReaderVersion", 3);
        protocol.put("minWriterVersion", 7);
        protocol.putArray("readerFeatures").add("deletionVectors");
        ArrayNode writerFeatures =
                protocol.has("writerFeatures")
                        ? (ArrayNode) protocol.get("writerFeatures")
                        : protocol.putArray("writerFeatures");
        writerFeatures.add("deletionVectors");

        ObjectNode remove = OBJECT_MAPPER.createObjectNode();
        remove.put("path", add.get("path").asText());
        remove.put("deletionTimestamp", System.currentTimeMillis());
        remove.put("dataChange", true);
        remove.put("extendedFileMetadata", true);
        remove.set("partitionValues", add.get("partitionValues"));
        remove.put("size", add.get("size").asLong());

        byte[] bitmap = serializeDeletionVector(deletedRows);
        ObjectNode deletionVector = add.putObject("deletionVector");
        deletionVector.put("storageType", "i");
        deletionVector.put("pathOrInlineDv", Base85Codec.encodeBytes(bitmap));
        deletionVector.put("sizeInBytes", bitmap.length);
        deletionVector.put("cardinality", deletedRows.length);

        List<String> actions = new ArrayList<>();
        for (Map.Entry<String, ObjectNode> action :
                Arrays.asList(
                        entry("protocol", protocol), entry("remove", remove), entry("add", add))) {
            ObjectNode node = OBJECT_MAPPER.createObjectNode();
            node.set(action.getKey(), action.getValue());
            actions.add(node.toString());
        }
        Files.write(
                logDir.resolve("00000000000000000002.json"),
                actions,
                StandardOpenOption.CREATE_NEW);
    }

    /**
     * Serializes row indexes in the portable format of the Delta RoaringBitmapArray: a magic
     * number, the number of 32-bit bitmaps, and per bitmap its key followed by the portable
     * RoaringBitmap.
     */
    private static byte[] serializeDeletionVector(int... rows) {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(rows);
        ByteBuffer buffer =
                ByteBuffer.allocate(4 + 8 + 4 + bitmap.serializedSizeInBytes())
                        .order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(1681511377);
        buffer.putLong(1);
        buffer.putInt(0);
        bitmap.serialize(buffer);
        return buffer.array();
    }

    private static Map.Entry<String, ObjectNode> entry(String key, ObjectNode value) {
        return new java.util.AbstractMap.SimpleEntry<>(key, value);
    }

    // ------------------------------------------------------------------------------------------
    // rendering, so that rows of any InternalRow implementation compare by value

    private static List<String> render(List<GenericRow> rows, RowType rowType) {
        return rows.stream().map(row -> render(row, rowType)).collect(Collectors.toList());
    }

    private static String render(InternalRow row, RowType rowType) {
        StringJoiner joiner = new StringJoiner(", ", "(", ")");
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            joiner.add(render(row, i, rowType.getTypeAt(i)));
        }
        return joiner.toString();
    }

    private static String render(DataGetters getters, int pos, DataType type) {
        if (getters.isNullAt(pos)) {
            return "null";
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
                return getters.getString(pos).toString();
            case BOOLEAN:
                return String.valueOf(getters.getBoolean(pos));
            case TINYINT:
                return String.valueOf(getters.getByte(pos));
            case SMALLINT:
                return String.valueOf(getters.getShort(pos));
            case INTEGER:
            case DATE:
                return String.valueOf(getters.getInt(pos));
            case BIGINT:
                return String.valueOf(getters.getLong(pos));
            case FLOAT:
                return String.valueOf(getters.getFloat(pos));
            case DOUBLE:
                return String.valueOf(getters.getDouble(pos));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return getters.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale())
                        .toString();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return String.valueOf(getters.getTimestampLtz(pos, 6).toEpochMicros());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return getters.getTimestampNtz(pos, 6).toString();
            case BINARY:
            case BYTES:
                return Arrays.toString(getters.getBytes(pos));
            case ARRAY:
                InternalArray array = getters.getArray(pos);
                StringJoiner elements = new StringJoiner(", ", "[", "]");
                for (int i = 0; i < array.size(); i++) {
                    elements.add(render(array, i, ((ArrayType) type).getElementType()));
                }
                return elements.toString();
            case MAP:
                InternalMap map = getters.getMap(pos);
                MapType mapType = (MapType) type;
                StringJoiner entries = new StringJoiner(", ", "{", "}");
                for (int i = 0; i < map.size(); i++) {
                    entries.add(
                            render(map.keyArray(), i, mapType.getKeyType())
                                    + "="
                                    + render(map.valueArray(), i, mapType.getValueType()));
                }
                return entries.toString();
            case ROW:
                RowType nested = (RowType) type;
                return render(getters.getRow(pos, nested.getFieldCount()), nested);
            default:
                throw new UnsupportedOperationException("Unsupported type " + type);
        }
    }
}
