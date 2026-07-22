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

package org.apache.fluss.flink.source;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.source.lookup.FlussLookupInputPartitioner;
import org.apache.fluss.flink.source.lookup.LookupNormalizer;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.encode.KeyEncoder;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT case for {@code SupportsLookupCustomShuffle} (custom lookup shuffle) in Flink 2.2.
 *
 * <p>Runs lookup joins on a real Fluss + Flink mini-cluster with the {@code LOOKUP(..,'shuffle' =
 * 'true')} hint enabled (and lookup cache disabled) to verify that:
 *
 * <ul>
 *   <li>the Flink 2.2 planner actually invokes {@code getPartitioner()} on {@link
 *       FlinkLookupShuffleTableSource} (validating the getLookupRuntimeProvider -&gt;
 *       getPartitioner call order the implementation relies on),
 *   <li>the {@code FlussLookupInputPartitioner} is serialized and executed on task managers, and
 *   <li>results stay correct with the shuffle applied, including for nullable probe keys and
 *       partitioned primary-key tables.
 * </ul>
 */
public class Flink22LookupShuffleITCase extends FlinkTestBase {

    private static final String CATALOG_NAME = "test_catalog";

    private StreamExecutionEnvironment execEnv;
    private StreamTableEnvironment tEnv;

    @BeforeEach
    public void beforeEach() {
        bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.useCatalog(CATALOG_NAME);
        tEnv.executeSql(String.format("create database if not exists `%s`", DEFAULT_DB));
        tEnv.useDatabase(DEFAULT_DB);
        // parallelism > 1 so that the custom shuffle actually redistributes the probe stream
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    }

    @AfterEach
    void after() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database `%s` cascade", DEFAULT_DB));
    }

    @Test
    void testLookupShuffleOnNonPartitionedPkTable() throws Exception {
        String dim = "dim_pk";
        // pk == bucket key == id, 3 buckets, lookup cache disabled (not set)
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + "  id int not null,"
                                + "  address varchar,"
                                + "  name varchar,"
                                + "  primary key (id) NOT ENFORCED"
                                + ") with ('bucket.num' = '3', 'bucket.key' = 'id',"
                                + " 'lookup.async' = 'false')",
                        dim));
        try (Table dimTable = conn.getTable(TablePath.of(DEFAULT_DB, dim))) {
            UpsertWriter writer = dimTable.newUpsert().createWriter();
            for (int i = 1; i <= 5; i++) {
                writer.upsert(row(i, "address" + i, "name" + (i % 4)));
            }
            writer.flush();
        }

        registerNonPartitionedSrc();

        List<String> expected =
                Arrays.asList("+I[1, 11, name1]", "+I[2, 2, name2]", "+I[3, 33, name3]");
        String columns = "src.a, src.c, %s.name";
        String from = "FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc ON src.a = %s.id";

        // with the custom shuffle enabled
        String withShuffle =
                String.format(
                        "SELECT /*+ LOOKUP('table' = '%s', 'shuffle' = 'true') */ "
                                + columns
                                + " "
                                + from,
                        dim,
                        dim,
                        dim,
                        dim);
        // baseline without shuffle
        String withoutShuffle = String.format("SELECT " + columns + " " + from, dim, dim, dim);

        // The custom lookup shuffle must actually be applied: Flink wraps our InputDataPartitioner
        // in a RowDataCustomStreamPartitioner on the probe-side edge feeding the lookup join.
        // Checking the plan text is not enough here, because the 'shuffle=[true]' digest is driven
        // by the hint alone and Flink would fall back to a hash shuffle (still shown as
        // shuffle=[true]) when the source provides no custom partitioner.
        assertThat(usesCustomShufflePartitioner(withShuffle))
                .as("probe stream should be repartitioned by the Fluss custom partitioner")
                .isTrue();
        assertThat(usesCustomShufflePartitioner(withoutShuffle))
                .as("no custom partitioner without the shuffle hint")
                .isFalse();

        assertResultsIgnoreOrder(tEnv.executeSql(withShuffle).collect(), expected, true);
        // parity: same results without the shuffle
        assertResultsIgnoreOrder(tEnv.executeSql(withoutShuffle).collect(), expected, true);
    }

    @Test
    void testLeftLookupShuffleWithNullProbeKey() throws Exception {
        String dim = "dim_nullable_probe";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + "  id int not null,"
                                + "  name varchar,"
                                + "  primary key (id) NOT ENFORCED"
                                + ") with ('bucket.num' = '3', 'bucket.key' = 'id',"
                                + " 'lookup.async' = 'false')",
                        dim));
        try (Table dimTable = conn.getTable(TablePath.of(DEFAULT_DB, dim))) {
            UpsertWriter writer = dimTable.newUpsert().createWriter();
            writer.upsert(row(1, "name1"));
            writer.flush();
        }

        registerNullableKeySrc();

        String query =
                String.format(
                        "SELECT /*+ LOOKUP('table' = '%s', 'shuffle' = 'true') */ "
                                + "src.id, src.payload, %s.name "
                                + "FROM nullable_src AS src "
                                + "LEFT JOIN %s FOR SYSTEM_TIME AS OF src.proc "
                                + "ON src.id = %s.id",
                        dim, dim, dim, dim);

        assertThat(usesCustomShufflePartitioner(query))
                .as("nullable probe stream should use the Fluss custom partitioner")
                .isTrue();
        assertResultsIgnoreOrder(
                tEnv.executeSql(query).collect(),
                Arrays.asList("+I[1, matched, name1]", "+I[null, null-key, null]"),
                true);
    }

    @Test
    void testLookupShuffleOnPartitionedPkTable() throws Exception {
        String dim = "dim_pk_part";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + "  id int not null,"
                                + "  address varchar,"
                                + "  name varchar,"
                                + "  p_date varchar,"
                                + "  primary key (id, p_date) NOT ENFORCED"
                                + ") partitioned by (p_date) with ("
                                + " 'bucket.num' = '3', 'bucket.key' = 'id', 'lookup.async' = 'false',"
                                + " 'table.auto-partition.enabled' = 'true',"
                                + " 'table.auto-partition.time-unit' = 'year')",
                        dim));

        TablePath dimPath = TablePath.of(DEFAULT_DB, dim);
        Map<Long, String> partitionNameById =
                waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), dimPath);
        Iterator<String> partitionIterator = partitionNameById.values().iterator();
        String partition1 = partitionIterator.next();
        String partition2 = partitionIterator.next();

        // dim data only lives in partition1
        try (Table dimTable = conn.getTable(dimPath)) {
            UpsertWriter writer = dimTable.newUpsert().createWriter();
            for (int i = 1; i <= 5; i++) {
                writer.upsert(row(i, "address" + i, "name" + (i % 4), partition1));
            }
            writer.flush();
        }

        registerPartitionedSrc(partition1, partition2);

        // only src rows in partition1 match dim data
        List<String> expected = Arrays.asList("+I[1, 11, name1]", "+I[2, 2, name2]");
        String columns = "src.a, src.c, %s.name";
        String from =
                "FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc ON src.a = %s.id"
                        + " AND src.p_date = %s.p_date";

        String withShuffle =
                String.format(
                        "SELECT /*+ LOOKUP('table' = '%s', 'shuffle' = 'true') */ "
                                + columns
                                + " "
                                + from,
                        dim,
                        dim,
                        dim,
                        dim,
                        dim);
        String withoutShuffle = String.format("SELECT " + columns + " " + from, dim, dim, dim, dim);

        assertThat(usesCustomShufflePartitioner(withShuffle))
                .as("probe stream should be repartitioned by the Fluss custom partitioner")
                .isTrue();
        assertThat(usesCustomShufflePartitioner(withoutShuffle))
                .as("no custom partitioner without the shuffle hint")
                .isFalse();

        assertResultsIgnoreOrder(tEnv.executeSql(withShuffle).collect(), expected, true);
        assertResultsIgnoreOrder(tEnv.executeSql(withoutShuffle).collect(), expected, true);
    }

    @Test
    void testLookupShuffleOnPrefixKeyLookup() throws Exception {
        String dim = "dim_prefix";
        // primary key (name, id), bucket key = name (a strict prefix of the PK). A lookup on the
        // bucket key alone is a prefix lookup, which must still be shuffled by the bucket key.
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + "  name varchar not null,"
                                + "  id int not null,"
                                + "  address varchar,"
                                + "  primary key (name, id) NOT ENFORCED"
                                + ") with ('bucket.num' = '3', 'bucket.key' = 'name',"
                                + " 'lookup.async' = 'false')",
                        dim));
        try (Table dimTable = conn.getTable(TablePath.of(DEFAULT_DB, dim))) {
            UpsertWriter writer = dimTable.newUpsert().createWriter();
            writer.upsert(row("name1", 1, "address1"));
            writer.upsert(row("name1", 5, "address5"));
            writer.upsert(row("name2", 2, "address2"));
            writer.upsert(row("name0", 10, "address4"));
            writer.flush();
        }

        registerNonPartitionedSrc();

        // prefix lookup on the bucket key (name) returns every dim row sharing that name
        List<String> expected =
                Arrays.asList(
                        "+I[1, name1, address1]",
                        "+I[1, name1, address5]",
                        "+I[2, name2, address2]",
                        "+I[10, name0, address4]");
        String columns = "src.a, src.b, %s.address";
        String from = "FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc ON src.b = %s.name";

        String withShuffle =
                String.format(
                        "SELECT /*+ LOOKUP('table' = '%s', 'shuffle' = 'true') */ "
                                + columns
                                + " "
                                + from,
                        dim,
                        dim,
                        dim,
                        dim);
        String withoutShuffle = String.format("SELECT " + columns + " " + from, dim, dim, dim);

        // The prefix lookup must actually be shuffled by the custom partitioner (before this
        // change prefix lookups were excluded and produced no shuffle at all).
        assertThat(usesCustomShufflePartitioner(withShuffle))
                .as("prefix lookup probe stream should be repartitioned by the Fluss partitioner")
                .isTrue();
        assertThat(usesCustomShufflePartitioner(withoutShuffle))
                .as("no custom partitioner without the shuffle hint")
                .isFalse();

        assertResultsIgnoreOrder(tEnv.executeSql(withShuffle).collect(), expected, true);
        assertResultsIgnoreOrder(tEnv.executeSql(withoutShuffle).collect(), expected, true);
    }

    @Test
    void testLookupShuffleOnPartitionedPrefixKeyLookup() throws Exception {
        String dim = "dim_prefix_part";
        // partitioned table; primary key (name, id, p_date), bucket key = name (a strict prefix of
        // the PK). A lookup on the bucket key + partition key (name, p_date) is a partitioned
        // prefix
        // lookup, which must still be shuffled by the bucket key.
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + "  name varchar not null,"
                                + "  id int not null,"
                                + "  address varchar,"
                                + "  p_date varchar,"
                                + "  primary key (name, id, p_date) NOT ENFORCED"
                                + ") partitioned by (p_date) with ("
                                + " 'bucket.num' = '3', 'bucket.key' = 'name', 'lookup.async' = 'false',"
                                + " 'table.auto-partition.enabled' = 'true',"
                                + " 'table.auto-partition.time-unit' = 'year')",
                        dim));

        TablePath dimPath = TablePath.of(DEFAULT_DB, dim);
        Map<Long, String> partitionNameById =
                waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), dimPath);
        Iterator<String> partitionIterator = partitionNameById.values().iterator();
        String partition1 = partitionIterator.next();
        String partition2 = partitionIterator.next();

        try (Table dimTable = conn.getTable(dimPath)) {
            UpsertWriter writer = dimTable.newUpsert().createWriter();
            writer.upsert(row("name1", 1, "address1", partition1));
            writer.upsert(row("name1", 5, "address5", partition1));
            writer.upsert(row("name2", 2, "address2", partition1));
            writer.upsert(row("name0", 10, "address4", partition2));
            writer.flush();
        }

        registerPartitionedSrc(partition1, partition2);

        // prefix lookup on bucket key + partition key (name, p_date)
        List<String> expected =
                Arrays.asList(
                        "+I[1, name1, address1]",
                        "+I[1, name1, address5]",
                        "+I[2, name2, address2]",
                        "+I[10, name0, address4]");
        String columns = "src.a, src.b, %s.address";
        String from =
                "FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc ON src.b = %s.name"
                        + " AND src.p_date = %s.p_date";

        String withShuffle =
                String.format(
                        "SELECT /*+ LOOKUP('table' = '%s', 'shuffle' = 'true') */ "
                                + columns
                                + " "
                                + from,
                        dim,
                        dim,
                        dim,
                        dim,
                        dim);
        String withoutShuffle = String.format("SELECT " + columns + " " + from, dim, dim, dim, dim);

        assertThat(usesCustomShufflePartitioner(withShuffle))
                .as("partitioned prefix lookup should use the Fluss custom partitioner")
                .isTrue();
        assertThat(usesCustomShufflePartitioner(withoutShuffle))
                .as("no custom partitioner without the shuffle hint")
                .isFalse();

        assertResultsIgnoreOrder(tEnv.executeSql(withShuffle).collect(), expected, true);
        assertResultsIgnoreOrder(tEnv.executeSql(withoutShuffle).collect(), expected, true);
    }

    /**
     * Builds the {@link StreamGraph} for the given query and reports whether the probe stream
     * feeding the lookup join is repartitioned by Fluss' custom {@code InputDataPartitioner} (which
     * Flink wraps in a {@code RowDataCustomStreamPartitioner}). Returns {@code false} when the edge
     * uses a forward/hash partitioner instead, i.e. when the custom shuffle was not applied.
     */
    private boolean usesCustomShufflePartitioner(String query) {
        tEnv.toChangelogStream(tEnv.sqlQuery(query));
        // clearTransformations=true so the next inspection starts from a clean environment
        StreamGraph streamGraph = execEnv.getStreamGraph(true);
        for (StreamNode node : streamGraph.getStreamNodes()) {
            for (StreamEdge edge : node.getInEdges()) {
                if ("RowDataCustomStreamPartitioner"
                        .equals(edge.getPartitioner().getClass().getSimpleName())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Test
    void testSameBucketKeysAreRoutedToSameSubtask() throws Exception {
        int numBuckets = 4;
        int parallelism = 4;
        int numKeys = 12;
        execEnv.setParallelism(parallelism);

        // The production partitioner, wired exactly as FlinkLookupShuffleTableSource#getPartitioner
        // builds it for a table whose primary key == bucket key (identity normalizer).
        RowType keyRowType =
                RowType.of(new LogicalType[] {new IntType(false)}, new String[] {"id"});
        LookupNormalizer normalizer =
                LookupNormalizer.createPrimaryKeyLookupNormalizer(new int[] {0}, keyRowType);
        FlussLookupInputPartitioner flussPartitioner =
                new FlussLookupInputPartitioner(
                        normalizer,
                        keyRowType,
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        /* lakeFormat */ null,
                        numBuckets);

        List<Integer> ids = new ArrayList<>();
        for (int i = 1; i <= numKeys; i++) {
            ids.add(i);
        }

        // Route the keys through a real Flink job using the production partitioner. This mirrors
        // RowDataCustomStreamPartitioner#selectChannel (partition(key, numberOfChannels)); the
        // downstream map is a FORWARD chain, so the subtask it observes IS the channel the key was
        // routed to.
        DataStream<Row> tagged =
                execEnv.fromCollection(ids)
                        .partitionCustom(
                                new DelegatingPartitioner(flussPartitioner), new IdKeySelector())
                        .map(new SubtaskTagger())
                        .returns(Types.ROW(Types.INT, Types.INT));

        Map<Integer, Integer> subtaskById = new HashMap<>();
        try (CloseableIterator<Row> it = tagged.executeAndCollect()) {
            while (it.hasNext()) {
                Row r = it.next();
                subtaskById.put((Integer) r.getField(1), (Integer) r.getField(0));
            }
        }
        assertThat(subtaskById).as("every key should be observed once").hasSize(numKeys);

        // Core property: same Fluss bucket -> same subtask (subtask == bucketId % parallelism).
        Map<Integer, Integer> subtaskByBucket = new HashMap<>();
        for (Map.Entry<Integer, Integer> e : subtaskById.entrySet()) {
            int id = e.getKey();
            int subtask = e.getValue();
            int bucket = flussBucketOf(id, numBuckets);
            assertThat(subtask)
                    .as("id=%d (bucket=%d) should be routed to bucketId %% parallelism", id, bucket)
                    .isEqualTo(Math.floorMod(bucket, parallelism));
            Integer prev = subtaskByBucket.putIfAbsent(bucket, subtask);
            if (prev != null) {
                assertThat(subtask)
                        .as("all keys of bucket %d must share one subtask", bucket)
                        .isEqualTo(prev);
            }
        }

        // Sanity: keys actually spread across multiple subtasks (not trivially co-located).
        assertThat(new HashSet<>(subtaskById.values()).size())
                .as("keys should be spread across more than one subtask")
                .isGreaterThan(1);
    }

    @Test
    void testPartitionedTableSpreadsAcrossSubtasks() throws Exception {
        // A partitioned table with bucket.num == 1: every row has bucketId 0, so routing by bucket
        // alone would send all probe rows to subtask 0. Partition-aware routing must spread the
        // partitions across subtasks while keeping each (partition, bucket) co-located.
        int numBuckets = 1;
        int parallelism = 4;
        execEnv.setParallelism(parallelism);

        RowType keyRowType =
                RowType.of(
                        new LogicalType[] {
                            new IntType(false), new VarCharType(false, Integer.MAX_VALUE)
                        },
                        new String[] {"id", "p_date"});
        LookupNormalizer normalizer =
                LookupNormalizer.createPrimaryKeyLookupNormalizer(new int[] {0, 1}, keyRowType);
        FlussLookupInputPartitioner flussPartitioner =
                new FlussLookupInputPartitioner(
                        normalizer,
                        keyRowType,
                        Collections.singletonList("id"),
                        Collections.singletonList("p_date"),
                        /* lakeFormat */ null,
                        numBuckets);

        List<Row> rows = new ArrayList<>();
        for (int p = 0; p < 12; p++) {
            rows.add(Row.of(1, "2024-" + p));
        }

        DataStream<Row> tagged =
                execEnv.fromCollection(rows)
                        .returns(Types.ROW(Types.INT, Types.STRING))
                        .partitionCustom(
                                new DelegatingPartitioner(flussPartitioner),
                                new PartitionKeySelector())
                        .map(new PartitionSubtaskTagger())
                        .returns(Types.ROW(Types.INT, Types.STRING));

        Map<String, Integer> subtaskByPartition = new HashMap<>();
        try (CloseableIterator<Row> it = tagged.executeAndCollect()) {
            while (it.hasNext()) {
                Row r = it.next();
                subtaskByPartition.put((String) r.getField(1), (Integer) r.getField(0));
            }
        }

        assertThat(new HashSet<>(subtaskByPartition.values()).size())
                .as("partitions sharing bucket id 0 must not all collapse onto subtask 0")
                .isGreaterThan(1);
    }

    /** Independently computes the Fluss bucket id for an int key, mirroring the client routing. */
    private static int flussBucketOf(int id, int numBuckets) {
        RowType keyRowType =
                RowType.of(new LogicalType[] {new IntType(false)}, new String[] {"id"});
        org.apache.fluss.types.RowType flussKeyType = FlinkConversions.toFlussRowType(keyRowType);
        KeyEncoder encoder =
                KeyEncoder.ofBucketKeyEncoder(flussKeyType, Collections.singletonList("id"), null);
        byte[] bytes = encoder.encodeKey(new FlinkAsFlussRow().replace(GenericRowData.of(id)));
        return BucketingFunction.of(null).bucketing(bytes, numBuckets);
    }

    /** Flink partitioner that delegates channel selection to Fluss' custom lookup partitioner. */
    private static class DelegatingPartitioner implements Partitioner<RowData> {
        private final FlussLookupInputPartitioner partitioner;

        private DelegatingPartitioner(FlussLookupInputPartitioner partitioner) {
            this.partitioner = partitioner;
        }

        @Override
        public int partition(RowData key, int numPartitions) {
            return partitioner.partition(key, numPartitions);
        }
    }

    /** Builds the single-column int lookup key row from an id. */
    private static class IdKeySelector implements KeySelector<Integer, RowData> {
        @Override
        public RowData getKey(Integer id) {
            return GenericRowData.of(id);
        }
    }

    /** Builds the (id, p_date) lookup key row for a partitioned table. */
    private static class PartitionKeySelector implements KeySelector<Row, RowData> {
        @Override
        public RowData getKey(Row row) {
            return GenericRowData.of(
                    (Integer) row.getField(0), StringData.fromString((String) row.getField(1)));
        }
    }

    /** Tags each element with the subtask index that processed it. */
    private static class SubtaskTagger extends RichMapFunction<Integer, Row> {
        @Override
        public Row map(Integer id) {
            return Row.of(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), id);
        }
    }

    /** Tags each partitioned row with the subtask index that processed it. */
    private static class PartitionSubtaskTagger extends RichMapFunction<Row, Row> {
        @Override
        public Row map(Row row) {
            return Row.of(
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), row.getField(1));
        }
    }

    private void registerNonPartitionedSrc() {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, "name1", 11),
                        Row.of(2, "name2", 2),
                        Row.of(3, "name33", 33),
                        Row.of(10, "name0", 44));
        RowTypeInfo typeInfo =
                new RowTypeInfo(
                        new TypeInformation[] {Types.INT, Types.STRING, Types.INT},
                        new String[] {"a", "b", "c"});
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.INT())
                        .columnByExpression("proc", "PROCTIME()")
                        .build();
        DataStream<Row> srcDs = execEnv.fromCollection(testData).returns(typeInfo);
        tEnv.createTemporaryView("src", tEnv.fromDataStream(srcDs, schema));
    }

    private void registerNullableKeySrc() {
        List<Row> testData = Arrays.asList(Row.of(1, "matched"), Row.of(null, "null-key"));
        RowTypeInfo typeInfo =
                new RowTypeInfo(
                        new TypeInformation[] {Types.INT, Types.STRING},
                        new String[] {"id", "payload"});
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .columnByExpression("proc", "PROCTIME()")
                        .build();
        DataStream<Row> srcDs = execEnv.fromCollection(testData).returns(typeInfo);
        tEnv.createTemporaryView("nullable_src", tEnv.fromDataStream(srcDs, schema));
    }

    private void registerPartitionedSrc(String partition1, String partition2) {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, "name1", 11, partition1),
                        Row.of(2, "name2", 2, partition1),
                        Row.of(3, "name33", 33, partition2),
                        Row.of(10, "name0", 44, partition2));
        RowTypeInfo typeInfo =
                new RowTypeInfo(
                        new TypeInformation[] {Types.INT, Types.STRING, Types.INT, Types.STRING},
                        new String[] {"a", "b", "c", "p_date"});
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.INT())
                        .column("p_date", DataTypes.STRING())
                        .columnByExpression("proc", "PROCTIME()")
                        .build();
        DataStream<Row> srcDs = execEnv.fromCollection(testData).returns(typeInfo);
        tEnv.createTemporaryView("src", tEnv.fromDataStream(srcDs, schema));
    }
}
