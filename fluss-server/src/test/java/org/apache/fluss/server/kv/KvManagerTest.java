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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestData;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KvManager} . */
final class KvManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private final RowType baseRowType = TestData.DATA1_ROW_TYPE;
    private static final short schemaId = 1;
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(schemaId);
    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(baseRowType);

    private static ZooKeeperClient zkClient;

    private @TempDir File tempDir;
    private TablePath tablePath1;
    private TablePath tablePath2;

    private TableBucket tableBucket1;
    private TableBucket tableBucket2;

    private LogManager logManager;
    private KvManager kvManager;
    private Configuration conf;

    @BeforeAll
    static void baseBeforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void setup() throws Exception {
        conf = new Configuration();
        conf.setString(ConfigOptions.DATA_DIR, tempDir.getAbsolutePath());

        String dbName = "db1";
        tablePath1 = TablePath.of(dbName, "t1");
        tablePath2 = TablePath.of(dbName, "t2");

        // we need a log manager for kv manager

        logManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        kvManager =
                KvManager.create(
                        conf, zkClient, logManager, TestingMetricGroups.TABLET_SERVER_METRICS);
        kvManager.startup();
    }

    @AfterEach
    void tearDown() {
        if (kvManager != null) {
            kvManager.shutdown();
        }
        if (logManager != null) {
            logManager.shutdown();
        }
    }

    static List<String> partitionProvider() {
        return Arrays.asList(null, "2024");
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testCreateKv(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        KvTablet kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        KvTablet kv2 = getOrCreateKv(tablePath2, partitionName, tableBucket2);

        byte[] k1 = "k1".getBytes();
        KvRecord kvRecord1 = kvRecordFactory.ofRecord(k1, new Object[] {1, "a"});
        put(kv1, kvRecord1);

        byte[] k2 = "k2".getBytes();
        KvRecord kvRecord2 = kvRecordFactory.ofRecord(k2, new Object[] {2, "b"});
        put(kv2, kvRecord2);

        KvTablet newKv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        KvTablet newKv2 = getOrCreateKv(tablePath2, partitionName, tableBucket2);
        verifyMultiGet(newKv1, k1, valueOf(kvRecord1));
        verifyMultiGet(newKv2, k2, valueOf(kvRecord2));
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testRecoveryAfterKvManagerShutDown(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        KvTablet kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        int kvRecordCount = 50;
        KvRecord[] kvRecords1 = new KvRecord[kvRecordCount];
        for (int i = 0; i < kvRecordCount; i++) {
            kvRecords1[i] = kvRecordFactory.ofRecord(("key" + i).getBytes(), new Object[] {i, "a"});
        }
        put(kv1, kvRecords1);

        KvTablet kv2 = getOrCreateKv(tablePath2, partitionName, tableBucket2);
        KvRecord[] kvRecords2 = new KvRecord[kvRecordCount];
        for (int i = 0; i < kvRecordCount; i++) {
            kvRecords2[i] = kvRecordFactory.ofRecord(("key" + i).getBytes(), new Object[] {i, "b"});
        }
        put(kv2, kvRecords2);

        // restart
        kvManager.shutdown();
        kvManager =
                KvManager.create(
                        conf, zkClient, logManager, TestingMetricGroups.TABLET_SERVER_METRICS);
        kvManager.startup();
        kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        kv2 = getOrCreateKv(tablePath2, partitionName, tableBucket2);

        List<byte[]> kv1Keys = new ArrayList<>(kvRecordCount);
        List<byte[]> kv1Values = new ArrayList<>(kvRecordCount);
        List<byte[]> kv2Keys = new ArrayList<>(kvRecordCount);
        List<byte[]> kv2Values = new ArrayList<>(kvRecordCount);

        for (int i = 0; i < kvRecordCount; i++) {
            kv1Keys.add(("key" + i).getBytes());
            kv1Values.add(valueOf(kvRecords1[i]));
            kv2Keys.add(("key" + i).getBytes());
            kv2Values.add(valueOf(kvRecords2[i]));
        }

        // check kv1
        assertThat(kv1.multiGet(kv1Keys)).containsExactlyElementsOf(kv1Values);
        // check kv2
        assertThat(kv2.multiGet(kv2Keys)).containsExactlyElementsOf(kv2Values);
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testRecoveryWithSchemaChange(String partitionName) throws Exception {
        TestingSchemaGetter testingSchemaGetter =
                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, 1));
        initTableBuckets(partitionName);
        KvTablet kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1, testingSchemaGetter);
        int kvRecordCount = 50;

        // insert before restart.
        KvRecord[] kvRecords1 = new KvRecord[kvRecordCount];
        for (int i = 0; i < kvRecordCount; i++) {
            kvRecords1[i] = kvRecordFactory.ofRecord(("key" + i).getBytes(), new Object[] {i, "a"});
        }
        put(kv1, kvRecords1);

        // restart with schema change
        short newSchemaId = 2;
        kvManager.shutdown();
        testingSchemaGetter.updateLatestSchemaInfo(new SchemaInfo(DATA2_SCHEMA, newSchemaId));
        kvManager =
                KvManager.create(
                        conf, zkClient, logManager, TestingMetricGroups.TABLET_SERVER_METRICS);
        kvManager.startup();

        // insert again after restart.
        kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1, testingSchemaGetter);
        KvRecordTestUtils.KvRecordBatchFactory batchFactoryOfSchema2 =
                KvRecordTestUtils.KvRecordBatchFactory.of(newSchemaId);
        KvRecordTestUtils.KvRecordFactory kvRecordFactoryOfSchema2 =
                KvRecordTestUtils.KvRecordFactory.of(DATA2_SCHEMA.getRowType());
        KvRecord[] kvRecords2 = new KvRecord[kvRecordCount];
        for (int i = 0; i < kvRecordCount; i++) {
            kvRecords2[i] =
                    kvRecordFactoryOfSchema2.ofRecord(
                            ("key" + (i + kvRecordCount)).getBytes(),
                            new Object[] {i + kvRecordCount, "b", "c"});
        }
        put(kv1, batchFactoryOfSchema2, kvRecords2);

        // check result.
        List<byte[]> kvKeys = new ArrayList<>(kvRecordCount);
        List<byte[]> kvValues = new ArrayList<>(kvRecordCount);

        for (int i = 0; i < kvRecordCount; i++) {
            kvKeys.add(("key" + i).getBytes());
            kvValues.add(valueOf(kvRecords1[i]));
            kvKeys.add(("key" + (i + kvRecordCount)).getBytes());
            kvValues.add(ValueEncoder.encodeValue(newSchemaId, kvRecords2[i].getRow()));
        }

        // check kv1
        assertThat(kv1.multiGet(kvKeys)).containsExactlyElementsOf(kvValues);
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testSameTableNameInDifferentDb(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        KvTablet kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        byte[] k1 = "k1".getBytes();
        KvRecord kvRecord1 = kvRecordFactory.ofRecord(k1, new Object[] {1, "a"});
        put(kv1, kvRecord1);

        // different db with same table name
        TablePath anotherDbTablePath = TablePath.of("db2", tablePath1.getTableName());
        KvTablet kv2 = getOrCreateKv(anotherDbTablePath, partitionName, tableBucket2);
        KvRecord kvRecord2 = kvRecordFactory.ofRecord(k1, new Object[] {2, "b"});
        put(kv2, kvRecord2);

        KvTablet newKv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        KvTablet newKv2 = getOrCreateKv(anotherDbTablePath, partitionName, tableBucket2);
        verifyMultiGet(newKv1, k1, valueOf(kvRecord1));
        verifyMultiGet(newKv2, k1, valueOf(kvRecord2));
    }

    @ParameterizedTest
    @MethodSource("partitionProvider")
    void testDropKv(String partitionName) throws Exception {
        initTableBuckets(partitionName);
        KvTablet kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        kvManager.dropKv(kv1.getTableBucket());

        assertThat(kv1.getKvTabletDir()).doesNotExist();
        assertThat(kvManager.getKv(tableBucket1)).isNotPresent();

        kv1 = getOrCreateKv(tablePath1, partitionName, tableBucket1);
        assertThat(kv1.getKvTabletDir()).exists();
        assertThat(kvManager.getKv(tableBucket1)).isPresent();
    }

    @Test
    void testGetNonExistentKv() {
        initTableBuckets(null);
        Optional<KvTablet> kv = kvManager.getKv(tableBucket1);
        assertThat(kv).isNotPresent();
    }

    private void initTableBuckets(@Nullable String partitionName) {
        if (partitionName == null) {
            tableBucket1 = new TableBucket(15001L, 1);
            tableBucket2 = new TableBucket(15002L, 2);
        } else {
            tableBucket1 = new TableBucket(15001L, 11L, 1);
            tableBucket2 = new TableBucket(15002L, 11L, 1);
        }
    }

    private void put(KvTablet kvTablet, KvRecord... kvRecords) throws Exception {
        put(kvTablet, kvRecordBatchFactory, kvRecords);
    }

    private void put(
            KvTablet kvTablet,
            KvRecordTestUtils.KvRecordBatchFactory factory,
            KvRecord... kvRecords)
            throws Exception {
        KvRecordBatch kvRecordBatch = factory.ofRecords(Arrays.asList(kvRecords));
        kvTablet.putAsLeader(kvRecordBatch, null);
        // flush to make sure data is visible
        kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
    }

    private KvTablet getOrCreateKv(
            TablePath tablePath, @Nullable String partitionName, TableBucket tableBucket)
            throws Exception {
        return getOrCreateKv(
                tablePath,
                partitionName,
                tableBucket,
                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, 1)));
    }

    private KvTablet getOrCreateKv(
            TablePath tablePath,
            @Nullable String partitionName,
            TableBucket tableBucket,
            SchemaGetter schemaGetter)
            throws Exception {
        PhysicalTablePath physicalTablePath =
                PhysicalTablePath.of(
                        tablePath.getDatabaseName(), tablePath.getTableName(), partitionName);
        LogTablet logTablet =
                logManager.getOrCreateLog(physicalTablePath, tableBucket, LogFormat.ARROW, 1, true);
        return kvManager.getOrCreateKv(
                physicalTablePath,
                tableBucket,
                logTablet,
                KvFormat.COMPACTED,
                schemaGetter,
                new TableConfig(new Configuration()),
                DEFAULT_COMPRESSION);
    }

    private byte[] valueOf(KvRecord kvRecord) {
        return ValueEncoder.encodeValue(schemaId, kvRecord.getRow());
    }

    private void verifyMultiGet(KvTablet kvTablet, byte[] key, byte[] expectedValue)
            throws IOException {
        List<byte[]> gotValues = kvTablet.multiGet(Collections.singletonList(key));
        assertThat(gotValues).containsExactly(expectedValue);
    }
}
