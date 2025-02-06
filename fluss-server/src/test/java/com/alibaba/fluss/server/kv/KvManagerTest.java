/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.kv;

import com.alibaba.fluss.compression.ArrowCompressionInfo;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.TableConfig;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.KvRecord;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.KvRecordTestUtils;
import com.alibaba.fluss.record.TestData;
import com.alibaba.fluss.row.encode.ValueEncoder;
import com.alibaba.fluss.server.log.LogManager;
import com.alibaba.fluss.server.log.LogTablet;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.clock.SystemClock;
import com.alibaba.fluss.utils.concurrent.FlussScheduler;

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

import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA_PK;
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
                LogManager.create(conf, zkClient, new FlussScheduler(1), SystemClock.getInstance());
        kvManager = KvManager.create(conf, zkClient, logManager);
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
        kvManager = KvManager.create(conf, zkClient, logManager);
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
        KvRecordBatch kvRecordBatch = kvRecordBatchFactory.ofRecords(Arrays.asList(kvRecords));
        kvTablet.putAsLeader(kvRecordBatch, null);
        // flush to make sure data is visible
        kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
    }

    private KvTablet getOrCreateKv(
            TablePath tablePath, @Nullable String partitionName, TableBucket tableBucket)
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
                DATA1_SCHEMA_PK,
                new TableConfig(new Configuration()),
                ArrowCompressionInfo.NO_COMPRESSION);
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
