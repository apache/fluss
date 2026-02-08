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

package org.apache.fluss.server.zk.data.lease;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KvSnapshotLeaseMetadataManager}. */
public class KvSnapshotLeaseMetadataManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    protected static ZooKeeperClient zookeeperClient;
    private @TempDir Path tempDir;
    private KvSnapshotLeaseMetadataManager metadataManager;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void beforeEach() {
        metadataManager = new KvSnapshotLeaseMetadataManager(zookeeperClient, tempDir.toString());
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @Test
    void testGetLeasesList() throws Exception {
        List<String> leasesList = metadataManager.getLeasesList();
        assertThat(leasesList).isEmpty();

        metadataManager.registerLease("leaseId1", new KvSnapshotLease(1000L));

        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        tableIdToTableLease.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L, -1L}));
        metadataManager.registerLease("leaseId2", new KvSnapshotLease(2000L, tableIdToTableLease));
        leasesList = metadataManager.getLeasesList();
        assertThat(leasesList).containsExactlyInAnyOrder("leaseId1", "leaseId2");
    }

    @Test
    void testRegisterAndUpdateLease() throws Exception {
        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        tableIdToTableLease.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L, -1L}));

        Map<Long, Long[]> partitionSnapshots = new HashMap<>();
        partitionSnapshots.put(1000L, new Long[] {111L, 122L});
        partitionSnapshots.put(1001L, new Long[] {122L, -1L});
        tableIdToTableLease.put(2L, new KvSnapshotTableLease(2L, partitionSnapshots));

        KvSnapshotLease expectedLease = new KvSnapshotLease(1000L, tableIdToTableLease);
        metadataManager.registerLease("leaseId1", expectedLease);

        Optional<KvSnapshotLease> lease = metadataManager.getLease("leaseId1");
        assertThat(lease).hasValue(expectedLease);
        // assert zk and remote fs.
        assertRemoteFsAndZkEquals("leaseId1", expectedLease);

        // test update lease.
        tableIdToTableLease.remove(1L);
        expectedLease = new KvSnapshotLease(2000L, tableIdToTableLease);
        metadataManager.updateLease("leaseId1", expectedLease);
        lease = metadataManager.getLease("leaseId1");
        assertThat(lease).hasValue(expectedLease);
        // assert zk and remote fs.
        assertRemoteFsAndZkEquals("leaseId1", expectedLease);

        // test delete lease.
        metadataManager.deleteLease("leaseId1");
        lease = metadataManager.getLease("leaseId1");
        assertThat(lease).isEmpty();
    }

    private void assertRemoteFsAndZkEquals(String leaseId, KvSnapshotLease expectedLease)
            throws Exception {
        Optional<KvSnapshotLeaseMetadata> leaseMetadataOpt =
                zookeeperClient.getKvSnapshotLeaseMetadata(leaseId);
        assertThat(leaseMetadataOpt).isPresent();
        KvSnapshotLeaseMetadata leaseMetadata = leaseMetadataOpt.get();
        assertThat(leaseMetadata.getExpirationTime()).isEqualTo(expectedLease.getExpirationTime());
        Map<Long, FsPath> actualFsPathSet = leaseMetadata.getTableIdToRemoteMetadataFilePath();
        Map<Long, KvSnapshotTableLease> expectedTableLeases =
                expectedLease.getTableIdToTableLease();
        assertThat(actualFsPathSet).hasSize(expectedTableLeases.size());
        for (Map.Entry<Long, FsPath> actualEntry : actualFsPathSet.entrySet()) {
            long tableId = actualEntry.getKey();
            FsPath actualMetadataPath = actualEntry.getValue();
            assertThat(actualMetadataPath).isNotNull();
            KvSnapshotTableLease expectedTableLease = expectedTableLeases.get(tableId);
            assertThat(expectedTableLease).isNotNull();
            FSDataInputStream inputStream =
                    actualMetadataPath.getFileSystem().open(actualMetadataPath);
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                IOUtils.copyBytes(inputStream, outputStream, true);
                assertThat(KvSnapshotTableLeaseJsonSerde.fromJson(outputStream.toByteArray()))
                        .isEqualTo(expectedTableLease);
            }
        }
    }
}
