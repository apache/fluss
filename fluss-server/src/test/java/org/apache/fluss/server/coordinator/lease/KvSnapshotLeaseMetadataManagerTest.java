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

package org.apache.fluss.server.coordinator.lease;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.lease.KvSnapshotLeaseMetadata;
import org.apache.fluss.server.zk.data.lease.KvSnapshotTableLease;
import org.apache.fluss.server.zk.data.lease.KvSnapshotTableLeaseJsonSerde;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.MapUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        metadataManager.registerLease("leaseId1", new KvSnapshotLeaseHandler(1000L));

        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        tableIdToTableLease.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L, -1L}));
        metadataManager.registerLease(
                "leaseId2", new KvSnapshotLeaseHandler(2000L, tableIdToTableLease));
        leasesList = metadataManager.getLeasesList();
        assertThat(leasesList).containsExactlyInAnyOrder("leaseId1", "leaseId2");
    }

    @Test
    void testRegisterAndUpdateLease() throws Exception {
        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        tableIdToTableLease.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L, -1L}));

        ConcurrentHashMap<Long, Long[]> partitionSnapshots = MapUtils.newConcurrentHashMap();
        partitionSnapshots.put(1000L, new Long[] {111L, 122L});
        partitionSnapshots.put(1001L, new Long[] {122L, -1L});
        tableIdToTableLease.put(2L, new KvSnapshotTableLease(2L, partitionSnapshots));

        KvSnapshotLeaseHandler expectedLease =
                new KvSnapshotLeaseHandler(1000L, tableIdToTableLease);
        metadataManager.registerLease("leaseId1", expectedLease);

        Optional<KvSnapshotLeaseHandler> lease = metadataManager.getLease("leaseId1");
        assertThat(lease).hasValue(expectedLease);
        // assert zk and remote fs.
        assertRemoteFsAndZkEquals("leaseId1", expectedLease);

        // test update lease.
        tableIdToTableLease.remove(1L);
        expectedLease = new KvSnapshotLeaseHandler(2000L, tableIdToTableLease);
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

    @Test
    void testRegisterLeaseFailureOnlyCleanupCreatedFiles() throws Exception {
        // Step 1: Register a lease successfully with table 1.
        Map<Long, KvSnapshotTableLease> tableLeases1 = new HashMap<>();
        tableLeases1.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L}));
        KvSnapshotLeaseHandler lease1 = new KvSnapshotLeaseHandler(1000L, tableLeases1);
        metadataManager.registerLease("leaseId1", lease1);

        // Record the original file path from ZK metadata.
        Optional<KvSnapshotLeaseMetadata> originalMeta =
                zookeeperClient.getKvSnapshotLeaseMetadata("leaseId1");
        assertThat(originalMeta).isPresent();
        FsPath originalFile = originalMeta.get().getTableIdToRemoteMetadataFilePath().get(1L);
        assertThat(originalFile.getFileSystem().exists(originalFile)).isTrue();

        // Step 2: Try to register again with the same leaseId but a different table.
        // This should fail because the ZK node already exists.
        Map<Long, KvSnapshotTableLease> tableLeases2 = new HashMap<>();
        tableLeases2.put(2L, new KvSnapshotTableLease(2L, new Long[] {200L}));
        KvSnapshotLeaseHandler lease2 = new KvSnapshotLeaseHandler(2000L, tableLeases2);

        assertThatThrownBy(() -> metadataManager.registerLease("leaseId1", lease2))
                .isInstanceOf(Exception.class);

        // Step 3: Verify the original file (from the first successful register) still exists.
        assertThat(originalFile.getFileSystem().exists(originalFile)).isTrue();

        // Step 4: Verify the file created for table 2 was cleaned up.
        Path table2Dir = tempDir.resolve("lease/kv-snapshot/leaseId1/2");
        if (Files.exists(table2Dir)) {
            try (Stream<Path> files = Files.list(table2Dir)) {
                assertThat(files.filter(p -> p.toString().endsWith(".metadata")).count())
                        .isEqualTo(0);
            }
        }
    }

    @Test
    void testUpdateLeaseFailureOnlyCleanupCreatedFiles() throws Exception {
        // Step 1: Register a lease successfully with table 1.
        Map<Long, KvSnapshotTableLease> tableLeases = new HashMap<>();
        tableLeases.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L}));
        KvSnapshotLeaseHandler originalLease = new KvSnapshotLeaseHandler(1000L, tableLeases);
        metadataManager.registerLease("leaseId1", originalLease);

        // Record the original file path.
        Optional<KvSnapshotLeaseMetadata> originalMeta =
                zookeeperClient.getKvSnapshotLeaseMetadata("leaseId1");
        assertThat(originalMeta).isPresent();
        FsPath originalFile = originalMeta.get().getTableIdToRemoteMetadataFilePath().get(1L);
        assertThat(originalFile.getFileSystem().exists(originalFile)).isTrue();

        // Step 2: Delete the ZK node to simulate a failure scenario.
        // updateKvSnapshotLeaseMetadata will throw NoNodeException.
        zookeeperClient.deleteKvSnapshotLease("leaseId1");

        // Step 3: Try to update the lease (will fail at ZK update).
        // generateMetadataFile creates new files, but they should be cleaned up on failure.
        Map<Long, KvSnapshotTableLease> newTableLeases = new HashMap<>();
        newTableLeases.put(1L, new KvSnapshotTableLease(1L, new Long[] {200L}));
        newTableLeases.put(2L, new KvSnapshotTableLease(2L, new Long[] {300L}));
        KvSnapshotLeaseHandler updatedLease = new KvSnapshotLeaseHandler(2000L, newTableLeases);

        assertThatThrownBy(() -> metadataManager.updateLease("leaseId1", updatedLease))
                .isInstanceOf(Exception.class);

        // Step 4: Verify the original file (from the first register) is preserved on disk.
        // It was NOT in createdFiles, so it should NOT be cleaned up.
        assertThat(originalFile.getFileSystem().exists(originalFile)).isTrue();

        // Step 5: Verify the newly created files for table 2 were cleaned up.
        Path table2Dir = tempDir.resolve("lease/kv-snapshot/leaseId1/2");
        if (Files.exists(table2Dir)) {
            try (Stream<Path> files = Files.list(table2Dir)) {
                assertThat(files.filter(p -> p.toString().endsWith(".metadata")).count())
                        .isEqualTo(0);
            }
        }

        // Verify the newly created file for table 1 (with new UUID) was also cleaned up.
        // Only the original file should remain under table 1 directory.
        Path table1Dir = tempDir.resolve("lease/kv-snapshot/leaseId1/1");
        try (Stream<Path> files = Files.list(table1Dir)) {
            long metadataFileCount = files.filter(p -> p.toString().endsWith(".metadata")).count();
            // Only the original file should remain.
            assertThat(metadataFileCount).isEqualTo(1);
        }
    }

    @Test
    void testUpdateLeaseSuccessCleansUpOldFiles() throws Exception {
        // Step 1: Register a lease with table 1.
        Map<Long, KvSnapshotTableLease> tableLeases = new HashMap<>();
        tableLeases.put(1L, new KvSnapshotTableLease(1L, new Long[] {100L}));
        KvSnapshotLeaseHandler originalLease = new KvSnapshotLeaseHandler(1000L, tableLeases);
        metadataManager.registerLease("leaseId1", originalLease);

        // Record the original file path.
        Optional<KvSnapshotLeaseMetadata> originalMeta =
                zookeeperClient.getKvSnapshotLeaseMetadata("leaseId1");
        assertThat(originalMeta).isPresent();
        FsPath originalFile = originalMeta.get().getTableIdToRemoteMetadataFilePath().get(1L);
        assertThat(originalFile.getFileSystem().exists(originalFile)).isTrue();

        // Step 2: Update the lease successfully with different data.
        Map<Long, KvSnapshotTableLease> newTableLeases = new HashMap<>();
        newTableLeases.put(1L, new KvSnapshotTableLease(1L, new Long[] {200L}));
        KvSnapshotLeaseHandler updatedLease = new KvSnapshotLeaseHandler(2000L, newTableLeases);
        metadataManager.updateLease("leaseId1", updatedLease);

        // Step 3: Verify the old file was cleaned up after successful update.
        assertThat(originalFile.getFileSystem().exists(originalFile)).isFalse();

        // Step 4: Verify the new file exists and ZK points to it.
        Optional<KvSnapshotLeaseMetadata> newMeta =
                zookeeperClient.getKvSnapshotLeaseMetadata("leaseId1");
        assertThat(newMeta).isPresent();
        FsPath newFile = newMeta.get().getTableIdToRemoteMetadataFilePath().get(1L);
        assertThat(newFile.getFileSystem().exists(newFile)).isTrue();
        // The new file should be different from the old one (different UUID).
        assertThat(newFile).isNotEqualTo(originalFile);
    }

    private void assertRemoteFsAndZkEquals(String leaseId, KvSnapshotLeaseHandler expectedLease)
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
                Assertions.assertThat(
                                KvSnapshotTableLeaseJsonSerde.fromJson(outputStream.toByteArray()))
                        .isEqualTo(expectedTableLease);
            }
        }
    }
}
