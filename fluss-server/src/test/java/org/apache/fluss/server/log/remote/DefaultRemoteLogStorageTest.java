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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogStorage.IndexType;
import org.apache.fluss.shaded.guava32.com.google.common.io.Files;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.InputStream;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DefaultRemoteLogStorage}. */
class DefaultRemoteLogStorageTest extends RemoteLogTestBase {
    private DefaultRemoteLogStorage remoteLogStorageManager;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        remoteLogStorageManager = new DefaultRemoteLogStorage(conf);
    }

    @AfterEach
    public void teardown() throws Exception {
        remoteLogStorageManager.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCopyLogSegmentFiles(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogSegment remoteLogSegment =
                copyLogSegmentToRemote(logTablet, remoteLogStorageManager, 0);
        File remoteLogDir = getTestingRemoteLogSegmentDir(remoteLogSegment);
        assertThat(remoteLogDir.exists()).isTrue();
        File[] remoteFiles = remoteLogDir.listFiles();
        assertThat(remoteFiles).isNotNull();
        assertThat(remoteFiles).hasSize(4);

        File[] allFilesForLocalLog = logTablet.getLogDir().listFiles();
        assertThat(allFilesForLocalLog).isNotNull();
        for (File remoteFile : remoteFiles) {
            File localFile = getLocalFileByName(remoteFile.getName(), allFilesForLocalLog);
            assertThat(localFile).isNotNull();
            assertThat(Files.equal(remoteFile, localFile)).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeleteRemoteLogSegment(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogSegment remoteLogSegment =
                copyLogSegmentToRemote(logTablet, remoteLogStorageManager, 0);
        File remoteLogDir = getTestingRemoteLogSegmentDir(remoteLogSegment);
        assertThat(remoteLogDir.exists()).isTrue();
        File[] remoteFiles = remoteLogDir.listFiles();
        assertThat(remoteFiles).hasSize(4);

        remoteLogStorageManager.deleteLogSegmentFiles(remoteLogSegment);
        remoteLogDir = getTestingRemoteLogSegmentDir(remoteLogSegment);
        assertThat(remoteLogDir.exists()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFetchIndex(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogSegment remoteLogSegment =
                copyLogSegmentToRemote(logTablet, remoteLogStorageManager, 0);
        File remoteLogDir = getTestingRemoteLogSegmentDir(remoteLogSegment);
        assertThat(remoteLogDir.exists()).isTrue();

        File[] remoteFiles = remoteLogDir.listFiles();
        assertThat(remoteFiles).isNotNull();
        assertThat(remoteFiles).hasSize(4);

        File tmpIndexFile = new File(tempDir, "tmp-index");
        try (InputStream inputStream =
                remoteLogStorageManager.fetchIndex(remoteLogSegment, IndexType.OFFSET)) {
            java.nio.file.Files.copy(
                    inputStream, tmpIndexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }

        File[] allFilesForLocalLog = logTablet.getLogDir().listFiles();
        assertThat(allFilesForLocalLog).isNotNull();
        for (File remoteFile : remoteFiles) {
            File localFile = getLocalFileByName(remoteFile.getName(), allFilesForLocalLog);
            if (localFile
                    .getName()
                    .endsWith(RemoteLogStorage.IndexType.getFileSuffix(IndexType.OFFSET))) {
                assertThat(Files.equal(remoteFile, tmpIndexFile)).isTrue();
                assertThat(Files.equal(tmpIndexFile, localFile)).isTrue();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadWriteDeleteRemoteLogManifestSnapshot(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        // do snapshot.
        RemoteLogTablet remoteLogTablet = buildRemoteLogTablet(logTablet);
        List<RemoteLogSegment> remoteLogSegmentList = createRemoteLogSegmentList(logTablet);
        remoteLogTablet.addAndDeleteLogSegments(remoteLogSegmentList, Collections.emptyList());
        assertThat(remoteLogTablet.getIdToRemoteLogSegmentMap())
                .hasSize(remoteLogSegmentList.size());
        RemoteLogManifest manifestSnapshot = remoteLogTablet.currentManifest();
        assertThat(manifestSnapshot.getRemoteLogSegmentList()).isNotEmpty();

        FsPath remoteSnapshotDir =
                remoteLogStorageManager.writeRemoteLogManifestSnapshot(manifestSnapshot);
        assertThat(remoteSnapshotDir).isNotNull();
        File snapshotFile = new File(remoteSnapshotDir.getPath());
        assertThat(snapshotFile.exists()).isTrue();

        RemoteLogManifest result =
                remoteLogStorageManager.readRemoteLogManifestSnapshot(remoteSnapshotDir);
        assertThat(result).isEqualTo(manifestSnapshot);

        remoteLogStorageManager.deleteRemoteLogManifestSnapshot(remoteSnapshotDir);
        snapshotFile = new File(remoteSnapshotDir.getPath());
        assertThat(snapshotFile.exists()).isFalse();

        assertThatThrownBy(
                        () ->
                                remoteLogStorageManager.readRemoteLogManifestSnapshot(
                                        remoteSnapshotDir))
                .isInstanceOf(RemoteStorageException.class)
                .hasMessageContaining("Failed to read remote log manifest from " + snapshotFile);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeleteTable(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogSegment remoteLogSegment =
                copyLogSegmentToRemote(logTablet, remoteLogStorageManager, 0);
        File remoteLogDir = getTestingRemoteLogSegmentDir(remoteLogSegment);
        assertThat(remoteLogDir.exists()).isTrue();
        File[] remoteFiles = remoteLogDir.listFiles();
        assertThat(remoteFiles).hasSize(4);

        PhysicalTablePath physicalTablePath = logTablet.getPhysicalTablePath();
        TableBucket tableBucket = logTablet.getTableBucket();
        File remoteDirForBucket =
                new File(
                        FlussPaths.remoteLogTabletDir(
                                        remoteLogStorageManager.getRemoteLogDir(),
                                        physicalTablePath,
                                        tableBucket)
                                .toString());
        assertThat(remoteDirForBucket.exists()).isTrue();

        remoteLogStorageManager.deleteTableBucket(physicalTablePath, tableBucket);
        assertThat(remoteDirForBucket.exists()).isFalse();
        assertThatThrownBy(
                        () ->
                                remoteLogStorageManager.fetchIndex(
                                        remoteLogSegment, IndexType.OFFSET))
                .isInstanceOf(RemoteStorageException.class)
                .hasMessageContaining("Failed to fetch index file type: OFFSET from path");
    }

    private File getTestingRemoteLogSegmentDir(RemoteLogSegment remoteLogSegment) {
        return new File(
                FlussPaths.remoteLogSegmentDir(
                                FlussPaths.remoteLogTabletDir(
                                        remoteLogStorageManager.getRemoteLogDir(),
                                        remoteLogSegment.physicalTablePath(),
                                        remoteLogSegment.tableBucket()),
                                remoteLogSegment.remoteLogSegmentId())
                        .toString());
    }

    private File getLocalFileByName(String fileName, File[] allFilesForLocalLog) {
        for (File localFile : allFilesForLocalLog) {
            if (localFile.getName().equals(fileName)) {
                return localFile;
            }
        }
        throw new IllegalStateException("Can not find file: " + fileName);
    }
}
