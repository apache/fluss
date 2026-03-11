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

package org.apache.fluss.server.storage;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.exception.LogStorageException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LocalDiskManager}. */
class LocalDiskManagerTest {

    private static final int TABLET_SERVER_ID = 1;

    @TempDir private File tempDir;

    @Test
    void testCreateDiskPropertiesAndReuseDiskId() throws Exception {
        File dataDir1 = new File(tempDir, "data-1");
        File dataDir2 = new File(tempDir, "data-2");
        Configuration conf = createConf(dataDir1, dataDir2);

        String diskId1;
        String diskId2;
        try (LocalDiskManager localDiskManager = LocalDiskManager.create(conf)) {
            diskId1 = diskId(dataDir1);
            diskId2 = diskId(dataDir2);
            assertThat(new File(dataDir1, LocalDiskManager.DISK_PROPERTIES_FILE_NAME)).exists();
            assertThat(new File(dataDir2, LocalDiskManager.DISK_PROPERTIES_FILE_NAME)).exists();
            assertThat(new File(dataDir1, LocalDiskManager.LOCK_FILE_NAME)).exists();
            assertThat(new File(dataDir2, LocalDiskManager.LOCK_FILE_NAME)).exists();
            assertThat(localDiskManager.diskId(dataDir1)).isEqualTo(diskId1);
            assertThat(localDiskManager.diskId(dataDir2)).isEqualTo(diskId2);
        }

        try (LocalDiskManager localDiskManager = LocalDiskManager.create(conf)) {
            assertThat(localDiskManager.diskId(dataDir1)).isEqualTo(diskId1);
            assertThat(localDiskManager.diskId(dataDir2)).isEqualTo(diskId2);
        }
    }

    @Test
    void testFallbackToDataDirWhenDataDirsIsNotConfigured() throws Exception {
        File expectedDataDir = new File(tempDir, "nested/../data-single");
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLET_SERVER_ID, TABLET_SERVER_ID);
        conf.setString(ConfigOptions.DATA_DIR, expectedDataDir.getPath());

        try (LocalDiskManager localDiskManager = LocalDiskManager.create(conf)) {
            assertThat(localDiskManager.dataDirs())
                    .containsExactly(
                            expectedDataDir.getAbsoluteFile().toPath().normalize().toFile());
        }
    }

    @Test
    void testDataDirsTakePrecedenceOverDataDir() throws Exception {
        File fallbackDataDir = new File(tempDir, "fallback-single");
        File dataDir1 = new File(tempDir, "data-1");
        File dataDir2 = new File(tempDir, "data-2");

        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLET_SERVER_ID, TABLET_SERVER_ID);
        conf.setString(ConfigOptions.DATA_DIR, fallbackDataDir.getAbsolutePath());
        conf.set(
                ConfigOptions.DATA_DIRS,
                Arrays.asList(dataDir1.getAbsolutePath(), dataDir2.getAbsolutePath()));

        try (LocalDiskManager localDiskManager = LocalDiskManager.create(conf)) {
            assertThat(localDiskManager.dataDirs())
                    .containsExactly(dataDir1.getAbsoluteFile(), dataDir2.getAbsoluteFile());
            assertThat(fallbackDataDir).doesNotExist();
        }
    }

    @Test
    void testNormalizeConfiguredDataDirPaths() throws Exception {
        File normalizedDataDir = new File(tempDir, "data-1");
        Configuration normalizedConf =
                createConfFromPaths(new File(tempDir, "nested/../data-1").getPath());

        try (LocalDiskManager localDiskManager = LocalDiskManager.create(normalizedConf)) {
            assertThat(localDiskManager.dataDirs())
                    .containsExactly(normalizedDataDir.getAbsoluteFile());
        }
    }

    @Test
    void testRejectDuplicateAndOverlappingDirs() {
        // exact duplicate
        Configuration duplicateConf = new Configuration();
        duplicateConf.set(ConfigOptions.TABLET_SERVER_ID, TABLET_SERVER_ID);
        String sameDir = new File(tempDir, "same").getAbsolutePath();
        duplicateConf.set(ConfigOptions.DATA_DIRS, Arrays.asList(sameDir, sameDir));

        assertThatThrownBy(() -> LocalDiskManager.create(duplicateConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Duplicate local data directory");

        // normalized duplicate
        File normalizedDataDir = new File(tempDir, "data-1");
        Configuration normalizedDupConf =
                createConfFromPaths(
                        normalizedDataDir.getAbsolutePath(),
                        new File(tempDir, "nested/../data-1").getPath());
        assertThatThrownBy(() -> LocalDiskManager.create(normalizedDupConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Duplicate local data directory");

        // parent-child overlap
        File parent = new File(tempDir, "parent");
        Configuration overlapConf = new Configuration();
        overlapConf.set(ConfigOptions.TABLET_SERVER_ID, TABLET_SERVER_ID);
        overlapConf.set(
                ConfigOptions.DATA_DIRS,
                Arrays.asList(
                        parent.getAbsolutePath(), new File(parent, "child").getAbsolutePath()));

        assertThatThrownBy(() -> LocalDiskManager.create(overlapConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("must not overlap");

        // normalized overlap
        Configuration normalizedOverlapConf =
                createConfFromPaths(
                        new File(tempDir, "root").getAbsolutePath(),
                        new File(tempDir, "nested/../root/child").getPath());
        assertThatThrownBy(() -> LocalDiskManager.create(normalizedOverlapConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("must not overlap");
    }

    @Test
    void testDirectoryLocksPreventDoubleOwnership() throws Exception {
        File dataDir = new File(tempDir, "locked");
        Configuration conf = createConf(dataDir);

        try (LocalDiskManager ignored = LocalDiskManager.create(conf)) {
            assertThatThrownBy(() -> LocalDiskManager.create(conf))
                    .isInstanceOf(IllegalConfigurationException.class)
                    .hasMessageContaining("Failed to acquire lock on file .lock")
                    .hasMessageContaining("another process or thread is using this directory");
        }
    }

    @Test
    void testRejectDuplicateDiskIdsAcrossDataDirs() throws Exception {
        File dataDir1 = new File(tempDir, "data-1");
        File dataDir2 = new File(tempDir, "data-2");
        assertThat(dataDir1.mkdirs()).isTrue();
        assertThat(dataDir2.mkdirs()).isTrue();
        writeDiskProperties(dataDir1, "shared-disk-id", TABLET_SERVER_ID);
        writeDiskProperties(dataDir2, "shared-disk-id", TABLET_SERVER_ID);

        Configuration conf = createConf(dataDir1, dataDir2);

        assertThatThrownBy(() -> LocalDiskManager.create(conf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("duplicate disk.id");
        assertThat(new File(dataDir1, LocalDiskManager.LOCK_FILE_NAME)).doesNotExist();
        assertThat(new File(dataDir2, LocalDiskManager.LOCK_FILE_NAME)).doesNotExist();
    }

    @Test
    void testKeepHealthyDirWhenAnotherDirHasBrokenDiskProperties() throws Exception {
        File brokenDataDir = new File(tempDir, "broken-data");
        File healthyDataDir = new File(tempDir, "healthy-data");
        assertThat(brokenDataDir.mkdirs()).isTrue();
        assertThat(healthyDataDir.mkdirs()).isTrue();

        Properties properties = new Properties();
        properties.setProperty("version", "999");
        properties.setProperty("disk.id", "broken-disk-id");
        properties.setProperty("server.id", String.valueOf(TABLET_SERVER_ID));
        try (FileOutputStream outputStream =
                new FileOutputStream(
                        new File(brokenDataDir, LocalDiskManager.DISK_PROPERTIES_FILE_NAME))) {
            properties.store(outputStream, null);
        }

        Configuration conf = createConf(brokenDataDir, healthyDataDir);

        try (LocalDiskManager localDiskManager = LocalDiskManager.create(conf)) {
            assertThat(localDiskManager.dataDirs())
                    .containsExactly(healthyDataDir.getAbsoluteFile());
            assertThat(localDiskManager.diskId(healthyDataDir)).isNotBlank();
            assertThatThrownBy(() -> localDiskManager.diskId(brokenDataDir))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testKeepHealthyDirWhenAnotherDirCannotBeValidated() throws Exception {
        File badPath = new File(tempDir, "not-a-dir");
        assertThat(badPath.createNewFile()).isTrue();
        File healthyDataDir = new File(tempDir, "healthy-data");

        Configuration conf = createConf(badPath, healthyDataDir);

        try (LocalDiskManager localDiskManager = LocalDiskManager.create(conf)) {
            assertThat(localDiskManager.dataDirs())
                    .containsExactly(healthyDataDir.getAbsoluteFile());
            assertThat(new File(healthyDataDir, LocalDiskManager.DISK_PROPERTIES_FILE_NAME))
                    .exists();
            assertThat(new File(healthyDataDir, LocalDiskManager.LOCK_FILE_NAME)).exists();
        }
    }

    @Test
    void testSelectDataDirUsesConfiguredOrderAsTieBreaker() throws Exception {
        File dataDir1 = new File(tempDir, "data-1");
        File dataDir2 = new File(tempDir, "data-2");
        Configuration conf = createConf(dataDir2, dataDir1);

        try (LocalDiskManager localDiskManager = LocalDiskManager.create(conf)) {
            assertThat(localDiskManager.selectDataDirForNewBucket(false))
                    .isEqualTo(dataDir2.getAbsoluteFile());
            assertThat(localDiskManager.selectDataDirForNewBucket(true))
                    .isEqualTo(dataDir2.getAbsoluteFile());
        }
    }

    @Test
    void testSelectDataDirUsesSeparatedLogAndKvCounters() throws Exception {
        File dataDir1 = new File(tempDir, "data-1");
        File dataDir2 = new File(tempDir, "data-2");
        Configuration conf = createConf(dataDir1, dataDir2);

        try (LocalDiskManager localDiskManager = LocalDiskManager.create(conf)) {
            File logDataDir1 = localDiskManager.selectDataDirForNewBucket(false);
            assertThat(logDataDir1).isEqualTo(dataDir1.getAbsoluteFile());
            localDiskManager.recordReplicaLoad(logDataDir1, false);

            File logDataDir2 = localDiskManager.selectDataDirForNewBucket(false);
            assertThat(logDataDir2).isEqualTo(dataDir2.getAbsoluteFile());
            localDiskManager.recordReplicaLoad(logDataDir2, false);

            File kvDataDir1 = localDiskManager.selectDataDirForNewBucket(true);
            assertThat(kvDataDir1).isEqualTo(dataDir1.getAbsoluteFile());
            localDiskManager.recordReplicaLoad(kvDataDir1, true);

            File kvDataDir2 = localDiskManager.selectDataDirForNewBucket(true);
            assertThat(kvDataDir2).isEqualTo(dataDir2.getAbsoluteFile());

            assertThat(localDiskManager.logBucketCount(dataDir1)).isEqualTo(2);
            assertThat(localDiskManager.logBucketCount(dataDir2)).isEqualTo(1);
            assertThat(localDiskManager.kvBucketCount(dataDir1)).isEqualTo(1);
            assertThat(localDiskManager.kvBucketCount(dataDir2)).isEqualTo(0);
        }
    }

    @Test
    void testRejectMismatchedServerIdInDiskProperties() throws Exception {
        File dataDir = new File(tempDir, "data-1");
        assertThat(dataDir.mkdirs()).isTrue();
        writeDiskProperties(dataDir, "disk-1", TABLET_SERVER_ID + 1);

        assertThatThrownBy(() -> LocalDiskManager.create(createConf(dataDir)))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("does not match server.id");
    }

    @Test
    void testResolveDataDir() throws Exception {
        File dataDir1 = new File(tempDir, "data-1");
        File dataDir2 = new File(tempDir, "data-2");
        Configuration conf = createConf(dataDir1, dataDir2);

        try (LocalDiskManager localDiskManager = LocalDiskManager.create(conf)) {
            File childOfDir1 = new File(dataDir1, "db/table/bucket-0");
            assertThat(localDiskManager.resolveDataDir(childOfDir1))
                    .isEqualTo(dataDir1.getAbsoluteFile());

            File childOfDir2 = new File(dataDir2, "db/table/bucket-1");
            assertThat(localDiskManager.resolveDataDir(childOfDir2))
                    .isEqualTo(dataDir2.getAbsoluteFile());

            File unknownPath = new File(tempDir, "unknown/path");
            assertThatThrownBy(() -> localDiskManager.resolveDataDir(unknownPath))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("does not belong to any configured data directory");
        }
    }

    @Test
    void testRecordReplicaDeleteDecrementsCounters() throws Exception {
        File dataDir1 = new File(tempDir, "data-1");
        File dataDir2 = new File(tempDir, "data-2");
        Configuration conf = createConf(dataDir1, dataDir2);

        try (LocalDiskManager localDiskManager = LocalDiskManager.create(conf)) {
            // load some replicas
            localDiskManager.recordReplicaLoad(dataDir1.getAbsoluteFile(), false);
            localDiskManager.recordReplicaLoad(dataDir1.getAbsoluteFile(), true);
            localDiskManager.recordReplicaLoad(dataDir2.getAbsoluteFile(), false);

            assertThat(localDiskManager.logBucketCount(dataDir1)).isEqualTo(2);
            assertThat(localDiskManager.kvBucketCount(dataDir1)).isEqualTo(1);
            assertThat(localDiskManager.logBucketCount(dataDir2)).isEqualTo(1);

            // delete log-only replica
            localDiskManager.recordReplicaDelete(dataDir2.getAbsoluteFile(), false);
            assertThat(localDiskManager.logBucketCount(dataDir2)).isEqualTo(0);
            assertThat(localDiskManager.kvBucketCount(dataDir2)).isEqualTo(0);

            // delete PK replica
            localDiskManager.recordReplicaDelete(dataDir1.getAbsoluteFile(), true);
            assertThat(localDiskManager.logBucketCount(dataDir1)).isEqualTo(1);
            assertThat(localDiskManager.kvBucketCount(dataDir1)).isEqualTo(0);

            // ensure counts do not go below zero
            localDiskManager.recordReplicaDelete(dataDir2.getAbsoluteFile(), true);
            assertThat(localDiskManager.logBucketCount(dataDir2)).isEqualTo(0);
            assertThat(localDiskManager.kvBucketCount(dataDir2)).isEqualTo(0);
        }
    }

    @Test
    void testAllDirectoriesUnusableFailsStartup() throws Exception {
        // create two paths that are regular files, not directories
        File badPath1 = new File(tempDir, "bad-1");
        File badPath2 = new File(tempDir, "bad-2");
        assertThat(badPath1.createNewFile()).isTrue();
        assertThat(badPath2.createNewFile()).isTrue();

        Configuration conf = createConf(badPath1, badPath2);

        assertThatThrownBy(() -> LocalDiskManager.create(conf))
                .isInstanceOf(LogStorageException.class)
                .hasMessageContaining("None of the specified data dirs");
    }

    private Configuration createConf(File... dataDirs) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLET_SERVER_ID, TABLET_SERVER_ID);
        conf.set(
                ConfigOptions.DATA_DIRS,
                Arrays.stream(dataDirs).map(File::getAbsolutePath).collect(Collectors.toList()));
        return conf;
    }

    private Configuration createConfFromPaths(String... dataDirs) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLET_SERVER_ID, TABLET_SERVER_ID);
        conf.set(ConfigOptions.DATA_DIRS, Arrays.asList(dataDirs));
        return conf;
    }

    private String diskId(File dataDir) throws Exception {
        Properties properties = new Properties();
        try (FileInputStream inputStream =
                new FileInputStream(
                        new File(dataDir, LocalDiskManager.DISK_PROPERTIES_FILE_NAME))) {
            properties.load(inputStream);
        }
        assertThat(properties.getProperty("version")).isEqualTo("1");
        assertThat(properties.getProperty("server.id")).isEqualTo(String.valueOf(TABLET_SERVER_ID));
        return properties.getProperty("disk.id");
    }

    private void writeDiskProperties(File dataDir, String diskId, int serverId) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("version", "1");
        properties.setProperty("disk.id", diskId);
        properties.setProperty("server.id", String.valueOf(serverId));
        try (FileOutputStream outputStream =
                new FileOutputStream(
                        new File(dataDir, LocalDiskManager.DISK_PROPERTIES_FILE_NAME))) {
            properties.store(outputStream, null);
        }
    }
}
