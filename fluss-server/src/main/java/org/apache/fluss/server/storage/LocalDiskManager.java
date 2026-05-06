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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Manages the local data directories used by a TabletServer.
 *
 * <p>The manager has two responsibilities:
 *
 * <ol>
 *   <li>During startup, validate configured data directories, recover or create {@code
 *       disk.properties}, and hold a {@code .lock} file for every live directory.
 *   <li>After startup, provide runtime helpers for path resolution and simple per-disk load
 *       accounting used by replica placement.
 * </ol>
 *
 * <p>Thread-safety: initialization happens before the manager is published to other components.
 * After that, the mutable state is limited to in-memory bucket counters and shutdown cleanup. The
 * public runtime APIs synchronize on the manager instance so directory lookups, counter updates,
 * and {@link #close()} observe a consistent view.
 */
public final class LocalDiskManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDiskManager.class);

    public static final String DISK_PROPERTIES_FILE_NAME = "disk.properties";
    public static final String LOCK_FILE_NAME = ".lock";
    private static final int DISK_PROPERTIES_VERSION = 1;
    private static final String VERSION_KEY = "version";
    private static final String DISK_ID_KEY = "disk.id";
    private static final String SERVER_ID_KEY = "server.id";

    private final int serverId;
    private final List<File> dataDirs;
    private final Map<File, DiskInfo> diskInfosByDir = new LinkedHashMap<>();
    private final Map<File, DirectoryLock> directoryLocks = new LinkedHashMap<>();

    // ------------------------------------------------------------------------
    // Initialization
    // ------------------------------------------------------------------------

    private LocalDiskManager(Configuration conf) throws IOException {
        this.serverId = conf.getInt(ConfigOptions.TABLET_SERVER_ID);
        List<File> configuredDataDirs = resolveConfiguredDataDirs(conf);
        List<File> initializedDataDirs;
        try {
            initializedDataDirs = initializeDataDirs(configuredDataDirs);
        } catch (Throwable throwable) {
            close();
            throw throwable;
        }
        this.dataDirs = Collections.unmodifiableList(initializedDataDirs);
    }

    public static LocalDiskManager create(Configuration conf) throws IOException {
        LocalDiskManager localDiskManager = new LocalDiskManager(conf);
        LOG.info(
                "Initialized LocalDiskManager with local data directories {}.",
                localDiskManager.dataDirs);
        return localDiskManager;
    }

    /**
     * Resolves {@code data.dirs} or falls back to {@code data.dir}, then normalizes every path
     * exactly once.
     *
     * <p>The method rejects:
     *
     * <ul>
     *   <li>empty values, for example {@code ["/data-0", " "]}
     *   <li>duplicate directories, for example {@code ["/data-0", "/data-0"]}
     *   <li>overlapping directories, for example {@code ["/data", "/data/table-1"]}
     * </ul>
     *
     * <p>Overlapping directories are rejected because they do not describe independent storage
     * roots and would make placement and cleanup ambiguous.
     */
    private static List<File> resolveConfiguredDataDirs(Configuration conf) {
        List<String> configuredDataDirs = conf.get(ConfigOptions.DATA_DIRS);
        if (configuredDataDirs == null || configuredDataDirs.isEmpty()) {
            configuredDataDirs = Collections.singletonList(conf.get(ConfigOptions.DATA_DIR));
        }

        List<File> dataDirs = new ArrayList<>(configuredDataDirs.size());
        for (String configuredDataDir : configuredDataDirs) {
            if (configuredDataDir == null || configuredDataDir.trim().isEmpty()) {
                throw new IllegalConfigurationException(
                        "Configured local data directories must not contain empty values.");
            }
            File dataDir =
                    new File(configuredDataDir).toPath().toAbsolutePath().normalize().toFile();
            for (File existingDataDir : dataDirs) {
                if (existingDataDir.equals(dataDir)) {
                    throw new IllegalConfigurationException(
                            "Duplicate local data directory configured: " + dataDir);
                }

                Path existingPath = existingDataDir.toPath();
                Path candidatePath = dataDir.toPath();
                if (existingPath.startsWith(candidatePath)
                        || candidatePath.startsWith(existingPath)) {
                    throw new IllegalConfigurationException(
                            "Configured local data directories must not overlap: "
                                    + existingPath
                                    + " and "
                                    + candidatePath);
                }
            }
            dataDirs.add(dataDir);
        }
        return dataDirs;
    }

    /**
     * Initializes all configured data directories and returns the directories that remain online
     * after validation.
     *
     * <p>The initialization pipeline keeps dropping bad directories from {@code onlineDataDirs}. A
     * directory can be removed at any step if it fails metadata loading, local directory
     * validation, locking, or checkpoint writing. The remaining directories become the runtime
     * storage roots.
     */
    private List<File> initializeDataDirs(List<File> configuredDataDirs) throws IOException {
        Set<File> onlineDataDirs = new LinkedHashSet<>(configuredDataDirs);
        Set<File> offlineDataDirs = new LinkedHashSet<>();

        // 1. Create disk.properties checkpoints for all configured data directories.
        Map<File, DiskPropertiesCheckpoint> diskPropertiesCheckpoints =
                newDiskPropertiesCheckpoints(configuredDataDirs);
        // 2. Load persisted disk metadata from disk.properties.
        Map<File, DiskProperties> diskPropertiesMap =
                loadExistingDiskProperties(
                        onlineDataDirs, offlineDataDirs, diskPropertiesCheckpoints);
        // 3. Create and validate the configured data directories.
        createAndValidateDataDirs(onlineDataDirs, offlineDataDirs);
        // 4. Acquire .lock for all directories that are still online.
        lockDataDirs(onlineDataDirs, offlineDataDirs);
        // 5. Assign disk ids and write disk.properties for directories that do not have it yet.
        initializeMissingDiskProperties(
                onlineDataDirs, offlineDataDirs, diskPropertiesMap, diskPropertiesCheckpoints);
        // 6. Fail fast if every configured directory has been dropped during initialization.
        validateNonEmptyDataDirs(configuredDataDirs, onlineDataDirs);
        // 7. Publish the runtime disk state used by placement and path resolution.
        finishDiskInfos(onlineDataDirs, diskPropertiesMap);

        return new ArrayList<>(onlineDataDirs);
    }

    private static Map<File, DiskPropertiesCheckpoint> newDiskPropertiesCheckpoints(
            List<File> configuredDataDirs) {
        Map<File, DiskPropertiesCheckpoint> checkpoints = new LinkedHashMap<>();
        for (File dataDir : configuredDataDirs) {
            checkpoints.put(
                    dataDir,
                    new DiskPropertiesCheckpoint(new File(dataDir, DISK_PROPERTIES_FILE_NAME)));
        }
        return checkpoints;
    }

    private Map<File, DiskProperties> loadExistingDiskProperties(
            Set<File> onlineDataDirs,
            Set<File> offlineDataDirs,
            Map<File, DiskPropertiesCheckpoint> diskPropertiesCheckpoints) {
        Map<File, DiskProperties> diskPropertiesMap = new LinkedHashMap<>();
        Set<Integer> persistedServerIdSet = new LinkedHashSet<>();
        Map<String, File> diskIdToDataDir = new LinkedHashMap<>();

        for (File dataDir : new ArrayList<>(onlineDataDirs)) {
            try {
                DiskProperties diskProperties = diskPropertiesCheckpoints.get(dataDir).read();
                if (diskProperties != null) {
                    int persistedServerId = diskProperties.serverIdAsInt(dataDir);
                    String persistedDiskId = diskProperties.diskId(dataDir);
                    diskPropertiesMap.put(dataDir, diskProperties);
                    persistedServerIdSet.add(persistedServerId);

                    File previousDataDir = diskIdToDataDir.putIfAbsent(persistedDiskId, dataDir);
                    if (previousDataDir != null) {
                        throw new IllegalConfigurationException(
                                "Configured local data directories contain duplicate disk.id "
                                        + persistedDiskId
                                        + " for "
                                        + previousDataDir
                                        + " and "
                                        + dataDir);
                    }
                }
            } catch (IOException e) {
                markDataDirOffline(
                        dataDir,
                        onlineDataDirs,
                        offlineDataDirs,
                        "Failed to read disk.properties for data directory " + dataDir,
                        e);
            }
        }

        if (persistedServerIdSet.size() > 1) {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<File, DiskProperties> entry : diskPropertiesMap.entrySet()) {
                builder.append("- ")
                        .append(entry.getKey())
                        .append(" -> ")
                        .append(entry.getValue())
                        .append('\n');
            }
            throw new IllegalConfigurationException(
                    "disk.properties is not consistent across data.dirs. This could happen if "
                            + "multiple tablet servers shared a data directory (data.dirs) or partial data was "
                            + "manually copied from another server. Found:\n"
                            + builder);
        } else if (persistedServerIdSet.size() == 1 && !persistedServerIdSet.contains(serverId)) {
            int persistedServerId = persistedServerIdSet.iterator().next();
            throw new IllegalConfigurationException(
                    "Configured tablet server id "
                            + serverId
                            + " does not match server.id "
                            + persistedServerId
                            + " in disk.properties. If you moved your data, make sure the "
                            + "configured tablet server id matches. If you intend to create a "
                            + "new tablet server, remove all data under your data directories "
                            + "(data.dirs).");
        }

        return diskPropertiesMap;
    }

    private void createAndValidateDataDirs(Set<File> onlineDataDirs, Set<File> offlineDataDirs) {
        for (File dataDir : new ArrayList<>(onlineDataDirs)) {
            try {
                if (!dataDir.exists()) {
                    LOG.info(
                            "Data directory {} not found, creating it.", dataDir.getAbsolutePath());
                    if (!dataDir.mkdirs()) {
                        throw new IOException(
                                "Failed to create data directory " + dataDir.getAbsolutePath());
                    }
                    Path parentPath = dataDir.toPath().toAbsolutePath().normalize().getParent();
                    FileUtils.flushDir(parentPath);
                }
                if (!dataDir.isDirectory() || !dataDir.canRead()) {
                    throw new IOException(
                            dataDir.getAbsolutePath() + " is not a readable data directory.");
                }
            } catch (IOException e) {
                markDataDirOffline(
                        dataDir,
                        onlineDataDirs,
                        offlineDataDirs,
                        "Failed to create or validate data directory " + dataDir,
                        e);
            }
        }
    }

    private void lockDataDirs(Set<File> onlineDataDirs, Set<File> offlineDataDirs) {
        for (File dataDir : new ArrayList<>(onlineDataDirs)) {
            try {
                DirectoryLock directoryLock = new DirectoryLock(new File(dataDir, LOCK_FILE_NAME));
                if (!directoryLock.tryLock()) {
                    closeDirectoryLock(directoryLock);
                    // Only real disk/I/O failures should be downgraded to offline data dirs.
                    // A lock conflict means another Fluss process/thread already owns this
                    // directory, so startup should fail fast instead of silently skipping it.
                    throw new IllegalConfigurationException(
                            "Failed to acquire lock on file "
                                    + LOCK_FILE_NAME
                                    + " in "
                                    + directoryLock.file.getParent()
                                    + ". A Fluss tablet server instance in another process or "
                                    + "thread is using this directory.");
                }
                directoryLocks.put(dataDir, directoryLock);
            } catch (IOException e) {
                markDataDirOffline(
                        dataDir,
                        onlineDataDirs,
                        offlineDataDirs,
                        "Failed to acquire lock for data directory " + dataDir,
                        e);
            }
        }
    }

    private void initializeMissingDiskProperties(
            Set<File> onlineDataDirs,
            Set<File> offlineDataDirs,
            Map<File, DiskProperties> diskPropertiesMap,
            Map<File, DiskPropertiesCheckpoint> diskPropertiesCheckpoints) {

        // 1. Find directories that are live but do not have persisted local metadata yet.
        Set<File> dataDirsWithoutDiskProperties = new LinkedHashSet<>();
        for (File dataDir : onlineDataDirs) {
            if (!diskPropertiesMap.containsKey(dataDir)) {
                dataDirsWithoutDiskProperties.add(dataDir);
            }
        }
        // 2. Collect disk ids that are already persisted so we do not assign duplicates locally.
        Set<String> usedDiskIds = new LinkedHashSet<>();
        for (DiskProperties diskProperties : diskPropertiesMap.values()) {
            usedDiskIds.add(diskProperties.diskId);
        }
        // 3. Create the missing metadata in memory first.
        for (File dataDir : dataDirsWithoutDiskProperties) {
            diskPropertiesMap.put(
                    dataDir,
                    new DiskProperties(
                            DISK_PROPERTIES_VERSION,
                            newDiskId(usedDiskIds),
                            String.valueOf(serverId)));
        }
        // 4. Persist the missing metadata. A write failure makes the directory unusable for this
        // startup, so the directory is unlocked and moved to offlineDataDirs.
        for (File dataDir : new ArrayList<>(dataDirsWithoutDiskProperties)) {
            DiskProperties diskProperties = diskPropertiesMap.get(dataDir);
            try {
                diskPropertiesCheckpoints.get(dataDir).write(diskProperties);
            } catch (IOException e) {
                closeDirectoryLock(dataDir);
                diskPropertiesMap.remove(dataDir);
                markDataDirOffline(
                        dataDir,
                        onlineDataDirs,
                        offlineDataDirs,
                        "Failed to checkpoint disk.properties for data directory " + dataDir,
                        e);
            }
        }
    }

    private void validateNonEmptyDataDirs(List<File> configuredDataDirs, Set<File> onlineDataDirs) {
        if (onlineDataDirs.isEmpty()) {
            throw new LogStorageException(
                    "None of the specified data dirs from "
                            + configuredDataDirs
                            + " can be created, locked or checkpointed");
        }
    }

    private void finishDiskInfos(
            Set<File> onlineDataDirs, Map<File, DiskProperties> diskPropertiesMap) {
        for (File dataDir : onlineDataDirs) {
            DiskProperties diskProperties = diskPropertiesMap.get(dataDir);
            diskInfosByDir.put(dataDir, new DiskInfo(dataDir, diskProperties.diskId));
        }
    }

    // ------------------------------------------------------------------------
    // Public APIs
    // ------------------------------------------------------------------------

    public List<File> dataDirs() {
        return dataDirs;
    }

    /**
     * Resolves a local path back to the configured data directory that owns it.
     *
     * <p>Example: if {@code /data-0} is configured and the path is {@code /data-0/db/tablet-1}, the
     * returned directory is {@code /data-0}.
     */
    public synchronized File resolveDataDir(File path) {
        Path pathToResolve = path.toPath().toAbsolutePath().normalize();
        for (File dataDir : dataDirs) {
            if (pathToResolve.startsWith(dataDir.toPath())) {
                return dataDir;
            }
        }

        throw new IllegalArgumentException(
                "Path " + path + " does not belong to any configured data directory.");
    }

    /**
     * Selects a target directory for a new bucket replica.
     *
     * <p>Log-table buckets use {@code logBucketCount}. Primary-key table buckets use {@code
     * kvBucketCount} so KV-heavy workloads are balanced independently from pure log workloads.
     */
    public synchronized File selectDataDirForNewBucket(boolean hasPrimaryKey) {
        DiskInfo selectedDisk = null;
        Comparator<DiskInfo> comparator =
                hasPrimaryKey
                        ? Comparator.comparingInt((DiskInfo diskInfo) -> diskInfo.kvBucketCount)
                        : Comparator.comparingInt((DiskInfo diskInfo) -> diskInfo.logBucketCount);
        for (DiskInfo candidate : diskInfosByDir.values()) {
            if (selectedDisk == null || comparator.compare(candidate, selectedDisk) < 0) {
                selectedDisk = candidate;
            }
        }
        if (selectedDisk == null) {
            throw new IllegalStateException("No configured data directory is available.");
        }
        return selectedDisk.dataDir;
    }

    public synchronized void recordReplicaLoad(File dataDir, boolean hasPrimaryKey) {
        DiskInfo diskInfo = diskInfo(dataDir);
        diskInfo.logBucketCount++;
        if (hasPrimaryKey) {
            diskInfo.kvBucketCount++;
        }
    }

    public synchronized void recordReplicaDelete(File dataDir, boolean hasPrimaryKey) {
        DiskInfo diskInfo = diskInfo(dataDir);
        diskInfo.logBucketCount = Math.max(0, diskInfo.logBucketCount - 1);
        if (hasPrimaryKey) {
            diskInfo.kvBucketCount = Math.max(0, diskInfo.kvBucketCount - 1);
        }
    }

    @VisibleForTesting
    synchronized String diskId(File dataDir) {
        return diskInfo(dataDir).diskId;
    }

    @VisibleForTesting
    synchronized int logBucketCount(File dataDir) {
        return diskInfo(dataDir).logBucketCount;
    }

    @VisibleForTesting
    synchronized int kvBucketCount(File dataDir) {
        return diskInfo(dataDir).kvBucketCount;
    }

    @Override
    public synchronized void close() {
        for (DirectoryLock directoryLock : directoryLocks.values()) {
            closeDirectoryLock(directoryLock);
        }
        directoryLocks.clear();
        diskInfosByDir.clear();
    }

    // ------------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------------

    private void markDataDirOffline(
            File dataDir,
            Set<File> onlineDataDirs,
            Set<File> offlineDataDirs,
            String message,
            Exception exception) {
        onlineDataDirs.remove(dataDir);
        offlineDataDirs.add(dataDir);
        if (exception == null) {
            LOG.error(message);
        } else {
            LOG.error(message, exception);
        }
    }

    private static String newDiskId(Set<String> usedDiskIds) {
        String diskId;
        do {
            diskId = UUID.randomUUID().toString();
        } while (!usedDiskIds.add(diskId));
        return diskId;
    }

    private DiskInfo diskInfo(File dataDir) {
        DiskInfo diskInfo = diskInfosByDir.get(dataDir);
        if (diskInfo == null) {
            throw new IllegalArgumentException("Unknown data directory: " + dataDir);
        }
        return diskInfo;
    }

    private void closeDirectoryLock(File dataDir) {
        DirectoryLock directoryLock = directoryLocks.remove(dataDir);
        if (directoryLock != null) {
            closeDirectoryLock(directoryLock);
        }
    }

    private void closeDirectoryLock(DirectoryLock directoryLock) {
        try {
            directoryLock.destroy();
        } catch (IOException e) {
            LOG.warn(
                    "Failed to close directory lock for {}", directoryLock.file.getParentFile(), e);
        }
    }

    // ------------------------------------------------------------------------
    // Core data objects
    // ------------------------------------------------------------------------

    /** Parsed contents of a {@code disk.properties} file. */
    private static final class DiskProperties {
        private final int version;
        private final String diskId;
        private final String serverId;

        private DiskProperties(int version, String diskId, String serverId) {
            this.version = version;
            this.diskId = diskId == null ? null : diskId.trim();
            this.serverId = serverId == null ? null : serverId.trim();
        }

        private int serverIdAsInt(File dataDir) throws IOException {
            if (serverId == null || serverId.isEmpty()) {
                throw new IOException(
                        "Missing server.id in disk.properties under " + dataDir.getAbsolutePath());
            }

            try {
                return Integer.parseInt(serverId);
            } catch (NumberFormatException e) {
                throw new IOException(
                        "Invalid server.id in disk.properties under "
                                + dataDir.getAbsolutePath()
                                + ": "
                                + serverId,
                        e);
            }
        }

        private String diskId(File dataDir) throws IOException {
            if (diskId == null || diskId.isEmpty()) {
                throw new IOException(
                        "Missing disk.id in disk.properties under " + dataDir.getAbsolutePath());
            }
            return diskId;
        }

        @Override
        public String toString() {
            return "DiskProperties(version="
                    + version
                    + ", diskId="
                    + diskId
                    + ", serverId="
                    + serverId
                    + ")";
        }
    }

    /** Read/write helper for a single {@code disk.properties} file. */
    private static final class DiskPropertiesCheckpoint {
        private final File file;
        private final Object lock = new Object();

        private DiskPropertiesCheckpoint(File file) {
            this.file = file;
        }

        private void write(DiskProperties diskProperties) throws IOException {
            synchronized (lock) {
                try {
                    File tempFile = new File(file.getAbsolutePath() + ".tmp");
                    FileOutputStream outputStream = new FileOutputStream(tempFile);
                    try {
                        Properties properties = new Properties();
                        properties.setProperty(VERSION_KEY, String.valueOf(diskProperties.version));
                        properties.setProperty(DISK_ID_KEY, diskProperties.diskId);
                        properties.setProperty(SERVER_ID_KEY, diskProperties.serverId);
                        properties.store(outputStream, "");
                        outputStream.flush();
                        outputStream.getFD().sync();
                    } finally {
                        IOUtils.closeQuietly(outputStream, tempFile.getName());
                    }
                    FileUtils.atomicMoveWithFallback(tempFile.toPath(), file.toPath(), false);
                } catch (IOException e) {
                    LOG.error("Failed to write disk.properties due to", e);
                    throw e;
                }
            }
        }

        private DiskProperties read() throws IOException {
            Files.deleteIfExists(new File(file.getPath() + ".tmp").toPath());

            synchronized (lock) {
                InputStream inputStream = null;
                try {
                    inputStream = Files.newInputStream(file.toPath());
                    Properties properties = new Properties();
                    properties.load(inputStream);

                    String version = properties.getProperty(VERSION_KEY);
                    if (version == null) {
                        throw new IOException(
                                "Unrecognized version of disk.properties file under "
                                        + file.getAbsolutePath()
                                        + ": null");
                    }

                    int parsedVersion;
                    try {
                        parsedVersion = Integer.parseInt(version);
                    } catch (NumberFormatException e) {
                        throw new IOException(
                                "Unrecognized version of disk.properties file under "
                                        + file.getAbsolutePath()
                                        + ": "
                                        + version,
                                e);
                    }

                    if (parsedVersion != DISK_PROPERTIES_VERSION) {
                        throw new IOException(
                                "Unrecognized version of disk.properties file under "
                                        + file.getAbsolutePath()
                                        + ": "
                                        + version);
                    }

                    String diskId = properties.getProperty(DISK_ID_KEY);
                    String serverId = properties.getProperty(SERVER_ID_KEY);
                    return new DiskProperties(parsedVersion, diskId, serverId);
                } catch (NoSuchFileException e) {
                    LOG.info("No disk.properties file under dir {}", file.getAbsolutePath());
                    return null;
                } catch (Exception e) {
                    LOG.error(
                            "Failed to read disk.properties file under dir {} due to {}",
                            file.getAbsolutePath(),
                            e.getMessage());
                    if (e instanceof IOException) {
                        throw (IOException) e;
                    }
                    throw new IOException(
                            "Failed to read disk.properties file under dir "
                                    + file.getAbsolutePath(),
                            e);
                } finally {
                    IOUtils.closeQuietly(inputStream, file.getName());
                }
            }
        }
    }

    /** Holds the {@code .lock} file for one live data directory. */
    private static final class DirectoryLock {
        private final File file;
        private final FileChannel channel;
        private FileLock flock;

        private DirectoryLock(File file) throws IOException {
            this(
                    file,
                    FileChannel.open(
                            file.toPath(),
                            StandardOpenOption.CREATE,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE),
                    null);
        }

        private DirectoryLock(File file, FileChannel channel, FileLock flock) {
            this.file = file;
            this.channel = channel;
            this.flock = flock;
        }

        private synchronized void lock() throws IOException {
            flock = channel.lock();
        }

        private synchronized boolean tryLock() throws IOException {
            try {
                flock = channel.tryLock();
                return flock != null;
            } catch (OverlappingFileLockException e) {
                return false;
            }
        }

        private synchronized void unlock() throws IOException {
            if (flock != null) {
                flock.release();
            }
        }

        private synchronized void destroy() throws IOException {
            unlock();
            channel.close();
        }
    }

    /** Runtime state for one online data directory. */
    private static final class DiskInfo {
        private final File dataDir;
        private final String diskId;
        private int logBucketCount;
        private int kvBucketCount;

        private DiskInfo(File dataDir, String diskId) {
            this.dataDir = dataDir;
            this.diskId = diskId;
        }
    }
}
