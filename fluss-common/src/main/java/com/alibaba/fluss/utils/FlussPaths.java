/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.utils.types.Tuple2;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.UUID;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * Central place for defining all the paths of kv and log local/remote files/directories.
 *
 * <p>All the local file/directories returns the {@link File java.io.File} interface.
 *
 * <p>All the remote file/directories returns the {@link FsPath com.alibaba.fs.FsPath} interface.
 */
public class FlussPaths {

    /** Prefix of a local log tablet directory to store log files for a specific log tablet. */
    public static final String LOG_TABLET_DIR_PREFIX = "log-";

    /** Prefix of a local kv tablet directory to store kv files for a specific kv tablet. */
    public static final String KV_TABLET_DIR_PREFIX = "kv-";

    /** Prefix for a partition id to distinguish between table id and partition id. */
    public static final String PARTITION_DIR_PREFIX = "p";

    /** Suffix of a log file. */
    public static final String LOG_FILE_SUFFIX = ".log";

    /** Suffix of an index file. */
    public static final String INDEX_FILE_SUFFIX = ".index";

    /** Suffix of a time index file. */
    public static final String TIME_INDEX_FILE_SUFFIX = ".timeindex";

    /** Suffix of a writer snapshot file. */
    public static final String WRITER_SNAPSHOT_FILE_SUFFIX = ".writer_snapshot";

    /** The directory name for storing remote log index files. */
    public static final String REMOTE_LOG_INDEX_LOCAL_CACHE = "remote-log-index-cache";

    /** The directory name for storing remote log files. */
    public static final String REMOTE_LOG_DIR_NAME = "log";

    /** The directory name for storing metadata files (e.g., manifest) for a log tablet. */
    private static final String REMOTE_LOG_METADATA_DIR_NAME = "metadata";

    /** Suffix of a manifest file. */
    private static final String REMOTE_LOG_MANIFEST_FILE_SUFFIX = ".manifest";

    /** Suffix for a file that is scheduled to be deleted. */
    public static final String DELETED_FILE_SUFFIX = ".deleted";

    /** The directory name for storing remote kv snapshot files. */
    public static final String REMOTE_KV_DIR_NAME = "kv";

    /** The prefix of directory containing the remote kv snapshot data exclusive to a snapshot. */
    public static final String REMOTE_KV_SNAPSHOT_DIR_PREFIX = "snap-";

    /** The name of the directory for shared remote snapshot kv files. */
    public static final String REMOTE_KV_SNAPSHOT_SHARED_DIR = "shared";

    // ----------------------------------------------------------------------------------------
    // LOG/KV Tablet Paths
    // ----------------------------------------------------------------------------------------

    /**
     * Returns the local directory path for storing log files for a log tablet.
     *
     * <p>The path contract:
     *
     * <pre>
     * Non-Partitioned Table:
     * {$data.dir}/{databaseName}/{tableName}-{tableId}/log-{bucket}
     *
     * Partitioned Table:
     * {$data.dir}/{databaseName}/{tableName}-{tableId}/{partitionName}-p{partitionId}/log-{bucket}
     * </pre>
     *
     * @param dataDir the local data root directory, i.e. the "data.dir" in the configuration
     */
    public static File logTabletDir(
            File dataDir, PhysicalTablePath tablePath, TableBucket tableBucket) {
        final Path tabletParentDir = tabletParentDir(dataDir, tablePath, tableBucket);
        return tabletParentDir.resolve(LOG_TABLET_DIR_PREFIX + tableBucket.getBucket()).toFile();
    }

    /**
     * Returns the local directory path for storing kv files for a kv tablet.
     *
     * <p>The path contract:
     *
     * <pre>
     * Non-Partitioned Table:
     * {$data.dir}/{databaseName}/{tableName}-{tableId}/kv-{bucket}
     *
     * Partitioned Table:
     * {$data.dir}/{databaseName}/{tableName}-{tableId}/{partitionName}-p{partitionId}/kv-{bucket}
     * </pre>
     *
     * @param dataDir the local data root directory, i.e. the "data.dir" in the configuration
     */
    public static File kvTabletDir(
            File dataDir, PhysicalTablePath tablePath, TableBucket tableBucket) {
        final Path tabletParentDir = tabletParentDir(dataDir, tablePath, tableBucket);
        return tabletParentDir.resolve(KV_TABLET_DIR_PREFIX + tableBucket.getBucket()).toFile();
    }

    private static Path tabletParentDir(
            File dataDir, PhysicalTablePath tablePath, TableBucket tableBucket) {
        String dbName = tablePath.getDatabaseName();
        Path tableDir =
                Paths.get(
                        dataDir.getAbsolutePath(),
                        dbName,
                        tablePath.getTableName() + "-" + tableBucket.getTableId());
        if (tablePath.getPartitionName() == null) {
            return tableDir;
        } else {
            checkNotNull(
                    tableBucket.getPartitionId(),
                    "partition id shouldn't be null for partitioned table");
            return tableDir.resolve(
                    tablePath.getPartitionName()
                            + "-"
                            + PARTITION_DIR_PREFIX
                            + tableBucket.getPartitionId());
        }
    }

    /**
     * Parse the table path, optional partition name and bucket id from the given (log/kv) tablet
     * directory.
     *
     * <p>See {@link #logTabletDir(File, PhysicalTablePath, TableBucket)} and {@link
     * #kvTabletDir(File, PhysicalTablePath, TableBucket)} for the contracts of the directory
     * structure.
     *
     * @return tuple2 of (physical table path, table bucket), if the tablet is not for a partition,
     *     the partition name will be null.
     */
    public static Tuple2<PhysicalTablePath, TableBucket> parseTabletDir(File tabletDir) {
        checkNotNull(tabletDir, "tabletDir should not be null");

        final String partitionName;
        final Long partitionId;
        final File tableDir;

        // may be a tablet for a partition, get the parent directory
        // check whether it matches the pattern for a partition
        String tabletParentDirName = tabletDir.getParentFile().getName();
        if (isPartitionDir(tabletParentDirName)) {
            // tabletParentDirName should be {partitionName}-p{partitionId}
            int splitIndex = tabletParentDirName.lastIndexOf('-');
            String lastSplit = tabletParentDirName.substring(splitIndex + 1);
            partitionName = tabletParentDirName.substring(0, splitIndex);
            partitionId = Long.parseLong(lastSplit.substring(PARTITION_DIR_PREFIX.length()));
            // for partitioned tables, the parent of the partition dir is the table dir
            tableDir = tabletDir.getParentFile().getParentFile();
        } else {
            // for non-partitioned tables, the parent of the tablet dir is the table dir
            tableDir = tabletDir.getParentFile();
            partitionName = null;
            partitionId = null;
        }

        // Get path with db name.
        String dbDirName = tableDir.getParentFile().getName();
        // Get path with table name and table id.
        String tableDirName = tableDir.getName();
        checkState(
                tableDirName.contains("-"),
                "Found table directory '%s' is not in form of '{tableName}-{tableId}'.",
                tableDirName);
        String tableName;
        long tableId;
        int splitIndex = tableDirName.lastIndexOf("-");
        try {
            tableId = Long.parseLong(tableDirName.substring(splitIndex + 1));
            tableName = tableDirName.substring(0, splitIndex);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid table id in table directory path: " + tableDirName, e);
        }

        // Get path with bucket id.
        String tabletDirName = tabletDir.getName();
        String[] bucketIdSplit = tabletDirName.split("-");
        checkState(
                tabletDirName.startsWith(LOG_TABLET_DIR_PREFIX)
                        || tabletDirName.startsWith(KV_TABLET_DIR_PREFIX),
                "Found tablet directory '%s' is not in form of 'log-{bucket}' or 'kv-{bucket}'.",
                tabletDirName);
        checkState(
                bucketIdSplit.length == 2,
                "Found tablet directory '%s' is not in form of 'log-{bucket}' or 'kv-{bucket}'.",
                tabletDirName);
        int bucketId;
        try {
            bucketId = Integer.parseInt(bucketIdSplit[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid bucket id in tablet directory path: " + tabletDirName, e);
        }

        return Tuple2.of(
                PhysicalTablePath.of(dbDirName, tableName, partitionName),
                new TableBucket(tableId, partitionId, bucketId));
    }

    public static boolean isPartitionDir(String dirName) {
        int splitIndex = dirName.lastIndexOf('-');
        return splitIndex >= 0
                && dirName.substring(splitIndex + 1).startsWith(PARTITION_DIR_PREFIX);
    }

    // ----------------------------------------------------------------------------------------
    // Local Log Files
    // ----------------------------------------------------------------------------------------

    /**
     * Construct a log file name in the given dir with the given base offset and the given suffix.
     *
     * @param logTabletDir The log tablet directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File logFile(File logTabletDir, long offset) {
        return new File(logTabletDir, filenamePrefixFromOffset(offset) + LOG_FILE_SUFFIX);
    }

    /**
     * Construct an index file name in the given dir using the given base offset.
     *
     * @param dir The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File offsetIndexFile(File dir, long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + INDEX_FILE_SUFFIX);
    }

    /**
     * Returns the offset from the given file. The file name is of the form: {number}.{suffix}. This
     * method extracts the number from the given file's name.
     *
     * @param file file with the offset information as part of its name.
     * @return offset of the given file
     */
    public static Long offsetFromFile(File file) {
        return offsetFromFileName(file.getName());
    }

    /**
     * Returns the offset for the given file name. The file name is of the form: {number}.{suffix}.
     * This method extracts the number from the given file name.
     *
     * @param fileName name of the file
     * @return offset of the given file name
     */
    public static long offsetFromFileName(String fileName) {
        return Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
    }

    /**
     * Construct a time index file name in the given dir using the given base offset.
     *
     * @param dir The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File timeIndexFile(File dir, long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + TIME_INDEX_FILE_SUFFIX);
    }

    /**
     * Returns a File instance with parent directory as logDir and the file name as writer snapshot
     * file for the given offset.
     *
     * @param logTabletDir The log tablet directory in which the log will reside
     * @param offset The last offset (exclusive) included in the snapshot
     * @return a File instance for producer snapshot.
     */
    public static File writerSnapshotFile(File logTabletDir, long offset) {
        return new File(
                logTabletDir, filenamePrefixFromOffset(offset) + WRITER_SNAPSHOT_FILE_SUFFIX);
    }

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with
     * zeros so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     */
    public static String filenamePrefixFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    // ----------------------------------------------------------------------------------------
    // Remote Index Local Cache
    // ----------------------------------------------------------------------------------------

    /**
     * Returns the local cache directory for storing remote log index files.
     *
     * <p>The path contract:
     *
     * <pre>
     * {$data.dir}/remote-log-index-cache
     * </pre>
     *
     * @param dataDir the local data root directory, i.e. the "data.dir" in the configuration.
     */
    public static File remoteLogIndexCacheDir(File dataDir) {
        return new File(dataDir, REMOTE_LOG_INDEX_LOCAL_CACHE);
    }

    /**
     * Returns the local file for storing the remote log offset index file.
     *
     * <p>The path contract:
     *
     * <pre>
     * {$data.dir}/remote-log-index-cache/{baseOffset}_{segmentId}.index
     * </pre>
     *
     * @param cacheDir see {@link #remoteLogIndexCacheDir(File)}.
     * @param segment the remote log segment for the offset index.
     */
    public static File remoteOffsetIndexCacheFile(File cacheDir, RemoteLogSegment segment) {
        String prefix = segment.remoteLogStartOffset() + "_" + segment.remoteLogSegmentId();
        return new File(cacheDir, prefix + INDEX_FILE_SUFFIX);
    }

    /**
     * Returns the local file for storing the remote log time index file.
     *
     * <p>The path contract:
     *
     * <pre>
     * {$data.dir}/remote-log-index-cache/{baseOffset}_{segmentId}.timeindex
     * </pre>
     *
     * @param cacheDir see {@link #remoteLogIndexCacheDir(File)}.
     * @param segment the remote log segment for the offset index.
     */
    public static File remoteTimeIndexCacheFile(File cacheDir, RemoteLogSegment segment) {
        String prefix = segment.remoteLogStartOffset() + "_" + segment.remoteLogSegmentId();
        return new File(cacheDir, prefix + TIME_INDEX_FILE_SUFFIX);
    }

    /**
     * Extracts the base offset of the log segment from the path of the remote log index cache file,
     * see {@link #remoteOffsetIndexCacheFile(File, RemoteLogSegment)}.
     */
    public static long offsetFromRemoteIndexCacheFileName(String fileName) {
        return Long.parseLong(fileName.substring(0, fileName.indexOf('_')));
    }

    /**
     * Extracts the UUID of the log segment from the path of the remote log index cache file, see
     * {@link #remoteOffsetIndexCacheFile(File, RemoteLogSegment)}.
     */
    public static UUID uuidFromRemoteIndexCacheFileName(String fileName) {
        return UUID.fromString(
                fileName.substring(fileName.indexOf('_') + 1, fileName.indexOf('.')));
    }

    // ----------------------------------------------------------------------------------------
    // Remote Log Paths
    // ----------------------------------------------------------------------------------------

    /**
     * Returns the remote root directory path for storing log files.
     *
     * <p>The path contract:
     *
     * <pre>
     * {$remote.data.dir}/log
     * </pre>
     */
    public static FsPath remoteLogDir(Configuration conf) {
        return new FsPath(conf.get(ConfigOptions.REMOTE_DATA_DIR) + "/" + REMOTE_LOG_DIR_NAME);
    }

    /**
     * Returns the remote directory path for storing log files for a log tablet.
     *
     * <p>The path contract:
     *
     * <pre>
     * Non-Partitioned Table:
     * {$remote.data.dir}/log/{databaseName}/{tableName}-{tableId}/{bucket}
     *
     * Partitioned Table:
     * {$remote.data.dir}/log/{databaseName}/{tableName}-{tableId}/{partitionName}-p{partitionId}/{bucket}
     * </pre>
     *
     * @param remoteLogDir the remote log directory, usually should be "{$remote.data.dir}/log"
     */
    public static FsPath remoteLogTabletDir(
            FsPath remoteLogDir, PhysicalTablePath physicalPath, TableBucket tableBucket) {
        FsPath remoteTableDir = remoteTabletParentDir(remoteLogDir, physicalPath, tableBucket);
        return new FsPath(remoteTableDir, String.valueOf(tableBucket.getBucket()));
    }

    /**
     * Returns the remote file path of manifest file.
     *
     * <p>The path contract:
     *
     * <pre>
     * {remoteLogTabletDir}/metadata/{manifestId}.manifest
     * </pre>
     *
     * @param remoteLogTabletDir see {@link #remoteLogTabletDir(FsPath, PhysicalTablePath,
     *     TableBucket)}
     * @param manifestId the UUID of the manifest
     */
    public static FsPath remoteLogManifestFile(FsPath remoteLogTabletDir, UUID manifestId) {
        return new FsPath(
                remoteLogTabletDir,
                REMOTE_LOG_METADATA_DIR_NAME + "/" + manifestId + REMOTE_LOG_MANIFEST_FILE_SUFFIX);
    }

    /**
     * Returns the remote directory path for storing log segment files (log and index files).
     *
     * <p>The path contract:
     *
     * <pre>
     * {remoteTabletDir}/{remoteLogSegmentId}/
     * </pre>
     *
     * @param remoteLogTabletDir the remote log tablet dir, see {@link #remoteLogTabletDir(FsPath,
     *     PhysicalTablePath, TableBucket)}
     * @param remoteLogSegmentId the UUID of the remote log segment
     */
    public static FsPath remoteLogSegmentDir(FsPath remoteLogTabletDir, UUID remoteLogSegmentId) {
        return new FsPath(remoteLogTabletDir, remoteLogSegmentId.toString());
    }

    /** Returns the remote directory path for storing log segment files (log and index files). */
    public static FsPath remoteLogSegmentDir(
            FsPath remoteLogDir, RemoteLogSegment remoteLogSegment) {
        return new FsPath(
                remoteLogTabletDir(
                        remoteLogDir,
                        remoteLogSegment.physicalTablePath(),
                        remoteLogSegment.tableBucket()),
                remoteLogSegment.remoteLogSegmentId().toString());
    }

    /**
     * Returns the remote file path for storing the log segment file.
     *
     * <p>The path contract:
     *
     * <pre>
     * {remoteLogSegmentDir}/{baseOffset}.log
     * </pre>
     *
     * @param remoteLogSegmentDir the remote log segment dir, see {@link
     *     #remoteLogSegmentDir(FsPath, UUID)}
     * @param baseOffset the base offset of the log segment
     */
    public static FsPath remoteLogSegmentFile(FsPath remoteLogSegmentDir, long baseOffset) {
        return new FsPath(
                remoteLogSegmentDir, filenamePrefixFromOffset(baseOffset) + LOG_FILE_SUFFIX);
    }

    /** Returns the remote file path for storing the offset index file. */
    public static FsPath remoteOffsetIndexFile(
            FsPath remoteLogDir, RemoteLogSegment remoteLogSegment, String indexSuffix) {
        return remoteLogIndexFile(
                remoteLogSegmentDir(
                        remoteLogTabletDir(
                                remoteLogDir,
                                remoteLogSegment.physicalTablePath(),
                                remoteLogSegment.tableBucket()),
                        remoteLogSegment.remoteLogSegmentId()),
                remoteLogSegment.remoteLogStartOffset(),
                indexSuffix);
    }

    public static FsPath remoteWriterSnapshotFile(
            FsPath remoteLogDir, RemoteLogSegment remoteLogSegment, String indexSuffix) {
        return remoteLogIndexFile(
                remoteLogSegmentDir(
                        remoteLogTabletDir(
                                remoteLogDir,
                                remoteLogSegment.physicalTablePath(),
                                remoteLogSegment.tableBucket()),
                        remoteLogSegment.remoteLogSegmentId()),
                remoteLogSegment.remoteLogEndOffset(),
                indexSuffix);
    }

    /**
     * Returns the remote file path for storing the offset index file.
     *
     * <p>The path contract:
     *
     * <pre>
     * {remoteLogSegmentDir}/{baseOffset}.index
     * </pre>
     *
     * @param remoteLogSegmentDir the remote log segment dir, see {@link
     *     #remoteLogSegmentDir(FsPath, UUID)}
     * @param baseOffset the base offset of the log segment
     */
    public static FsPath remoteOffsetIndexFile(FsPath remoteLogSegmentDir, long baseOffset) {
        return remoteLogIndexFile(remoteLogSegmentDir, baseOffset, INDEX_FILE_SUFFIX);
    }

    /**
     * Returns the remote file path for storing the offset index file.
     *
     * <p>The path contract:
     *
     * <pre>
     * {remoteLogSegmentDir}/{baseOffset}.{indexSuffix}
     * </pre>
     *
     * @param remoteLogSegmentDir the remote log segment dir, see {@link
     *     #remoteLogSegmentDir(FsPath, UUID)}
     * @param baseOffset the base offset of the log segment
     * @param indexSuffix the file suffix of the index file
     */
    public static FsPath remoteLogIndexFile(
            FsPath remoteLogSegmentDir, long baseOffset, String indexSuffix) {
        return new FsPath(remoteLogSegmentDir, filenamePrefixFromOffset(baseOffset) + indexSuffix);
    }

    // ----------------------------------------------------------------------------------------
    // Remote KV Paths
    // ----------------------------------------------------------------------------------------

    /**
     * Returns the remote root directory path for storing kv snapshot files.
     *
     * <p>The path contract:
     *
     * <pre>
     * {$remote.data.dir}/kv
     * </pre>
     */
    public static FsPath remoteKvDir(Configuration conf) {
        return new FsPath(conf.get(ConfigOptions.REMOTE_DATA_DIR) + "/" + REMOTE_KV_DIR_NAME);
    }

    /**
     * Returns the remote directory path for storing kv snapshot files for a kv tablet.
     *
     * <p>The path contract:
     *
     * <pre>
     * Non-Partitioned Table:
     * {remoteKvDir}/{databaseName}/{tableName}-{tableId}/{bucket}
     *
     * Partitioned Table:
     * {remoteKvDir}/{databaseName}/{tableName}-{tableId}/{partitionName}-p{partitionId}/{bucket}
     * </pre>
     *
     * @param remoteKvDir the remote kv directory, usually should be "{$remote.data.dir}/kv"
     */
    public static FsPath remoteKvTabletDir(
            FsPath remoteKvDir, PhysicalTablePath physicalPath, TableBucket tableBucket) {
        FsPath remoteTableDir = remoteTabletParentDir(remoteKvDir, physicalPath, tableBucket);
        return new FsPath(remoteTableDir, String.valueOf(tableBucket.getBucket()));
    }

    /**
     * Returns the remote directory path for storing kv snapshot files or log segments file of
     * table.
     *
     * <p>The path contract:
     *
     * <pre>
     * Remote kv table dir
     * {$remote.data.dir}/kv/{databaseName}/{tableName}-{tableId}
     *
     * Remote log table dir.
     * {$remote.data.dir}/log/{databaseName}/{tableName}-{tableId}
     *
     * </pre>
     *
     * @param remoteKvOrLogBaseDir - the remote kv snapshots or log segments root dir of table.
     * @param tablePath - table path.
     * @param tableId - table id.
     */
    public static FsPath remoteTableDir(
            FsPath remoteKvOrLogBaseDir, TablePath tablePath, long tableId) {
        return new FsPath(
                remoteKvOrLogBaseDir,
                String.format(
                        "%s/%s-%d",
                        tablePath.getDatabaseName(), tablePath.getTableName(), tableId));
    }

    /**
     * Returns the remote directory path for storing kv snapshot files or log segments file of a
     * single partition.
     *
     * <pre>
     * Remote kv table dir
     * {$remote.data.dir}/kv/{databaseName}/{tableName}-{tableId}/{partitionName}-p{partitionId}
     *
     * Remote log table dir.
     * {$remote.data.dir}/log/{databaseName}/{tableName}-{tableId}/{partitionName}-p{partitionId}
     *
     * </pre>
     *
     * @param remoteKvOrLogBaseDir - the remote kv snapshots or log segments root dir of table.
     * @param partitionPath - the partition path.
     * @param tablePartition - table partition.
     */
    public static FsPath remotePartitionDir(
            FsPath remoteKvOrLogBaseDir,
            PhysicalTablePath partitionPath,
            TablePartition tablePartition) {
        return new FsPath(
                remoteTableDir(
                        remoteKvOrLogBaseDir,
                        partitionPath.getTablePath(),
                        tablePartition.getTableId()),
                String.format(
                        "%s-%s",
                        partitionPath.getPartitionName(),
                        PARTITION_DIR_PREFIX + tablePartition.getPartitionId()));
    }

    /**
     * Returns the remote directory path for storing kv snapshot exclusive files (manifest and
     * CURRENT files).
     *
     * <p>The path contract:
     *
     * <pre>
     * {remoteTabletDir}/snap-{snapshotId}/
     * </pre>
     *
     * @param remoteKvTabletDir the remote kv tablet dir, see {@link #remoteKvTabletDir(FsPath,
     *     PhysicalTablePath, TableBucket)}.
     * @param snapshotId the unique id of the kv snapshot.
     */
    public static FsPath remoteKvSnapshotDir(FsPath remoteKvTabletDir, long snapshotId) {
        return new FsPath(remoteKvTabletDir, REMOTE_KV_SNAPSHOT_DIR_PREFIX + snapshotId);
    }

    /**
     * Returns the remote directory path for storing kv snapshot shared files (SST files with UUID
     * prefix).
     *
     * <p>The path contract:
     *
     * <pre>
     * {remoteTabletDir}/shared/
     * </pre>
     *
     * @param remoteKvTabletDir the remote kv tablet dir, see {@link #remoteKvTabletDir(FsPath,
     *     PhysicalTablePath, TableBucket)}.
     */
    public static FsPath remoteKvSharedDir(FsPath remoteKvTabletDir) {
        return new FsPath(remoteKvTabletDir, REMOTE_KV_SNAPSHOT_SHARED_DIR);
    }

    // ----------------------------------------------------------------------------------------
    // Remote Paths for common use
    // ----------------------------------------------------------------------------------------

    /**
     * remoteDir can be {@link #remoteLogDir(Configuration)} or {@link #remoteKvDir(Configuration)}.
     */
    private static FsPath remoteTabletParentDir(
            FsPath remoteDir, PhysicalTablePath physicalPath, TableBucket tableBucket) {
        if (physicalPath.getPartitionName() == null) {
            return remoteTableDir(remoteDir, physicalPath.getTablePath(), tableBucket.getTableId());
        } else {
            checkNotNull(
                    tableBucket.getPartitionId(),
                    "partition id shouldn't be null for partitioned table");
            return remotePartitionDir(
                    remoteDir,
                    physicalPath,
                    new TablePartition(tableBucket.getTableId(), tableBucket.getPartitionId()));
        }
    }
}
