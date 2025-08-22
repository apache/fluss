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

package org.apache.fluss.server.replica.standby;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.FsPathAndFileName;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.remote.RemoteFileDownloader;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.kv.KvApplyLogHelper;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.utils.FlussPaths.LOG_FILE_SUFFIX;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentDir;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentFile;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A task interface to become hotStandbyReplica. */
public class BecomeHotStandbyTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(BecomeHotStandbyTask.class);

    private final Replica replica;
    private final RemoteFileDownloader remoteFileDownloader;
    private final Path remoteLogTempDir;
    private final RemoteLogManager remoteLogManager;
    private final FsPath remoteLogTabletDir;
    private final KvApplyLogHelper kvApplyLogHelper;
    private final int maxFetchLogSizeWhenApplying;

    private volatile boolean cancelled = false;

    public BecomeHotStandbyTask(
            Replica replica,
            RemoteFileDownloader remoteFileDownloader,
            RemoteLogManager remoteLogManager,
            Path remoteLogTempDir,
            KvApplyLogHelper kvApplyLogHelper,
            int maxFetchLogSizeWhenApplying) {
        this.kvApplyLogHelper = kvApplyLogHelper;
        this.replica = replica;
        this.remoteFileDownloader = remoteFileDownloader;
        this.remoteLogManager = remoteLogManager;
        this.remoteLogTempDir = remoteLogTempDir;
        this.remoteLogTabletDir =
                FlussPaths.remoteLogTabletDir(
                        remoteLogManager.remoteLogDir(),
                        replica.getPhysicalTablePath(),
                        replica.getTableBucket());
        this.maxFetchLogSizeWhenApplying = maxFetchLogSizeWhenApplying;
    }

    @Override
    public void run() {
        if (isCancelled()) {
            return;
        }

        // 1. try to drop kv if it exists.
        replica.dropKv();

        // update minRetainOffset to 0 means all logs are retained from now on.
        replica.getLogTablet().updateMinKvAppliedOffset(0L);

        // 2. apply snapshot to kvTablet.
        Optional<CompletedSnapshot> completedSnapshotOpt = replica.initKvTablet(false);
        TableBucket tableBucket = replica.getTableBucket();
        if (completedSnapshotOpt.isPresent()) {
            CompletedSnapshot snapshot = completedSnapshotOpt.get();
            long logOffset = snapshot.getLogOffset();
            LOG.info(
                    "Init kv tablet for {} of {} finish, logOffset: {}",
                    replica.getPhysicalTablePath(),
                    tableBucket,
                    logOffset);
            KvTablet kvTablet = replica.getKvTablet();
            checkNotNull(kvTablet, "kvTablet cannot be null");
            // update kvAppliedOffset.
            kvTablet.setKvAppliedOffset(logOffset);
            replica.getLogTablet().updateMinKvAppliedOffset(logOffset);
        } else {
            LOG.warn("snapshot is not present");
        }

        // 3. apply remote log to kvTablet.
        if (isCancelled()) {
            return;
        }

        KvTablet kvTablet = replica.getKvTablet();
        checkNotNull(kvTablet, "kvTablet cannot be null");
        long currentAppliedOffset = kvTablet.getKvAppliedOffset();

        if (!remoteLogManager.hasRemoteLog(tableBucket)) {
            try {
                remoteLogManager.addRemoteLog(replica);
            } catch (Exception e) {
                LOG.warn("No remote log segment to apply");
                // TODO handle the exception
            }
        }
        List<RemoteLogSegment> remoteLogSegments =
                remoteLogManager.relevantRemoteLogSegments(tableBucket, currentAppliedOffset);
        LOG.info(
                "Try to apply remote log for {}, currentAppliedOffset: {}.",
                tableBucket,
                currentAppliedOffset);
        if (remoteLogSegments.isEmpty()) {
            LOG.warn("No remote log segment to apply");
        } else {
            int startPos;
            for (int i = 0; i < remoteLogSegments.size(); i++) {
                RemoteLogSegment remoteLogSegment = remoteLogSegments.get(i);
                if (i == 0) {
                    startPos =
                            remoteLogManager.lookupPositionForOffset(
                                    remoteLogSegment, currentAppliedOffset);
                } else {
                    startPos = 0;
                }

                long remoteLogEndOffset = remoteLogSegment.remoteLogEndOffset();
                if (remoteLogEndOffset <= replica.getLocalLogStartOffset()) {
                    long nextFetchOffset = 0;
                    try {
                        nextFetchOffset = applyRemoteLogSegment(remoteLogSegment, startPos);
                    } catch (Exception e) {
                        LOG.warn("Failed to apply remote log segment: {}", remoteLogSegment, e);
                        // TODO handle the exception
                    }
                    kvTablet.setKvAppliedOffset(nextFetchOffset);
                    // flush kv data from kvPreWriteBuffer to rocksdb.
                    replica.mayFlushKv(nextFetchOffset);
                    replica.getLogTablet().updateMinKvAppliedOffset(nextFetchOffset);
                } else {
                    // When remoteLogEndOffset > localLogStartOffset, it indicates that log segments
                    // already exist locally. In this case, we prioritize consuming the local logs
                    // to accelerate the recovery process.
                    break;
                }
            }
        }
        remoteLogManager.removeRemoteLog(tableBucket);

        // 4. apply local log to kvTablet.
        LogTablet logTablet = replica.getLogTablet();
        while (true) {
            if (isCancelled()) {
                return;
            }

            long nextFetchOffset = kvTablet.getKvAppliedOffset();
            try {
                LogRecords logRecords =
                        logTablet
                                .read(
                                        nextFetchOffset,
                                        maxFetchLogSizeWhenApplying,
                                        FetchIsolation.LOG_END,
                                        true,
                                        null)
                                .getRecords();
                if (logRecords == MemoryLogRecords.EMPTY) {
                    Thread.sleep(100);
                    continue;
                }

                nextFetchOffset = kvApplyLogHelper.applyLogRecords(logRecords, nextFetchOffset);
                kvTablet.setKvAppliedOffset(nextFetchOffset);
                // flush kv data from kvPreWriteBuffer to rocksdb.
                replica.mayFlushKv(nextFetchOffset);
                replica.getLogTablet().updateMinKvAppliedOffset(nextFetchOffset);
            } catch (Exception e) {
                LOG.warn("read log failed", e);
                // TODO handle the error.
                replica.truncateTo(nextFetchOffset);
            }
        }
    }

    public void cancel() {
        cancelled = true;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    private long applyRemoteLogSegment(RemoteLogSegment remoteLogSegment, int startPos)
            throws Exception {
        LOG.info("Apply remote log segment: {}", remoteLogSegment);
        FsPathAndFileName fsPathAndFileName =
                getFsPathAndFileName(remoteLogTabletDir, remoteLogSegment);
        try {
            remoteFileDownloader.downloadFileAsync(fsPathAndFileName, remoteLogTempDir).get();
        } catch (InterruptedException | ExecutionException e) {
            // TODO handle the exception
        }

        // local log file to consume.
        File localFile = new File(remoteLogTempDir.toFile(), fsPathAndFileName.getFileName());
        FileLogRecords fileLogRecords = getFileLogRecords(localFile, startPos);

        // consume and write to kv.
        long nextFetchOffset = kvApplyLogHelper.applyLogRecords(fileLogRecords, startPos);

        // delete local file after consuming.
        localFile.delete();

        return nextFetchOffset;
    }

    private static FsPathAndFileName getFsPathAndFileName(
            FsPath remoteLogTabletDir, RemoteLogSegment segment) {
        FsPath remotePath =
                remoteLogSegmentFile(
                        remoteLogSegmentDir(remoteLogTabletDir, segment.remoteLogSegmentId()),
                        segment.remoteLogStartOffset());
        return new FsPathAndFileName(remotePath, getLocalFileNameOfRemoteSegment(segment));
    }

    /**
     * Get the local file name of the remote log segment.
     *
     * <p>The file name is in pattern:
     *
     * <pre>
     *     ${remote_segment_id}_${offset_prefix}.log
     * </pre>
     */
    private static String getLocalFileNameOfRemoteSegment(RemoteLogSegment segment) {
        return segment.remoteLogSegmentId()
                + "_"
                + FlussPaths.filenamePrefixFromOffset(segment.remoteLogStartOffset())
                + LOG_FILE_SUFFIX;
    }

    private FileLogRecords getFileLogRecords(File localFile, int startPosition) {
        try {
            FileLogRecords fileLogRecords = FileLogRecords.open(localFile, false);
            if (startPosition > 0) {
                return fileLogRecords.slice(startPosition, Integer.MAX_VALUE);
            } else {
                return fileLogRecords;
            }
        } catch (IOException e) {
            throw new FlussRuntimeException(e);
        }
    }
}
