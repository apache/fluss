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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.concurrent.FutureUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A CompletedSnapshot describes a snapshot after the snapshot files has been uploaded to remote
 * storage and that is considered successful. The CompletedSnapshot class contains all the metadata
 * of the snapshot, i.e., snapshot ID, and the handles to all kv files that are part of the
 * snapshot.
 *
 * <h2>Size the CompletedSnapshot Instances</h2>
 *
 * <p>In most cases, the CompletedSnapshot objects are very small, because the handles to the
 * snapshot kv are only pointers (such as file paths).
 *
 * <h2>Metadata Persistence</h2>
 *
 * <p>The metadata of the CompletedSnapshot is also persisted in the snapshot directory with the
 * name {@link #SNAPSHOT_METADATA_FILE_NAME}.
 */
@NotThreadSafe
public class CompletedSnapshot {

    private static final String SNAPSHOT_METADATA_FILE_NAME = "_METADATA";

    /** The table bucket that this snapshot belongs to. */
    private final TableBucket tableBucket;

    /** The ID (logical timestamp) of the snapshot. */
    private final long snapshotID;

    /** The handle for the kv that belongs to this snapshot. */
    private final KvSnapshotHandle kvSnapshotHandle;

    /** The next log offset when the snapshot is triggered. */
    private final long logOffset;

    /** The location where the snapshot is stored. */
    private final FsPath snapshotLocation;

    public static final String SNAPSHOT_DATA_NOT_EXISTS_ERROR_MESSAGE = "No such file or directory";

    public CompletedSnapshot(
            TableBucket tableBucket,
            long snapshotID,
            FsPath snapshotLocation,
            KvSnapshotHandle kvSnapshotHandle,
            long logOffset) {
        this.tableBucket = tableBucket;
        this.snapshotID = snapshotID;
        this.snapshotLocation = snapshotLocation;
        this.kvSnapshotHandle = kvSnapshotHandle;
        this.logOffset = logOffset;
    }

    @VisibleForTesting
    CompletedSnapshot(
            TableBucket tableBucket,
            long snapshotID,
            FsPath snapshotLocation,
            KvSnapshotHandle kvSnapshotHandle) {
        this(tableBucket, snapshotID, snapshotLocation, kvSnapshotHandle, 0);
    }

    public long getSnapshotID() {
        return snapshotID;
    }

    public KvSnapshotHandle getKvSnapshotHandle() {
        return kvSnapshotHandle;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public long getLogOffset() {
        return logOffset;
    }

    public long getSnapshotSize() {
        return kvSnapshotHandle.getSnapshotSize();
    }

    /**
     * Register all shared kv files in the given registry. This method is called before the snapshot
     * is added into the store.
     *
     * @param sharedKvFileRegistry The registry where shared kv files are registered
     */
    public void registerSharedKvFilesAfterRestored(SharedKvFileRegistry sharedKvFileRegistry) {
        sharedKvFileRegistry.registerAllAfterRestored(this);
    }

    public CompletableFuture<Void> discardAsync(Executor ioExecutor) {
        // it'll discard the snapshot files for kv, it'll always discard
        // the private files; for shared files, only if they're not be registered in
        // SharedKvRegistry, can the files be deleted.
        CompletableFuture<Void> discardKvFuture =
                FutureUtils.runAsync(kvSnapshotHandle::discard, ioExecutor);

        CompletableFuture<Void> discardMetaFileFuture =
                FutureUtils.runAsync(this::disposeMetadata, ioExecutor);

        return FutureUtils.runAfterwards(
                FutureUtils.completeAll(Arrays.asList(discardKvFuture, discardMetaFileFuture)),
                this::disposeSnapshotStorage);
    }

    private void disposeSnapshotStorage() throws IOException {
        if (snapshotLocation != null) {
            FileSystem fileSystem = snapshotLocation.getFileSystem();
            fileSystem.delete(snapshotLocation, false);
        }
    }

    /**
     * Return the metadata file path that stores all the information that describes the snapshot.
     */
    public FsPath getMetadataFilePath() {
        return new FsPath(snapshotLocation, SNAPSHOT_METADATA_FILE_NAME);
    }

    public static FsPath getMetadataFilePath(FsPath snapshotLocation) {
        return new FsPath(snapshotLocation, SNAPSHOT_METADATA_FILE_NAME);
    }

    private void disposeMetadata() throws IOException {
        FsPath metadataFilePath = getMetadataFilePath();
        FileSystem fileSystem = metadataFilePath.getFileSystem();
        fileSystem.delete(metadataFilePath, false);
    }

    public FsPath getSnapshotLocation() {
        return snapshotLocation;
    }

    @Override
    public String toString() {
        return String.format(
                "CompletedSnapshot %d for %s located at %s",
                snapshotID, tableBucket, snapshotLocation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompletedSnapshot that = (CompletedSnapshot) o;
        return snapshotID == that.snapshotID
                && logOffset == that.logOffset
                && Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(kvSnapshotHandle, that.kvSnapshotHandle)
                && Objects.equals(snapshotLocation, that.snapshotLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableBucket, snapshotID, kvSnapshotHandle, logOffset, snapshotLocation);
    }
}
