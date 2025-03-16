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

package com.alibaba.fluss.server.log.remote;

import com.alibaba.fluss.exception.RemoteResourceNotFoundException;
import com.alibaba.fluss.exception.RemoteStorageException;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.remote.RemoteLogSegment;

import java.io.Closeable;
import java.io.InputStream;

import static com.alibaba.fluss.utils.FlussPaths.INDEX_FILE_SUFFIX;
import static com.alibaba.fluss.utils.FlussPaths.TIME_INDEX_FILE_SUFFIX;
import static com.alibaba.fluss.utils.FlussPaths.WRITER_SNAPSHOT_FILE_SUFFIX;

/**
 * This interface provides the lifecycle of remote log segments and remote log manifest snapshot
 * that include copy, fetch, and delete from remote storage.
 *
 * <p>Each upload or copy of a segment is initiated with {@link RemoteLogSegment} containing
 * RemoteLogSegmentId which is universally unique even for the same table bucket and offsets.
 *
 * <p>The implementation of this interface should be thread-safe.
 */
public interface RemoteLogStorage extends Closeable {
    /** Type of the index file. */
    enum IndexType {
        /** Represents offset index. */
        OFFSET,

        /** Represents timestamp index. */
        TIMESTAMP,

        /** Represents producer snapshot index. */
        WRITER_ID_SNAPSHOT;

        public static String getFileSuffix(IndexType indexType) {
            switch (indexType) {
                case OFFSET:
                    return INDEX_FILE_SUFFIX;
                case TIMESTAMP:
                    return TIME_INDEX_FILE_SUFFIX;
                case WRITER_ID_SNAPSHOT:
                    return WRITER_SNAPSHOT_FILE_SUFFIX;
                default:
                    throw new IllegalArgumentException("Unknown index type: " + indexType);
            }
        }
    }

    /**
     * Returns the remote log directory.
     *
     * @return the remote log directory.
     */
    FsPath getRemoteLogDir();

    /**
     * Copies the given {@link LogSegmentFiles} provided for the given {@link RemoteLogSegment}.
     * This includes log segment and its auxiliary indexes like offset index and writer id snapshot
     * index.
     *
     * <p>Invoker of this API should always send a unique id as part of {@link
     * RemoteLogSegment#remoteLogSegmentId()} even when it retries to invoke this method for the
     * same log segment data.
     *
     * <p>This operation is expected to be idempotent. If a copy operation is retried and there is
     * existing content already written, it should be overwritten, and do not throw {@link
     * RemoteStorageException}
     *
     * @param remoteLogSegment the remote log segment.
     * @param logSegmentFiles files to be copied to remote storage.
     * @throws RemoteStorageException if there are any errors in storing the data of the segment.
     */
    void copyLogSegmentFiles(RemoteLogSegment remoteLogSegment, LogSegmentFiles logSegmentFiles)
            throws RemoteStorageException;

    /**
     * Deletes the resources associated with the given {@link RemoteLogSegment}. Deletion is
     * considered as successful if this call returns successfully without any errors. It will throw
     * {@link RemoteStorageException} if there are any errors in deleting the file.
     *
     * <p>This operation is expected to be idempotent. If resources are not found, it is not
     * expected to throw {@link RemoteResourceNotFoundException} as it may be already removed from a
     * previous attempt.
     *
     * @param remoteLogSegment the remote log segment.
     * @throws RemoteStorageException if there are any storage related errors occurred.
     */
    void deleteLogSegmentFiles(RemoteLogSegment remoteLogSegment) throws RemoteStorageException;

    /**
     * Returns the index for the respective log segment of {@link RemoteLogSegment}.
     *
     * @param remoteLogSegment the remote log segment.
     * @param indexType type of the index to be fetched for the segment.
     * @return input stream of the requested index.
     * @throws RemoteStorageException if there are any errors while fetching the index.
     */
    InputStream fetchIndex(RemoteLogSegment remoteLogSegment, IndexType indexType)
            throws RemoteStorageException;

    /**
     * Read the remote log manifest from remote manifest file path.
     *
     * @param remoteLogManifestPath the path of the manifest file to be read in remote storage.
     * @return the remote log manifest.
     * @throws RemoteStorageException if there are any errors while download remote log manifest
     *     snapshot.
     */
    RemoteLogManifest readRemoteLogManifestSnapshot(FsPath remoteLogManifestPath)
            throws RemoteStorageException;

    /**
     * Deletes the remote log manifest file from remote storage.
     *
     * @param remoteLogManifestPath the path of the remote log manifest file to be deleted in remote
     *     storage.
     * @throws RemoteStorageException if there are any errors while delete remote log manifest
     *     snapshot.
     */
    void deleteRemoteLogManifestSnapshot(FsPath remoteLogManifestPath)
            throws RemoteStorageException;

    /**
     * Writes the remote log manifest to remote manifest file path.
     *
     * @param manifest the remote log manifest to be written.
     * @return the path of the written remote log manifest file.
     * @throws RemoteStorageException if there are any errors while upload remote log manifest
     *     snapshot.
     */
    FsPath writeRemoteLogManifestSnapshot(RemoteLogManifest manifest) throws RemoteStorageException;

    /**
     * Deletes the remote log data and metadata from remote storage for the input table bucket as
     * this table have been deleted.
     *
     * @param physicalTablePath the physical table path.
     * @param tableBucket the table bucket.
     * @throws RemoteStorageException if there are any errors while delete remote log data and
     *     metadata.
     */
    void deleteTableBucket(PhysicalTablePath physicalTablePath, TableBucket tableBucket)
            throws RemoteStorageException;
}
