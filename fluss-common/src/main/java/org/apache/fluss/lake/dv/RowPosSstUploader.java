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

package org.apache.fluss.lake.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Uploads RowPos SST files and index to remote storage for a given lake snapshot.
 *
 * <p>The upload layout is:
 *
 * <pre>
 * Non-partitioned:
 * {remoteLakeTableSnapshotDir}/rowPos/{snapshotId}/
 *   ├── index.json
 *   ├── {bucketId}/sst_0.sst
 *   └── ...
 *
 * Partitioned:
 * {remoteLakeTableSnapshotDir}/rowPos/{snapshotId}/{partitionId}/
 *   ├── index.json
 *   ├── {bucketId}/sst_0.sst
 *   └── ...
 * </pre>
 *
 * <p>The {@code index.json} is always written <b>last</b>, so its presence guarantees that all SST
 * files for the snapshot have been fully uploaded.
 */
@Internal
public class RowPosSstUploader {

    private static final String ROW_POS_DIR = "rowPos";
    private static final String INDEX_FILE = "index.json";
    private static final int UPLOAD_BUFFER_SIZE = 16 * 1024;

    private final FsPath remoteLakeTableSnapshotDir;

    public RowPosSstUploader(FsPath remoteLakeTableSnapshotDir) {
        this.remoteLakeTableSnapshotDir = remoteLakeTableSnapshotDir;
    }

    /**
     * Uploads SST data for all buckets under the given snapshot.
     *
     * <p>Steps:
     *
     * <ol>
     *   <li>Upload each bucket's SST files to {@code rowPos/{snapshotId}/{bucketId}/}
     *   <li>Write {@code index.json} last (atomic visibility guarantee)
     * </ol>
     *
     * @param snapshotId the lake snapshot ID used as the directory name
     * @param bucketSstMap mapping from bucketId to local SST data
     */
    public void upload(long snapshotId, Map<Integer, BucketSstData> bucketSstMap)
            throws IOException {
        FsPath snapshotDir = new FsPath(remoteLakeTableSnapshotDir, ROW_POS_DIR + "/" + snapshotId);

        FileSystem fs = snapshotDir.getFileSystem();
        fs.mkdirs(snapshotDir);

        // Build index while uploading SST files
        Map<Integer, List<RowPosSstIndex.SstFileEntry>> indexEntries = new HashMap<>();

        for (Map.Entry<Integer, BucketSstData> entry : bucketSstMap.entrySet()) {
            int bucketId = entry.getKey();
            BucketSstData data = entry.getValue();

            FsPath bucketDir = new FsPath(snapshotDir, String.valueOf(bucketId));
            fs.mkdirs(bucketDir);

            List<RowPosSstIndex.SstFileEntry> fileEntries = new ArrayList<>();
            for (RowPosSstFileWriter.SstFileMeta meta : data.getSstMetas()) {
                FsPath remoteSstPath = new FsPath(bucketDir, meta.getFileName());
                File localFile = new File(data.getLocalSstDir(), meta.getFileName());
                uploadFile(fs, localFile, remoteSstPath);
                fileEntries.add(
                        new RowPosSstIndex.SstFileEntry(meta.getFileName(), meta.getFileSize()));
            }
            indexEntries.put(bucketId, fileEntries);
        }

        // Write index.json last for atomic visibility
        RowPosSstIndex index = new RowPosSstIndex(indexEntries);
        FsPath indexPath = new FsPath(snapshotDir, INDEX_FILE);
        byte[] indexBytes = index.toJsonBytes();
        try (FSDataOutputStream out = fs.create(indexPath, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(indexBytes);
        }
    }

    /**
     * Uploads one bucket's SST files to remote storage without writing index.json. Called by Reader
     * for per-bucket parallel upload.
     *
     * @param snapshotId the lake snapshot ID used as the directory name
     * @param partitionId the partition ID, or null for non-partitioned tables
     * @param bucketId the bucket ID
     * @param data the local SST data for the bucket
     */
    public void uploadBucketSsts(
            long snapshotId, @Nullable Long partitionId, int bucketId, BucketSstData data)
            throws IOException {
        FsPath baseDir = buildBaseDir(snapshotId, partitionId);
        FsPath bucketDir = new FsPath(baseDir, String.valueOf(bucketId));

        FileSystem fs = bucketDir.getFileSystem();
        fs.mkdirs(bucketDir);

        for (RowPosSstFileWriter.SstFileMeta meta : data.getSstMetas()) {
            FsPath remoteSstPath = new FsPath(bucketDir, meta.getFileName());
            File localFile = new File(data.getLocalSstDir(), meta.getFileName());
            uploadFile(fs, localFile, remoteSstPath);
        }
    }

    /**
     * Writes the index.json file for a snapshot. Called by Committer after all Readers have
     * finished uploading their bucket SST files.
     *
     * @param snapshotId the lake snapshot ID
     * @param partitionId the partition ID, or null for non-partitioned tables
     * @param index the index containing all bucket SST file entries
     */
    public void writeIndex(long snapshotId, @Nullable Long partitionId, RowPosSstIndex index)
            throws IOException {
        FsPath baseDir = buildBaseDir(snapshotId, partitionId);
        FsPath indexPath = new FsPath(baseDir, INDEX_FILE);

        FileSystem fs = indexPath.getFileSystem();
        fs.mkdirs(baseDir);

        byte[] indexBytes = index.toJsonBytes();
        try (FSDataOutputStream out = fs.create(indexPath, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(indexBytes);
        }
    }

    /** Whether a RowPos scan was already uploaded for this snapshot (any partition). */
    public boolean isIndexed(long snapshotId) throws IOException {
        FsPath dir = buildBaseDir(snapshotId, null);
        return dir.getFileSystem().exists(dir);
    }

    /** Builds the base directory path for a snapshot, optionally scoped to a partition. */
    private FsPath buildBaseDir(long snapshotId, @Nullable Long partitionId) {
        String path = ROW_POS_DIR + "/" + snapshotId;
        if (partitionId != null) {
            path += "/" + partitionId;
        }
        return new FsPath(remoteLakeTableSnapshotDir, path);
    }

    private void uploadFile(FileSystem fs, File localFile, FsPath remotePath) throws IOException {
        FileInputStream fis = null;
        FSDataOutputStream out = null;
        try {
            fis = new FileInputStream(localFile);
            out = fs.create(remotePath, FileSystem.WriteMode.NO_OVERWRITE);
            byte[] buffer = new byte[UPLOAD_BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        } finally {
            IOUtils.closeQuietly(fis);
            IOUtils.closeQuietly(out);
        }
    }

    /** Local SST data for one bucket. */
    public static class BucketSstData {
        private final String localSstDir;
        private final List<RowPosSstFileWriter.SstFileMeta> sstMetas;

        public BucketSstData(String localSstDir, List<RowPosSstFileWriter.SstFileMeta> sstMetas) {
            this.localSstDir = localSstDir;
            this.sstMetas = sstMetas;
        }

        public String getLocalSstDir() {
            return localSstDir;
        }

        public List<RowPosSstFileWriter.SstFileMeta> getSstMetas() {
            return sstMetas;
        }
    }
}
