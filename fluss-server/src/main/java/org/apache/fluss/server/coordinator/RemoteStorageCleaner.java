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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * A cleaner responsible for asynchronously deleting remote storage directories.
 *
 * <p>This class handles the cleanup of remote data directories when tables or partitions are
 * deleted. It supports multiple remote data directories configured via {@link
 * ConfigOptions#REMOTE_DATA_DIR} and {@link ConfigOptions#REMOTE_DATA_DIRS}, and manages the
 * deletion of both KV table directories and log directories.
 *
 * <p>All deletion operations are executed asynchronously through a dedicated I/O executor to avoid
 * blocking the main coordinator operations.
 */
public class RemoteStorageCleaner {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteStorageCleaner.class);

    /** The list of remote data directory paths as strings. */
    private final List<String> remoteDataDirs;

    /** The list of remote KV directory paths for KV table data. */
    private final List<FsPath> remoteKvDirs;

    /** The list of remote log directory paths for log data. */
    private final List<FsPath> remoteLogDirs;

    /** The executor service for executing asynchronous I/O operations. */
    private final ExecutorService ioExecutor;

    public RemoteStorageCleaner(Configuration configuration, ExecutorService ioExecutor) {
        List<String> dirs = configuration.get(ConfigOptions.REMOTE_DATA_DIRS);

        remoteDataDirs = new ArrayList<>(dirs.size() + 1);
        remoteKvDirs = new ArrayList<>(dirs.size() + 1);
        remoteLogDirs = new ArrayList<>(dirs.size() + 1);

        remoteDataDirs.add(configuration.getString(ConfigOptions.REMOTE_DATA_DIR));
        remoteKvDirs.add(FlussPaths.remoteKvDir(configuration));
        remoteLogDirs.add(FlussPaths.remoteLogDir(configuration));

        for (String remoteDataDir : dirs) {
            remoteKvDirs.add(FlussPaths.remoteKvDir(new FsPath(remoteDataDir)));
            remoteLogDirs.add(FlussPaths.remoteLogDir(new FsPath(remoteDataDir)));
            remoteDataDirs.add(remoteDataDir);
        }

        this.ioExecutor = ioExecutor;
    }

    public void asyncDeleteTableRemoteDir(TablePath tablePath, boolean isKvTable, long tableId) {
        for (int i = 0; i < remoteDataDirs.size(); i++) {
            String remoteDataDir = remoteDataDirs.get(i);
            FsPath remoteKvDir = remoteKvDirs.get(i);
            FsPath remoteLogDir = remoteLogDirs.get(i);

            if (isKvTable) {
                asyncDeleteDir(FlussPaths.remoteTableDir(remoteKvDir, tablePath, tableId));
            }
            asyncDeleteDir(FlussPaths.remoteTableDir(remoteLogDir, tablePath, tableId));

            // Always delete lake snapshot metadata directory, regardless of isLakeEnabled flag.
            // This is because if a table was enabled datalake but turned off later, and then the
            // table was deleted, we may leave the lake snapshot metadata files behind if we only
            // delete when isLakeEnabled is true. By always deleting, we ensure cleanup of any
            // existing metadata files.
            asyncDeleteDir(
                    FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, tablePath, tableId));
        }
    }

    public void asyncDeletePartitionRemoteDir(
            PhysicalTablePath physicalTablePath, boolean isKvTable, TablePartition tablePartition) {
        for (int i = 0; i < remoteDataDirs.size(); i++) {
            FsPath remoteKvDir = remoteKvDirs.get(i);
            FsPath remoteLogDir = remoteLogDirs.get(i);

            if (isKvTable) {
                asyncDeleteDir(
                        FlussPaths.remotePartitionDir(
                                remoteKvDir, physicalTablePath, tablePartition));
            }
            asyncDeleteDir(
                    FlussPaths.remotePartitionDir(remoteLogDir, physicalTablePath, tablePartition));
        }
    }

    private void asyncDeleteDir(FsPath fsPath) {
        ioExecutor.submit(
                () -> {
                    try {
                        FileSystem remoteFileSystem = fsPath.getFileSystem();
                        if (remoteFileSystem.exists(fsPath)) {
                            remoteFileSystem.delete(fsPath, true);
                        }
                    } catch (IOException e) {
                        LOG.error("Delete remote data dir {} failed.", fsPath, e);
                    }
                });
    }
}
