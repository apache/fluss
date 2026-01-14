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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/** A cleaner for cleaning kv snapshots and log segments files of table. */
public class RemoteStorageCleaner {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteStorageCleaner.class);

    private final FsPath remoteKvDir;

    private final FsPath remoteLogDir;

    private final String remoteDataDir;

    private final FileSystem remoteFileSystem;

    private final ExecutorService ioExecutor;

    public RemoteStorageCleaner(Configuration configuration, ExecutorService ioExecutor) {
        this.remoteKvDir = FlussPaths.remoteKvDir(configuration);
        this.remoteLogDir = FlussPaths.remoteLogDir(configuration);
        this.remoteDataDir = configuration.getString(ConfigOptions.REMOTE_DATA_DIR);
        this.ioExecutor = ioExecutor;
        try {
            this.remoteFileSystem = remoteKvDir.getFileSystem();
        } catch (IOException e) {
            throw new FlussRuntimeException(
                    "Fail to get remote file system for path " + remoteKvDir, e);
        }
    }

    public void asyncDeleteTableRemoteDir(TablePath tablePath, boolean isKvTable, long tableId) {
        if (isKvTable) {
            asyncDeleteDir(FlussPaths.remoteTableDir(remoteKvDir, tablePath, tableId));
        }
        asyncDeleteDir(FlussPaths.remoteTableDir(remoteLogDir, tablePath, tableId));

        // Always delete lake snapshot metadata directory, regardless of isLakeEnabled flag.
        // This is because if a table was enabled datalake but turned off later, and then the table
        // was deleted, we may leave the lake snapshot metadata files behind if we only delete when
        // isLakeEnabled is true. By always deleting, we ensure cleanup of any existing metadata
        // files.
        asyncDeleteDir(FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, tablePath, tableId));
    }

    public void asyncDeletePartitionRemoteDir(
            PhysicalTablePath physicalTablePath, boolean isKvTable, TablePartition tablePartition) {
        if (isKvTable) {
            asyncDeleteDir(
                    FlussPaths.remotePartitionDir(remoteKvDir, physicalTablePath, tablePartition));
        }
        asyncDeleteDir(
                FlussPaths.remotePartitionDir(remoteLogDir, physicalTablePath, tablePartition));
    }

    private void asyncDeleteDir(FsPath fsPath) {
        ioExecutor.submit(
                () -> {
                    try {
                        if (remoteFileSystem.exists(fsPath)) {
                            remoteFileSystem.delete(fsPath, true);
                        }
                    } catch (IOException e) {
                        LOG.error("Delete remote data dir {} failed.", fsPath, e);
                    }
                });
    }
}
