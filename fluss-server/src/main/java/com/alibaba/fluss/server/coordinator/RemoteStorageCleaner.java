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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/** A cleaner for cleaning kv snapshots and log segments files of table. */
public class RemoteStorageCleaner {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteStorageCleaner.class);

    private final FsPath remoteKvDir;

    private final FsPath remoteLogDir;

    private final FileSystem remoteFileSystem;

    private final ExecutorService ioExecutor;

    public RemoteStorageCleaner(Configuration configuration, ExecutorService ioExecutor) {
        this.remoteKvDir = FlussPaths.remoteKvDir(configuration);
        this.remoteLogDir = FlussPaths.remoteLogDir(configuration);
        this.ioExecutor = ioExecutor;
        try {
            this.remoteFileSystem = remoteKvDir.getFileSystem();
        } catch (IOException e) {
            throw new FlussRuntimeException(
                    "Fail to get remote file system for path " + remoteKvDir, e);
        }
    }

    public void deleteTableRemoteDir(TablePath tablePath, boolean isKvTable, long tableId) {
        if (isKvTable) {
            asyncDeleteDir(FlussPaths.remoteTableDir(remoteKvDir, tablePath, tableId));
        }
        asyncDeleteDir(FlussPaths.remoteTableDir(remoteLogDir, tablePath, tableId));
    }

    public void deletePartitionRemoteDir(
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
