/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** A cleaner for cleaning kv snapshots and log segments files of table. */
public class RemoteStorageCleaner {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteStorageCleaner.class);

    private final FsPath remoteKvDir;

    private final FsPath remoteLogDir;

    private final FileSystem remoteFileSystem;

    public RemoteStorageCleaner(Configuration configuration) throws IOException {
        this.remoteKvDir = FlussPaths.remoteKvDir(configuration);
        this.remoteLogDir = FlussPaths.remoteLogDir(configuration);
        this.remoteFileSystem = remoteKvDir.getFileSystem();
    }

    public void deleteTableRemoteDir(TablePath tablePath, boolean isKvTable, long tableId) {
        if (isKvTable) {
            deleteDir(tableKvRemoteDir(tablePath, tableId));
        }
        deleteDir(tableLogRemoteDir(tablePath, tableId));
    }

    private void deleteDir(FsPath fsPath) {
        try {
            if (remoteFileSystem.exists(fsPath)) {
                long startTs = System.currentTimeMillis();
                remoteFileSystem.delete(fsPath, true);
                LOG.info(
                        "Delete table's remote data dir {} success, cost {} ms.",
                        fsPath,
                        System.currentTimeMillis() - startTs);
            }
        } catch (IOException e) {
            LOG.error("Delete table's remote data dir {} failed.", fsPath, e);
        }
    }

    private FsPath tableKvRemoteDir(TablePath tablePath, long tableId) {
        return FlussPaths.remoteTableDir(remoteKvDir, tablePath, tableId);
    }

    private FsPath tableLogRemoteDir(TablePath tablePath, long tableId) {
        return FlussPaths.remoteTableDir(remoteLogDir, tablePath, tableId);
    }
}
