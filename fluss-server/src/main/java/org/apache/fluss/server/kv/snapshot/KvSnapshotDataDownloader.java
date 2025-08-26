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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.FsPathAndFileName;
import org.apache.fluss.fs.utils.FileDownloadSpec;
import org.apache.fluss.fs.utils.FileDownloadUtils;
import org.apache.fluss.utils.CloseableRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Help class for downloading kv snapshot data files. */
public class KvSnapshotDataDownloader extends KvSnapshotDataTransfer {

    public KvSnapshotDataDownloader(ExecutorService dataTransferThreadPool) {
        super(dataTransferThreadPool);
    }

    /**
     * Transfer all data to the target directory, as specified in the download requests.
     *
     * @param kvSnapshotDownloadSpec the spec of download .
     * @throws Exception If anything about the download goes wrong.
     */
    public void transferAllDataToDirectory(
            KvSnapshotDownloadSpec kvSnapshotDownloadSpec, CloseableRegistry closeableRegistry)
            throws Exception {
        transferAllDataToDirectory(
                Collections.singletonList(kvSnapshotDownloadSpec), closeableRegistry);
    }

    /**
     * Transfer all data to the target directory, as specified in the download requests.
     *
     * @param kvSnapshotDownloadSpecs the list of downloads.
     * @throws Exception If anything about the download goes wrong.
     */
    void transferAllDataToDirectory(
            Collection<KvSnapshotDownloadSpec> kvSnapshotDownloadSpecs,
            CloseableRegistry closeableRegistry)
            throws Exception {
        List<FileDownloadSpec> fileDownloadSpecs = new ArrayList<>();
        for (KvSnapshotDownloadSpec kvSnapshotDownloadSpec : kvSnapshotDownloadSpecs) {
            KvSnapshotHandle kvSnapshotHandle = kvSnapshotDownloadSpec.getKvSnapshotHandle();
            List<FsPathAndFileName> fsPathAndFileNames =
                    Stream.concat(
                                    kvSnapshotHandle.getSharedKvFileHandles().stream(),
                                    kvSnapshotHandle.getPrivateFileHandles().stream())
                            .map(
                                    kvFileHandleAndLocalPath ->
                                            new FsPathAndFileName(
                                                    new FsPath(
                                                            kvFileHandleAndLocalPath
                                                                    .getKvFileHandle()
                                                                    .getFilePath()),
                                                    kvFileHandleAndLocalPath.getLocalPath()))
                            .collect(Collectors.toList());
            fileDownloadSpecs.add(
                    new FileDownloadSpec(
                            fsPathAndFileNames, kvSnapshotDownloadSpec.getDownloadDestination()));
        }
        FileDownloadUtils.transferAllDataToDirectory(
                fileDownloadSpecs, closeableRegistry, dataTransferThreadPool);
    }
}
