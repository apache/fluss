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

package org.apache.fluss.lake.hudi.utils.meta;

import org.apache.fluss.annotation.VisibleForTesting;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StoragePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** DFS-backed checkpoint metadata used as a lightweight message bus for Hudi instants. */
public class CkpMetadata implements Serializable, AutoCloseable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CkpMetadata.class);

    private static final int MAX_RETAIN_CKP_NUM = 1;
    private static final String CKP_META = "ckp_meta";

    private final FileSystem fs;
    private final Path path;

    private List<CkpMessage> messages;
    private List<String> instantCache;

    CkpMetadata(FileSystem fs, String basePath, String uniqueId) {
        this.fs = fs;
        this.path = new Path(ckpMetaPath(basePath, uniqueId));
    }

    public void bootstrap() throws IOException {
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
    }

    public void startInstant(String instant) {
        Path instantPath = fullPath(CkpMessage.getFileName(instant, CkpMessage.State.INFLIGHT));
        try {
            fs.createNewFile(instantPath);
        } catch (IOException e) {
            throw new HoodieException(
                    "Exception while adding checkpoint start metadata for instant: " + instant, e);
        }
        cache(instant);
        clean();
    }

    public void commitInstant(String instant) {
        Path instantPath = fullPath(CkpMessage.getFileName(instant, CkpMessage.State.COMPLETED));
        try {
            fs.createNewFile(instantPath);
        } catch (IOException e) {
            throw new HoodieException(
                    "Exception while adding checkpoint commit metadata for instant: " + instant, e);
        }
    }

    public void abortInstant(String instant) {
        Path instantPath = fullPath(CkpMessage.getFileName(instant, CkpMessage.State.ABORTED));
        try {
            fs.createNewFile(instantPath);
        } catch (IOException e) {
            throw new HoodieException(
                    "Exception while adding checkpoint abort metadata for instant: " + instant, e);
        }
    }

    @Nullable
    public String lastPendingInstant() {
        load();
        if (!messages.isEmpty()) {
            CkpMessage ckpMsg = messages.get(messages.size() - 1);
            if (ckpMsg.isInflight()) {
                return ckpMsg.getInstant();
            }
        }
        return null;
    }

    public List<CkpMessage> getMessages() {
        load();
        return messages;
    }

    @Nullable
    @VisibleForTesting
    public String lastCompleteInstant() {
        load();
        for (int i = messages.size() - 1; i >= 0; i--) {
            CkpMessage ckpMsg = messages.get(i);
            if (ckpMsg.isComplete()) {
                return ckpMsg.getInstant();
            }
        }
        return null;
    }

    @VisibleForTesting
    public List<String> getInstantCache() {
        return instantCache;
    }

    @Override
    public void close() {
        instantCache = null;
    }

    private void cache(String newInstant) {
        if (instantCache == null) {
            instantCache = new ArrayList<>();
        }
        instantCache.add(newInstant);
    }

    private void clean() {
        load();
        if (messages.size() <= MAX_RETAIN_CKP_NUM) {
            return;
        }
        String instant = messages.get(0).getInstant();
        for (String fileName : CkpMessage.getAllFileNames(instant)) {
            Path filePath = fullPath(fileName);
            try {
                fs.delete(filePath, false);
                LOG.info("Delete checkpoint metadata {}", filePath);
            } catch (IOException e) {
                LOG.warn("Exception while cleaning the checkpoint metadata file: {}", filePath, e);
            }
        }
    }

    private void load() {
        try {
            messages = scanCkpMetadata(path);
        } catch (IOException e) {
            throw new HoodieException(
                    "Exception while scanning the checkpoint metadata files under path: " + path,
                    e);
        }
    }

    private Path fullPath(String fileName) {
        return new Path(path, fileName);
    }

    protected Stream<CkpMessage> fetchCkpMessages(Path ckpMetaPath) throws IOException {
        if (!fs.exists(ckpMetaPath)) {
            return Stream.empty();
        }
        return Arrays.stream(fs.listStatus(ckpMetaPath)).map(CkpMessage::new);
    }

    protected List<CkpMessage> scanCkpMetadata(Path ckpMetaPath) throws IOException {
        return fetchCkpMessages(ckpMetaPath)
                .collect(Collectors.groupingBy(CkpMessage::getInstant))
                .values()
                .stream()
                .map(
                        ckpMessages ->
                                ckpMessages.stream()
                                        .reduce(
                                                (left, right) ->
                                                        left.getState().compareTo(right.getState())
                                                                        >= 0
                                                                ? left
                                                                : right)
                                        .get())
                .sorted()
                .collect(Collectors.toList());
    }

    protected static String ckpMetaPath(String basePath, String uniqueId) {
        String metaPath =
                basePath
                        + StoragePath.SEPARATOR
                        + HoodieTableMetaClient.AUXILIARYFOLDER_NAME
                        + StoragePath.SEPARATOR
                        + CKP_META;
        return uniqueId == null || uniqueId.isEmpty() ? metaPath : metaPath + "_" + uniqueId;
    }
}
