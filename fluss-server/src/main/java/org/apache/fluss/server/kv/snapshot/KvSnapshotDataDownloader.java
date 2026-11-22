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
import org.apache.fluss.utils.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
            List<KvFileHandleAndLocalPath> handles = fileHandles(kvSnapshotHandle);
            List<FsPathAndFileName> fsPathAndFileNames = new ArrayList<>(handles.size());
            for (KvFileHandleAndLocalPath handle : handles) {
                resolveSafeLocalPath(
                        kvSnapshotDownloadSpec.getDownloadDestination(), handle.getLocalPath());
                fsPathAndFileNames.add(
                        new FsPathAndFileName(
                                new FsPath(handle.getKvFileHandle().getFilePath()),
                                handle.getLocalPath()));
            }
            fileDownloadSpecs.add(
                    new FileDownloadSpec(
                            fsPathAndFileNames, kvSnapshotDownloadSpec.getDownloadDestination()));
        }
        try {
            FileDownloadUtils.transferAllDataToDirectory(
                    fileDownloadSpecs, closeableRegistry, dataTransferThreadPool);
            verifyDownloadedFiles(kvSnapshotDownloadSpecs);
        } catch (Exception failure) {
            for (KvSnapshotDownloadSpec downloadSpec : kvSnapshotDownloadSpecs) {
                FileUtils.deleteDirectoryQuietly(downloadSpec.getDownloadDestination().toFile());
            }
            throw failure;
        }
    }

    private static void verifyDownloadedFiles(
            Collection<KvSnapshotDownloadSpec> kvSnapshotDownloadSpecs) throws IOException {
        for (KvSnapshotDownloadSpec downloadSpec : kvSnapshotDownloadSpecs) {
            for (KvFileHandleAndLocalPath handle :
                    fileHandles(downloadSpec.getKvSnapshotHandle())) {
                String expectedSha256 = handle.getKvFileHandle().getSha256();
                if (expectedSha256 == null) {
                    continue;
                }
                Path localFile =
                        resolveSafeLocalPath(
                                downloadSpec.getDownloadDestination(), handle.getLocalPath());
                if (Files.size(localFile) != handle.getKvFileHandle().getSize()) {
                    throw new IOException(
                            "Downloaded KV snapshot file length differs: " + localFile + '.');
                }
                if (!expectedSha256.equals(sha256Hex(localFile))) {
                    throw new IOException(
                            "Downloaded KV snapshot file SHA-256 differs: " + localFile + '.');
                }
            }
        }
    }

    private static List<KvFileHandleAndLocalPath> fileHandles(KvSnapshotHandle handle) {
        return Stream.concat(
                        handle.getSharedKvFileHandles().stream(),
                        handle.getPrivateFileHandles().stream())
                .collect(Collectors.toList());
    }

    private static Path resolveSafeLocalPath(Path root, String localPath) throws IOException {
        if (localPath == null
                || localPath.isEmpty()
                || localPath.startsWith("/")
                || localPath.endsWith("/")
                || localPath.indexOf('\\') >= 0
                || localPath.contains("//")) {
            throw new IOException("Unsafe KV snapshot local path: " + localPath + '.');
        }
        String[] components = localPath.split("/", -1);
        for (String component : components) {
            if (component.isEmpty() || ".".equals(component) || "..".equals(component)) {
                throw new IOException("Unsafe KV snapshot local path: " + localPath + '.');
            }
        }
        Path normalized = Paths.get(localPath).normalize();
        Path normalizedRoot = root.toAbsolutePath().normalize();
        Path resolved = normalizedRoot.resolve(normalized).normalize();
        if (normalized.isAbsolute()
                || !localPath.equals(normalized.toString().replace('\\', '/'))
                || !resolved.startsWith(normalizedRoot)) {
            throw new IOException("Unsafe KV snapshot local path: " + localPath + '.');
        }
        return resolved;
    }

    private static String sha256Hex(Path path) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable.", e);
        }
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        byte[] hash = digest.digest();
        char[] result = new char[hash.length * 2];
        char[] alphabet = "0123456789abcdef".toCharArray();
        for (int i = 0; i < hash.length; i++) {
            int value = hash[i] & 0xff;
            result[i * 2] = alphabet[value >>> 4];
            result[i * 2 + 1] = alphabet[value & 0xf];
        }
        return new String(result);
    }
}
