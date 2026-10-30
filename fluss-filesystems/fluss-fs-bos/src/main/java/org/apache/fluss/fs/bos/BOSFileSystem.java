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

package org.apache.fluss.fs.bos;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.bos.token.BOSSecurityTokenProvider;
import org.apache.fluss.fs.bos.token.DynamicTemporaryBOSCredentialsProvider;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.baidubce.BceServiceException;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.ObjectMetadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.ACCESS_KEY;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.ENDPOINT;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.MAX_CONNECTIONS;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.MULTIPART_PART_SIZE;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.READ_WINDOW_SIZE;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.SECRET_KEY;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.getLong;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Fluss {@link FileSystem} for Baidu BOS, implemented directly on the BOS Java SDK. Directories are
 * key prefixes, an empty directory is an empty marker object {@code dir/} written by {@code
 * mkdirs}; {@code rename} is copy + delete and therefore not atomic.
 *
 * <p>Two modes: on the server the file system uses the configured long-term access key and can
 * issue delegation tokens; on a client (no access key configured) it signs every request with the
 * latest token pushed to {@code BOSSecurityTokenReceiver}.
 */
public class BOSFileSystem extends FileSystem {
    public static final String SCHEME = "bos";

    private final String bucket;
    private final Map<String, String> conf;
    private final boolean delegated;
    private final BosClient client;
    private final int partSize;
    private final long readWindow;
    private volatile BOSSecurityTokenProvider tokenProvider;

    BOSFileSystem(String bucket, Map<String, String> conf, boolean delegated) {
        this(bucket, conf, delegated, createClient(conf, delegated));
    }

    /** Package-private so tests can inject a fake client. */
    BOSFileSystem(String bucket, Map<String, String> conf, boolean delegated, BosClient client) {
        this.bucket = bucket;
        this.conf = conf;
        this.delegated = delegated;
        this.client = client;
        long part = getLong(conf, MULTIPART_PART_SIZE, 16 * 1024 * 1024);
        checkArgument(
                part > 0 && part <= Integer.MAX_VALUE,
                "bos.%s must be between 1 and %s, got %s",
                MULTIPART_PART_SIZE,
                Integer.MAX_VALUE,
                part);
        this.partSize = (int) part;
        this.readWindow = getLong(conf, READ_WINDOW_SIZE, 8 * 1024 * 1024);
        checkArgument(
                readWindow > 0, "bos.%s must be positive, got %s", READ_WINDOW_SIZE, readWindow);
    }

    private static BosClient createClient(Map<String, String> conf, boolean delegated) {
        String endpoint = conf.get(ENDPOINT);
        checkArgument(endpoint != null, "bos.endpoint is not set (e.g. http://hkg.bcebos.com)");
        BosClientConfiguration cfg = new BosClientConfiguration();
        cfg.setEndpoint(endpoint);
        cfg.setCredentials(
                delegated
                        ? new DynamicTemporaryBOSCredentialsProvider()
                        : new DefaultBceCredentials(conf.get(ACCESS_KEY), conf.get(SECRET_KEY)));
        long maxConnections = getLong(conf, MAX_CONNECTIONS, 200);
        checkArgument(
                maxConnections > 0 && maxConnections <= Integer.MAX_VALUE,
                "bos.%s must be a positive integer, got %s",
                MAX_CONNECTIONS,
                maxConnections);
        cfg.setMaxConnections((int) maxConnections);
        return new BosClient(cfg);
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        if (delegated) {
            throw new IOException(
                    "A client-side BOS file system (no access key configured) cannot issue security tokens");
        }
        if (tokenProvider == null) {
            synchronized (this) {
                if (tokenProvider == null) {
                    tokenProvider = new BOSSecurityTokenProvider(conf, bucket);
                }
            }
        }
        return tokenProvider.obtainSecurityToken(SCHEME);
    }

    @Override
    public URI getUri() {
        return URI.create(SCHEME + "://" + bucket);
    }

    // ---------------------------------------------------------------- path helpers

    private String key(FsPath path) throws IOException {
        URI u = path.toUri();
        if (u.getAuthority() != null && !u.getAuthority().equals(bucket)) {
            throw new IOException("Path " + path + " does not belong to bucket " + bucket);
        }
        String p = u.getPath() == null ? "" : u.getPath();
        while (p.startsWith("/")) {
            p = p.substring(1);
        }
        while (p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
    }

    private FsPath toPath(String key) {
        return new FsPath(SCHEME, bucket, "/" + key);
    }

    private static IOException wrap(String op, Object target, RuntimeException e) {
        if (e instanceof BceServiceException && ((BceServiceException) e).getStatusCode() == 404) {
            FileNotFoundException f =
                    new FileNotFoundException(target + ": " + op + " (404 not found)");
            f.initCause(e);
            return f;
        }
        return new IOException(op + " " + target + " failed: " + e.getMessage(), e);
    }

    private ListObjectsResponse list(String prefix, String delimiter, String marker, int max) {
        ListObjectsRequest req = new ListObjectsRequest(bucket);
        req.setPrefix(prefix);
        if (delimiter != null) {
            req.setDelimiter(delimiter);
        }
        if (marker != null) {
            req.setMarker(marker);
        }
        req.setMaxKeys(max);
        return client.listObjects(req);
    }

    /**
     * Whether anything lives under {@code key/}, including the directory's own marker object.
     *
     * <p>Listings on BOS are not read-your-deletes: observed on BOS (2026-10-19, 2026-10-20) a list
     * can keep returning objects, directory markers and prefixes that were already deleted for
     * seconds (with {@code maxKeys=1} for much longer), while HEAD is always right. So every listed
     * object is confirmed with a HEAD and every listed prefix is checked recursively.
     */
    private boolean hasChildren(String key) {
        return hasEntries(key.isEmpty() ? "" : key + "/", true);
    }

    /**
     * Like {@link #hasChildren}, but the directory's own marker object {@code key/} does not count.
     */
    private boolean hasEntriesBesidesMarker(String key) {
        return hasEntries(key + "/", false);
    }

    private boolean hasEntries(String prefix, boolean countMarker) {
        String marker = null;
        while (true) {
            ListObjectsResponse r = list(prefix, "/", marker, 100);
            for (BosObjectSummary o : r.getContents()) {
                if ((countMarker || !o.getKey().equals(prefix))
                        && client.doesObjectExist(bucket, o.getKey())) {
                    return true;
                }
            }
            for (String commonPrefix : r.getCommonPrefixes()) {
                if (hasEntries(commonPrefix, true)) {
                    return true;
                }
            }
            if (!r.isTruncated()) {
                return false;
            }
            marker = r.getNextMarker();
        }
    }

    // ---------------------------------------------------------------- FileSystem API

    @Override
    public FileStatus getFileStatus(FsPath path) throws IOException {
        String key = key(path);
        try {
            if (key.isEmpty()) {
                return BOSFileStatus.dir(path);
            }
            try {
                ObjectMetadata m = client.getObjectMetadata(bucket, key);
                return BOSFileStatus.file(path, m.getContentLength(), m.getLastModified());
            } catch (BceServiceException e) {
                if (e.getStatusCode() != 404) {
                    throw e;
                }
            }
            if (hasChildren(key)) {
                return BOSFileStatus.dir(path);
            }
        } catch (RuntimeException e) {
            throw wrap("getFileStatus", path, e);
        }
        throw new FileNotFoundException("No such file or directory: " + path);
    }

    @Override
    public FSDataInputStream open(FsPath path) throws IOException {
        String key = key(path);
        try {
            ObjectMetadata m = client.getObjectMetadata(bucket, key);
            return new BOSInputStream(client, bucket, key, m.getContentLength(), readWindow);
        } catch (RuntimeException e) {
            throw wrap("open", path, e);
        }
    }

    /**
     * Lists the direct children of a directory.
     *
     * <p>Listings on BOS are not read-your-deletes, so the result can still contain objects or
     * prefixes that were deleted a few seconds ago. Use {@link #exists} or {@link #getFileStatus}
     * when an accurate answer for a single path is needed, they confirm with a HEAD request.
     */
    @Override
    public FileStatus[] listStatus(FsPath path) throws IOException {
        FileStatus st = getFileStatus(path);
        if (!st.isDir()) {
            return new FileStatus[] {st};
        }
        String key = key(path);
        String prefix = key.isEmpty() ? "" : key + "/";
        List<FileStatus> out = new ArrayList<>();
        try {
            String marker = null;
            while (true) {
                ListObjectsResponse r = list(prefix, "/", marker, 1000);
                for (BosObjectSummary o : r.getContents()) {
                    if (!o.getKey().equals(prefix)) {
                        out.add(
                                BOSFileStatus.file(
                                        toPath(o.getKey()), o.getSize(), o.getLastModified()));
                    }
                }
                for (String cp : r.getCommonPrefixes()) {
                    out.add(
                            BOSFileStatus.dir(
                                    toPath(
                                            cp.endsWith("/")
                                                    ? cp.substring(0, cp.length() - 1)
                                                    : cp)));
                }
                if (!r.isTruncated()) {
                    break;
                }
                marker = r.getNextMarker();
            }
        } catch (RuntimeException e) {
            throw wrap("listStatus", path, e);
        }
        return out.toArray(new FileStatus[0]);
    }

    @Override
    public FSDataOutputStream create(FsPath path, WriteMode overwriteMode) throws IOException {
        String key = key(path);
        if (key.isEmpty()) {
            throw new IOException("Cannot write to the bucket root");
        }
        if (overwriteMode == WriteMode.NO_OVERWRITE && exists(path)) {
            throw new FileAlreadyExistsException(path.toString());
        }
        return new BOSOutputStream(client, bucket, key, partSize);
    }

    @Override
    public boolean delete(FsPath path, boolean recursive) throws IOException {
        String key = key(path);
        if (key.isEmpty()) {
            throw new IOException("Refusing to delete the bucket root");
        }
        FileStatus st;
        try {
            st = getFileStatus(path);
        } catch (FileNotFoundException e) {
            return false;
        }
        try {
            if (!st.isDir()) {
                client.deleteObject(bucket, key);
                return true;
            }
            if (!recursive && hasEntriesBesidesMarker(key)) {
                throw new IOException("Directory " + path + " is not empty");
            }
            deleteAllUnder(key + "/");
            return true;
        } catch (RuntimeException e) {
            throw wrap("delete", path, e);
        }
    }

    private void deleteAllUnder(String prefix) {
        String marker = null;
        while (true) {
            ListObjectsResponse r = list(prefix, null, marker, 1000);
            for (BosObjectSummary o : r.getContents()) {
                client.deleteObject(bucket, o.getKey());
            }
            if (!r.isTruncated()) {
                break;
            }
            marker = r.getNextMarker();
        }
    }

    /**
     * Creates the directory as an empty marker object {@code key/}, like S3A does, so that an empty
     * directory exists. Fails if the path itself or any of its ancestors is a file.
     */
    @Override
    public boolean mkdirs(FsPath path) throws IOException {
        String key = key(path);
        if (key.isEmpty()) {
            return true;
        }
        try {
            if (client.doesObjectExist(bucket, key)) {
                throw new FileAlreadyExistsException(path + " exists and is a file");
            }
            if (hasChildren(key)) {
                return true; // already a directory, so are its ancestors
            }
            StringBuilder ancestor = new StringBuilder();
            String[] parts = key.split("/");
            for (int i = 0; i < parts.length - 1; i++) {
                ancestor.append(i == 0 ? "" : "/").append(parts[i]);
                if (client.doesObjectExist(bucket, ancestor.toString())) {
                    throw new FileAlreadyExistsException(
                            "Cannot create "
                                    + path
                                    + ": ancestor bos://"
                                    + bucket
                                    + "/"
                                    + ancestor
                                    + " is a file");
                }
            }
            client.putObject(bucket, key + "/", new byte[0]);
            return true;
        } catch (RuntimeException e) {
            throw wrap("mkdirs", path, e);
        }
    }

    /**
     * Renames a file or a directory by copying every object to the new key and deleting the old
     * one. BOS flat buckets have no native rename, so the operation is not atomic: a failure in the
     * middle can leave objects under both paths, and concurrent readers can observe the
     * intermediate state. Returns {@code false} if the source does not exist or the destination
     * already exists.
     */
    @Override
    public boolean rename(FsPath src, FsPath dst) throws IOException {
        String s = key(src);
        String d = key(dst);
        if (s.isEmpty() || d.isEmpty()) {
            throw new IOException("Cannot rename the bucket root");
        }
        FileStatus st;
        try {
            st = getFileStatus(src);
        } catch (FileNotFoundException e) {
            return false;
        }
        try {
            try {
                getFileStatus(dst);
                return false; // destination exists
            } catch (FileNotFoundException expected) {
                // free to use
            }
            if (!st.isDir()) {
                client.copyObject(bucket, s, bucket, d);
                client.deleteObject(bucket, s);
                return true;
            }
            String sp = s + "/";
            String dp = d + "/";
            List<String> moved = new ArrayList<>();
            String marker = null;
            while (true) {
                ListObjectsResponse r = list(sp, null, marker, 1000);
                for (BosObjectSummary o : r.getContents()) {
                    client.copyObject(
                            bucket, o.getKey(), bucket, dp + o.getKey().substring(sp.length()));
                    moved.add(o.getKey());
                }
                if (!r.isTruncated()) {
                    break;
                }
                marker = r.getNextMarker();
            }
            for (String k : moved) {
                client.deleteObject(bucket, k);
            }
            return true;
        } catch (RuntimeException e) {
            throw wrap("rename", src + " -> " + dst, e);
        }
    }
}
