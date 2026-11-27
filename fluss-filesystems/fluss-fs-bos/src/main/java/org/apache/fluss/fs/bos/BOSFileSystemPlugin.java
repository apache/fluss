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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemPlugin;
import org.apache.fluss.fs.bos.token.BOSSecurityTokenReceiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Plugin for Baidu BOS ({@code bos://bucket/path}). Server-side options use the {@code bos.} (or
 * {@code fs.bos.}) prefix, dashes and dots are interchangeable:
 *
 * <pre>
 * remote.data.dir: bos://my-bucket/fluss
 * bos.endpoint: http://hkg.bcebos.com      # BOS native endpoint, not the s3.* one
 * bos.region: hkg
 * bos.access.key / bos.secret.key           # long-term credentials of the server
 * bos.sts.endpoint (default https://sts.bj.baidubce.com), bos.sts.duration.seconds (43200),
 * bos.sts.permissions (READ,LIST), bos.multipart.part.size (16MiB), bos.read.window.size (8MiB)
 * </pre>
 *
 * <p>Clients are configured with nothing: they use the token the server issues (endpoint and region
 * travel with it as addition info).
 */
public class BOSFileSystemPlugin implements FileSystemPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(BOSFileSystemPlugin.class);

    // The option keys below are shown without the "bos." / "fs.bos." prefix, see extract().

    /** BOS native endpoint, e.g. {@code http://hkg.bcebos.com}. */
    public static final String ENDPOINT = "endpoint";

    /** BOS region, e.g. {@code hkg}. */
    public static final String REGION = "region";

    /** Long-term access key of the server. Clients leave it unset and use the issued token. */
    public static final String ACCESS_KEY = "access.key";

    /** Long-term secret key of the server. */
    public static final String SECRET_KEY = "secret.key";

    /** BCE STS endpoint used to issue tokens, default {@code https://sts.bj.baidubce.com}. */
    public static final String STS_ENDPOINT = "sts.endpoint";

    /** Comma separated permissions of the issued tokens, default {@code READ,LIST}. */
    public static final String STS_PERMISSIONS = "sts.permissions";

    /** Validity of the issued tokens in seconds, default 43200. */
    public static final String STS_DURATION_SECONDS = "sts.duration.seconds";

    /** Size in bytes of one part of a multipart upload, default 16 MiB. */
    public static final String MULTIPART_PART_SIZE = "multipart.part.size";

    /** Size in bytes of one ranged read request, default 8 MiB. */
    public static final String READ_WINDOW_SIZE = "read.window.size";

    /** Maximum number of HTTP connections of the BOS client, default 200. */
    public static final String MAX_CONNECTIONS = "max.connections";

    private static final String[] PREFIXES = {"bos.", "fs.bos."};

    /**
     * Reads a numeric option. A malformed value fails with the name of the option instead of a bare
     * {@link NumberFormatException}.
     *
     * @param conf the options, already stripped of the {@code bos.} prefix
     * @param key the option key
     * @param defaultValue the value used when the option is not set
     * @return the configured value, or the default
     */
    public static long getLong(Map<String, String> conf, String key, long defaultValue) {
        String value = conf.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for bos.%s, expected a number", value, key),
                    e);
        }
    }

    @Override
    public String getScheme() {
        return BOSFileSystem.SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        String bucket = fsUri.getAuthority();
        if (bucket == null || bucket.isEmpty()) {
            throw new IOException("BOS path must be bos://<bucket>/..., got " + fsUri);
        }
        Map<String, String> conf = extract(flussConfig);
        boolean delegated = conf.get(ACCESS_KEY) == null;
        if (delegated) {
            if (BOSSecurityTokenReceiver.getCredentials() == null) {
                throw new IOException(
                        "Dynamic session credentials for Fluss: No BOS credentials (no security token received yet)");
            }
            for (Map.Entry<String, String> e :
                    BOSSecurityTokenReceiver.getAdditionInfos().entrySet()) {
                conf.putIfAbsent(e.getKey(), e.getValue());
            }
            LOG.info(
                    "No bos.access.key configured, using delegated session credentials for bucket {}",
                    bucket);
        }
        return new BOSFileSystem(bucket, conf, delegated);
    }

    static Map<String, String> extract(Configuration flussConfig) {
        Map<String, String> out = new HashMap<>();
        if (flussConfig == null) {
            return out;
        }
        for (Map.Entry<String, String> e : flussConfig.toMap().entrySet()) {
            for (String prefix : PREFIXES) {
                if (e.getKey().startsWith(prefix)) {
                    out.put(e.getKey().substring(prefix.length()).replace('-', '.'), e.getValue());
                    break;
                }
            }
        }
        return out;
    }
}
