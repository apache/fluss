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

package org.apache.fluss.fs.cos.token;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.auth.COSSessionCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.CosNFileSystem;
import org.apache.hadoop.fs.cosn.CosNUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Restores COS STS session credentials that {@code hadoop-cos} 3.3.5 drops while constructing
 * {@link COSClient}.
 *
 * <p>{@code CosNativeFileSystemStore#initCOSClient} copies only the access key and secret key into
 * {@code BasicCOSCredentials}, discarding the session token (HADOOP-19648). Hadoop 3.5.0 fixes this
 * by passing the original credentials through, but that release requires Java 17. Fluss still has
 * to compile on Java 8, so the session token is put back onto the live {@link COSClient} after
 * {@link CosNFileSystem#initialize(URI, Configuration)}.
 */
@Internal
public final class CosNSessionCredentialsRestorer {

    private static final Logger LOG = LoggerFactory.getLogger(CosNSessionCredentialsRestorer.class);

    private CosNSessionCredentialsRestorer() {}

    /**
     * Replaces the COS client's credential provider when the configured provider yields session
     * credentials. Permanent access-key credentials are left unchanged.
     *
     * @param fileSystem the already initialized CosN file system
     * @throws IOException if session credentials cannot be restored
     */
    public static void restore(CosNFileSystem fileSystem) throws IOException {
        checkNotNull(fileSystem, "fileSystem");
        URI uri = fileSystem.getUri();
        Configuration conf = fileSystem.getConf();
        COSCredentialsProvider provider = CosNUtils.createCosCredentialsProviderSet(uri, conf);
        COSCredentials credentials = provider.getCredentials();
        if (!(credentials instanceof COSSessionCredentials)) {
            return;
        }

        try {
            COSClient cosClient = getCosClient(fileSystem);
            cosClient.setCOSCredentialsProvider(provider);
            LOG.info(
                    "Restored COS session credentials on CosNFileSystem to preserve the STS session"
                            + " token dropped by hadoop-cos 3.3.5.");
        } catch (Exception e) {
            throw new IOException(
                    "Failed to restore COS session credentials dropped by hadoop-cos 3.3.5"
                            + " (HADOOP-19648). The STS session token is required for temporary"
                            + " credentials.",
                    e);
        }
    }

    /** Returns the credentials currently installed on the CosN COS client. */
    @VisibleForTesting
    static COSCredentials getCosClientCredentials(CosNFileSystem fileSystem) throws Exception {
        COSClient cosClient = getCosClient(fileSystem);
        COSCredentialsProvider provider =
                (COSCredentialsProvider) getFieldValue(cosClient, "credProvider");
        return provider.getCredentials();
    }

    private static COSClient getCosClient(CosNFileSystem fileSystem) throws Exception {
        Object store = unwrapStore(getFieldValue(fileSystem, "store"));
        COSClient cosClient = (COSClient) getFieldValue(store, "cosClient");
        if (cosClient == null) {
            throw new IllegalStateException("CosNFileSystem COS client is not initialized");
        }
        return cosClient;
    }

    private static Object unwrapStore(Object store) throws Exception {
        if (store == null) {
            throw new IllegalStateException("CosNFileSystem store is not initialized");
        }
        if (!Proxy.isProxyClass(store.getClass())) {
            return store;
        }

        InvocationHandler handler = Proxy.getInvocationHandler(store);
        Object descriptor = getFieldValue(handler, "proxyDescriptor");
        try {
            Method getProxy = descriptor.getClass().getDeclaredMethod("getProxy");
            getProxy.setAccessible(true);
            return getProxy.invoke(descriptor);
        } catch (NoSuchMethodException ignored) {
            Object proxyInfo = getFieldValue(descriptor, "proxyInfo");
            return getFieldValue(proxyInfo, "proxy");
        }
    }

    private static Object getFieldValue(Object target, String name) throws Exception {
        Field field = findField(target.getClass(), name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static Field findField(Class<?> type, String name) throws NoSuchFieldException {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            try {
                return current.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                // Continue with the superclass.
            }
        }
        throw new NoSuchFieldException(type.getName() + "." + name);
    }
}
