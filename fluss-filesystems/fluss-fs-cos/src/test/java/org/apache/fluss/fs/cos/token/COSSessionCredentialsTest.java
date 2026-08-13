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

import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.auth.COSSessionCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.CosNFileSystem;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class COSSessionCredentialsTest {

    private static final String SESSION_TOKEN = "test-session-token";

    @Test
    void testCosNFileSystemPreservesSessionToken() throws Exception {
        ObtainedSecurityToken token =
                new ObtainedSecurityToken(
                        "cosn",
                        CredentialsJsonSerde.toJson(
                                new Credentials(
                                        "test-access-key", "test-secret-key", SESSION_TOKEN)),
                        null,
                        additionInfos());
        new COSSecurityTokenReceiver().onNewTokensObtained(token);

        Configuration configuration = new Configuration(false);
        configuration.set(
                "fs.cosn.credentials.provider", DynamicTemporaryCOSCredentialsProvider.NAME);
        additionInfos().forEach(configuration::set);

        try (CosNFileSystem fileSystem = new CosNFileSystem()) {
            fileSystem.initialize(URI.create("cosn://test-bucket-1234567890"), configuration);

            COSCredentials credentials = getCosClientCredentials(fileSystem);
            assertThat(credentials).isInstanceOf(COSSessionCredentials.class);
            assertThat(((COSSessionCredentials) credentials).getSessionToken())
                    .isEqualTo(SESSION_TOKEN);
        }
    }

    private static COSCredentials getCosClientCredentials(CosNFileSystem fileSystem)
            throws Exception {
        Object store = getField(CosNFileSystem.class, "store").get(fileSystem);
        if (Proxy.isProxyClass(store.getClass())) {
            InvocationHandler handler = Proxy.getInvocationHandler(store);
            Object descriptor = getField(handler.getClass(), "proxyDescriptor").get(handler);
            Object proxyInfo = getField(descriptor.getClass(), "proxyInfo").get(descriptor);
            store = getField(proxyInfo.getClass(), "proxy").get(proxyInfo);
        }
        COSClient cosClient = (COSClient) getField(store.getClass(), "cosClient").get(store);
        COSCredentialsProvider provider =
                (COSCredentialsProvider) getField(COSClient.class, "credProvider").get(cosClient);
        return provider.getCredentials();
    }

    private static Field getField(Class<?> type, String name) throws Exception {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            try {
                Field field = current.getDeclaredField(name);
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException ignored) {
                // Continue with the superclass.
            }
        }
        throw new NoSuchFieldException(type.getName() + "." + name);
    }

    private static Map<String, String> additionInfos() {
        Map<String, String> additionInfos = new HashMap<>();
        additionInfos.put("fs.cosn.userinfo.region", "ap-guangzhou");
        additionInfos.put("fs.cosn.bucket.endpoint_suffix", "cos.ap-guangzhou.myqcloud.com");
        return additionInfos;
    }
}
