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

package org.apache.fluss.rpc.netty.ssl;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.util.SelfSignedCertificate;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;

/**
 * Test helpers for generating self-signed certificates and JKS keystores/truststores on the fly, so
 * TLS tests do not need committed key material. Backed by Netty's {@link SelfSignedCertificate}.
 */
public class TestSslUtils {

    public static final String PASSWORD = "test-password";

    /** The listener name {@link #setServerSslConfig} enables TLS for. */
    public static final String TLS_LISTENER = "CLIENT";

    private TestSslUtils() {}

    /** Generate a self-signed certificate whose subject CN is {@code fqdn}. */
    public static SelfSignedCertificate generateCertificate(String fqdn) throws Exception {
        return new SelfSignedCertificate(fqdn);
    }

    /** Create a JKS keystore file holding the private key and certificate of {@code cert}. */
    public static Path createKeyStore(Path dir, String fileName, SelfSignedCertificate cert)
            throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setKeyEntry(
                "key", cert.key(), PASSWORD.toCharArray(), new Certificate[] {cert.cert()});
        return store(dir, fileName, keyStore);
    }

    /** Create a JKS truststore file trusting the certificate of {@code cert}. */
    public static Path createTrustStore(Path dir, String fileName, SelfSignedCertificate cert)
            throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("cert", cert.cert());
        return store(dir, fileName, trustStore);
    }

    private static Path store(Path dir, String fileName, KeyStore keyStore) throws Exception {
        Path path = dir.resolve(fileName);
        try (OutputStream out = Files.newOutputStream(path)) {
            keyStore.store(out, PASSWORD.toCharArray());
        }
        return path;
    }

    /**
     * Populate {@code conf} with the server-side {@code security.ssl.*} transport options: TLS
     * enabled for the {@link #TLS_LISTENER} listener, a keystore (and, when {@code trustStore !=
     * null}, a truststore — needed for mTLS listeners). The per-listener client-certificate
     * requirement is derived from {@code security.protocol.map} ({@code mTLS}), which the caller
     * sets.
     */
    public static void setServerSslConfig(Configuration conf, Path keyStore, Path trustStore) {
        conf.setString(ConfigOptions.SERVER_SSL_ENABLED_LISTENERS.key(), TLS_LISTENER);
        conf.setString(ConfigOptions.SERVER_SSL_KEYSTORE_PATH.key(), keyStore.toString());
        conf.setString(ConfigOptions.SERVER_SSL_KEYSTORE_PASSWORD.key(), PASSWORD);
        if (trustStore != null) {
            conf.setString(ConfigOptions.SERVER_SSL_TRUSTSTORE_PATH.key(), trustStore.toString());
            conf.setString(ConfigOptions.SERVER_SSL_TRUSTSTORE_PASSWORD.key(), PASSWORD);
        }
    }

    /**
     * Populate {@code conf} with the client-side {@code client.security.ssl.*} options for a TLS
     * truststore (and, when {@code keyStore != null}, a keystore for mutual TLS).
     */
    public static void setClientSslConfig(Configuration conf, Path trustStore, Path keyStore) {
        conf.setBoolean(ConfigOptions.CLIENT_SSL_ENABLED.key(), true);
        if (trustStore != null) {
            conf.setString(ConfigOptions.CLIENT_SSL_TRUSTSTORE_PATH.key(), trustStore.toString());
            conf.setString(ConfigOptions.CLIENT_SSL_TRUSTSTORE_PASSWORD.key(), PASSWORD);
        }
        if (keyStore != null) {
            conf.setString(ConfigOptions.CLIENT_SSL_KEYSTORE_PATH.key(), keyStore.toString());
            conf.setString(ConfigOptions.CLIENT_SSL_KEYSTORE_PASSWORD.key(), PASSWORD);
        }
    }
}
