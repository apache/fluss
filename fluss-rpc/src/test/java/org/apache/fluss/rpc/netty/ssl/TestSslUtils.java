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

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Test helpers for generating self-signed certificates and JKS keystores/truststores on the fly, so
 * TLS tests do not need committed key material. Backed by Netty's {@link SelfSignedCertificate},
 * which generates the certificate with BouncyCastle when it is on the classpath — hence the
 * test-scoped {@code bcpkix-jdk18on} dependency. Its fallback generator reaches into {@code
 * sun.security.x509}, which needs {@code --add-opens} from JDK 9 on and stops working altogether
 * from JDK 18 on.
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

    /**
     * Generate a self-signed key and certificate for {@code fqdn} that lists {@code fqdn} and the
     * given IP addresses as subject alternative names (SAN).
     *
     * <p>{@link #generateCertificate} sets no SAN, and hostname verification matches an IP address
     * only against an IP SAN. A client that connects by IP address needs this variant.
     */
    public static KeyStore.PrivateKeyEntry generateCertificateWithSans(
            String fqdn, String... ipAddresses) throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        X500Name name = new X500Name("CN=" + fqdn);
        long now = System.currentTimeMillis();
        List<GeneralName> sans = new ArrayList<>();
        sans.add(new GeneralName(GeneralName.dNSName, fqdn));
        for (String ipAddress : ipAddresses) {
            sans.add(new GeneralName(GeneralName.iPAddress, ipAddress));
        }
        X509CertificateHolder holder =
                new JcaX509v3CertificateBuilder(
                                name,
                                BigInteger.valueOf(now),
                                new Date(now - 86_400_000L),
                                new Date(now + 86_400_000L),
                                name,
                                keyPair.getPublic())
                        .addExtension(
                                Extension.subjectAlternativeName,
                                false,
                                new GeneralNames(sans.toArray(new GeneralName[0])))
                        .build(
                                new JcaContentSignerBuilder("SHA256withRSA")
                                        .build(keyPair.getPrivate()));
        return new KeyStore.PrivateKeyEntry(
                keyPair.getPrivate(),
                new Certificate[] {new JcaX509CertificateConverter().getCertificate(holder)});
    }

    /** Create a JKS keystore file holding the private key and certificate of {@code cert}. */
    public static Path createKeyStore(Path dir, String fileName, SelfSignedCertificate cert)
            throws Exception {
        return createKeyStore(dir, fileName, "JKS", cert);
    }

    /**
     * Create a keystore file of the given {@code storeType} (as accepted by {@code
     * security.ssl.keystore.type}) holding the private key and certificate of {@code cert}.
     */
    public static Path createKeyStore(
            Path dir, String fileName, String storeType, SelfSignedCertificate cert)
            throws Exception {
        return createKeyStore(dir, fileName, storeType, cert.key(), cert.cert());
    }

    /**
     * Create a keystore file of the given {@code storeType} holding {@code key} and {@code cert}.
     */
    public static Path createKeyStore(
            Path dir, String fileName, String storeType, PrivateKey key, Certificate cert)
            throws Exception {
        KeyStore keyStore = KeyStore.getInstance(storeType);
        keyStore.load(null, null);
        keyStore.setKeyEntry("key", key, PASSWORD.toCharArray(), new Certificate[] {cert});
        return store(dir, fileName, keyStore);
    }

    /** Create a JKS truststore file trusting the certificate of {@code cert}. */
    public static Path createTrustStore(Path dir, String fileName, SelfSignedCertificate cert)
            throws Exception {
        return createTrustStore(dir, fileName, "JKS", cert);
    }

    /**
     * Create a truststore file of the given {@code storeType} (as accepted by {@code
     * security.ssl.truststore.type}) trusting the certificate of {@code cert}.
     */
    public static Path createTrustStore(
            Path dir, String fileName, String storeType, SelfSignedCertificate cert)
            throws Exception {
        return createTrustStore(dir, fileName, storeType, cert.cert());
    }

    /** Create a truststore file of the given {@code storeType} trusting {@code cert}. */
    public static Path createTrustStore(
            Path dir, String fileName, String storeType, Certificate cert) throws Exception {
        KeyStore trustStore = KeyStore.getInstance(storeType);
        trustStore.load(null, null);
        trustStore.setCertificateEntry("cert", cert);
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
     * null}, a truststore — needed for mTLS listeners). Mark the listener as requiring a client
     * certificate with {@link #setMutualTlsProtocolMap}.
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
     * Make {@link #TLS_LISTENER} an {@code mTLS} listener via {@code security.protocol.map}, i.e.
     * one that requires a client certificate.
     */
    public static void setMutualTlsProtocolMap(Configuration conf) {
        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), TLS_LISTENER + ":mTLS");
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
