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
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.fluss.shaded.netty4.io.netty.util.concurrent.Future;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SslContextFactory} and {@link SslConfig}. */
class SslContextFactoryTest {

    @TempDir private Path tempDir;

    private Path keyStore;
    private Path trustStore;

    @BeforeEach
    void setup() throws Exception {
        SelfSignedCertificate cert = TestSslUtils.generateCertificate("localhost");
        keyStore = TestSslUtils.createKeyStore(tempDir, "keystore.jks", cert);
        trustStore = TestSslUtils.createTrustStore(tempDir, "truststore.jks", cert);
    }

    @Test
    void testCreateServerSslContext() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, trustStore);

        SslContext sslContext = SslContextFactory.createServerSslContext(conf).get();
        assertThat(sslContext.isServer()).isTrue();
        assertThat(sslContext.newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols())
                .contains("TLSv1.2", "TLSv1.3");
    }

    @Test
    void testCreateClientSslContext() {
        Configuration conf = new Configuration();
        TestSslUtils.setClientSslConfig(conf, trustStore, keyStore);

        SslContext sslContext = SslContextFactory.createClientSslContext(conf).get();
        assertThat(sslContext.isClient()).isTrue();
    }

    @Test
    void testProtocolFiltering() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, null);
        conf.setString(ConfigOptions.SERVER_SSL_ENABLED_PROTOCOLS.key(), "TLSv1.2");

        SslContext sslContext = SslContextFactory.createServerSslContext(conf).get();
        assertThat(sslContext.newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols())
                .containsExactly("TLSv1.2");
    }

    @Test
    void testCipherSuiteFiltering() {
        String pinned = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, null);
        conf.setString(ConfigOptions.SERVER_SSL_ENABLED_PROTOCOLS.key(), "TLSv1.2");
        conf.setString(ConfigOptions.SERVER_SSL_CIPHER_SUITES.key(), pinned);

        SslContext sslContext = SslContextFactory.createServerSslContext(conf).get();
        assertThat(sslContext.newEngine(ByteBufAllocator.DEFAULT).getEnabledCipherSuites())
                .contains(pinned)
                .doesNotContain("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
    }

    @Test
    void testKeyPasswordFallsBackToKeystorePassword() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, null);

        SslConfig config = SslConfig.fromServerConfig(conf).get();
        assertThat(config.keyPassword()).isEqualTo(TestSslUtils.PASSWORD);

        conf.setString(ConfigOptions.SERVER_SSL_KEY_PASSWORD.key(), "key-only-password");
        config = SslConfig.fromServerConfig(conf).get();
        assertThat(config.keyPassword()).isEqualTo("key-only-password");
    }

    @Test
    void testClientSslHandlerEndpointIdentification() {
        Configuration conf = new Configuration();
        TestSslUtils.setClientSslConfig(conf, trustStore, null);
        SslContext sslContext = SslContextFactory.createClientSslContext(conf).get();

        SslHandler httpsHandler =
                SslContextFactory.createClientSslHandler(
                        sslContext, ByteBufAllocator.DEFAULT, "localhost", 9123, "https");
        assertThat(httpsHandler.engine().getSSLParameters().getEndpointIdentificationAlgorithm())
                .isEqualTo("https");

        SslHandler noVerifyHandler =
                SslContextFactory.createClientSslHandler(
                        sslContext, ByteBufAllocator.DEFAULT, "localhost", 9123, "");
        assertThat(noVerifyHandler.engine().getSSLParameters().getEndpointIdentificationAlgorithm())
                .isNull();
    }

    @Test
    void testServerSslHandlerClientAuthRequirement() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, trustStore);
        SslContext sslContext = SslContextFactory.createServerSslContext(conf).get();

        SslHandler noClientAuth =
                SslContextFactory.createServerSslHandler(
                        sslContext, ByteBufAllocator.DEFAULT, false);
        assertThat(noClientAuth.engine().getNeedClientAuth()).isFalse();

        SslHandler requireClientAuth =
                SslContextFactory.createServerSslHandler(
                        sslContext, ByteBufAllocator.DEFAULT, true);
        assertThat(requireClientAuth.engine().getNeedClientAuth()).isTrue();
    }

    @Test
    void testServerAndClientNegotiateTls() throws Exception {
        Configuration serverConf = new Configuration();
        TestSslUtils.setServerSslConfig(serverConf, keyStore, null);
        Configuration clientConf = new Configuration();
        TestSslUtils.setClientSslConfig(clientConf, trustStore, null);

        HandshakeResult result = handshake(serverConf, clientConf, false);

        assertThat(result.clientHandshake.isSuccess()).isTrue();
        assertThat(result.serverHandshake.isSuccess()).isTrue();
        // Proves real encryption was negotiated (not a NULL/plaintext cipher).
        assertThat(result.clientHandler.engine().getSession().getProtocol()).startsWith("TLS");
        assertThat(result.clientHandler.engine().getSession().getCipherSuite())
                .doesNotContain("NULL");
    }

    @Test
    void testClientAuthRejectsClientWithoutCertificate() throws Exception {
        Configuration serverConf = new Configuration();
        TestSslUtils.setServerSslConfig(serverConf, keyStore, trustStore);
        TestSslUtils.setMutualTlsProtocolMap(serverConf);
        Configuration clientConf = new Configuration();
        // trusts the server, but presents no certificate of its own.
        TestSslUtils.setClientSslConfig(clientConf, trustStore, null);

        HandshakeResult result = handshake(serverConf, clientConf, true);

        // The server is the side that enforces client auth. Under TLS 1.3 the client sends its
        // Finished before the server validates the (missing) certificate, so the client's
        // handshake future completes successfully and only learns of the rejection from the
        // alert that follows.
        assertThat(result.serverHandshake.isSuccess()).isFalse();
        assertThat(result.serverHandshake.cause())
                .hasMessageContaining("Empty client certificate chain");
    }

    @Test
    void testClientAuthAcceptsClientWithCertificate() throws Exception {
        Configuration serverConf = new Configuration();
        TestSslUtils.setServerSslConfig(serverConf, keyStore, trustStore);
        TestSslUtils.setMutualTlsProtocolMap(serverConf);
        Configuration clientConf = new Configuration();
        TestSslUtils.setClientSslConfig(clientConf, trustStore, keyStore);

        HandshakeResult result = handshake(serverConf, clientConf, true);

        assertThat(result.clientHandshake.isSuccess()).isTrue();
        assertThat(result.serverHandshake.isSuccess()).isTrue();
    }

    @Test
    void testPkcs12KeyStoreAndTrustStore() throws Exception {
        SelfSignedCertificate cert = TestSslUtils.generateCertificate("localhost");
        Path p12KeyStore = TestSslUtils.createKeyStore(tempDir, "keystore.p12", "PKCS12", cert);
        Path p12TrustStore =
                TestSslUtils.createTrustStore(tempDir, "truststore.p12", "PKCS12", cert);

        Configuration serverConf = new Configuration();
        TestSslUtils.setServerSslConfig(serverConf, p12KeyStore, p12TrustStore);
        serverConf.setString(ConfigOptions.SERVER_SSL_KEYSTORE_TYPE.key(), "PKCS12");
        serverConf.setString(ConfigOptions.SERVER_SSL_TRUSTSTORE_TYPE.key(), "PKCS12");
        Configuration clientConf = new Configuration();
        TestSslUtils.setClientSslConfig(clientConf, p12TrustStore, p12KeyStore);
        clientConf.setString(ConfigOptions.CLIENT_SSL_KEYSTORE_TYPE.key(), "PKCS12");
        clientConf.setString(ConfigOptions.CLIENT_SSL_TRUSTSTORE_TYPE.key(), "PKCS12");

        HandshakeResult result = handshake(serverConf, clientConf, true);

        assertThat(result.clientHandshake.isSuccess()).isTrue();
        assertThat(result.serverHandshake.isSuccess()).isTrue();
    }

    /** The outcome of a full embedded-channel handshake between a server and a client handler. */
    private static class HandshakeResult {
        private final Future<Channel> serverHandshake;
        private final Future<Channel> clientHandshake;
        private final SslHandler clientHandler;

        private HandshakeResult(
                Future<Channel> serverHandshake,
                Future<Channel> clientHandshake,
                SslHandler clientHandler) {
            this.serverHandshake = serverHandshake;
            this.clientHandshake = clientHandshake;
            this.clientHandler = clientHandler;
        }
    }

    /**
     * Pump a TLS handshake between a server and a client handler built from the given
     * configurations, and return both handshake futures once they settle.
     */
    private static HandshakeResult handshake(
            Configuration serverConf, Configuration clientConf, boolean requireClientAuth) {
        SslHandler serverHandler =
                SslContextFactory.createServerSslHandler(
                        SslContextFactory.createServerSslContext(serverConf).get(),
                        ByteBufAllocator.DEFAULT,
                        requireClientAuth);
        // endpoint identification disabled: these tests are about the certificate exchange.
        SslHandler clientHandler =
                SslContextFactory.createClientSslHandler(
                        SslContextFactory.createClientSslContext(clientConf).get(),
                        ByteBufAllocator.DEFAULT,
                        "localhost",
                        9123,
                        "");

        EmbeddedChannel serverChannel = new EmbeddedChannel(serverHandler);
        EmbeddedChannel clientChannel = new EmbeddedChannel(clientHandler);
        try {
            for (int i = 0;
                    i < 20
                            && !(clientHandler.handshakeFuture().isDone()
                                    && serverHandler.handshakeFuture().isDone());
                    i++) {
                transferOutbound(clientChannel, serverChannel);
                transferOutbound(serverChannel, clientChannel);
            }
            return new HandshakeResult(
                    serverHandler.handshakeFuture(),
                    clientHandler.handshakeFuture(),
                    clientHandler);
        } catch (Throwable rejected) {
            // A rejected handshake also propagates through the channel that decoded the alert.
            // The handshake futures already carry the outcome, which is what callers assert on.
            return new HandshakeResult(
                    serverHandler.handshakeFuture(),
                    clientHandler.handshakeFuture(),
                    clientHandler);
        } finally {
            releaseQuietly(clientChannel);
            releaseQuietly(serverChannel);
        }
    }

    /** {@link EmbeddedChannel#finishAndReleaseAll()} rethrows a failed handshake; ignore it. */
    private static void releaseQuietly(EmbeddedChannel channel) {
        try {
            channel.finishAndReleaseAll();
        } catch (Throwable ignored) {
            // asserted through the handshake futures instead.
        }
    }

    private static void transferOutbound(EmbeddedChannel from, EmbeddedChannel to) {
        Object msg;
        while ((msg = from.readOutbound()) != null) {
            to.writeInbound(msg);
        }
    }

    @Test
    void testServerConfigRequiresKeystore() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.SERVER_SSL_ENABLED_LISTENERS.key(), TestSslUtils.TLS_LISTENER);
        assertThatThrownBy(() -> SslConfig.fromServerConfig(conf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.SERVER_SSL_KEYSTORE_PATH.key());
    }

    @Test
    void testServerConfigRequiresTruststoreForMtlsListener() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, null);
        TestSslUtils.setMutualTlsProtocolMap(conf);

        // without a truststore the server would validate client certificates against the JVM
        // default truststore, accepting anything issued by a public CA.
        assertThatThrownBy(() -> SslConfig.fromServerConfig(conf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.SERVER_SSL_TRUSTSTORE_PATH.key())
                .hasMessageContaining(TestSslUtils.TLS_LISTENER);
    }

    @Test
    void testMtlsListenerWithTruststoreRequiresClientAuth() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, trustStore);
        TestSslUtils.setMutualTlsProtocolMap(conf);

        SslConfig config = SslConfig.fromServerConfig(conf).get();
        assertThat(config.clientAuthListeners()).containsExactly(TestSslUtils.TLS_LISTENER);
        assertThat(config.requiresClientAuth(TestSslUtils.TLS_LISTENER)).isTrue();
        assertThat(config.truststorePath()).isEqualTo(trustStore.toString());
    }

    @Test
    void testNonMtlsListenerNeedsNoTruststore() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, null);
        conf.setString(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(),
                TestSslUtils.TLS_LISTENER + ":PLAINTEXT");

        SslConfig config = SslConfig.fromServerConfig(conf).get();
        assertThat(config.clientAuthListeners()).isEmpty();
        assertThat(config.requiresClientAuth(TestSslUtils.TLS_LISTENER)).isFalse();
    }

    @Test
    void testMtlsListenerWithoutTlsNeedsNoTruststore() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, null);
        // INTERNAL is an mTLS listener but TLS is not enabled for it, so it imposes nothing here.
        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "INTERNAL:mTLS");

        SslConfig config = SslConfig.fromServerConfig(conf).get();
        assertThat(config.clientAuthListeners()).isEmpty();
    }

    @Test
    void testMtlsProtocolNameIsCaseInsensitive() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, trustStore);
        conf.setString(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(),
                TestSslUtils.TLS_LISTENER + ":MTLS");

        // AuthenticationFactory matches authentication protocol names with equalsIgnoreCase.
        assertThat(
                        SslConfig.fromServerConfig(conf)
                                .get()
                                .requiresClientAuth(TestSslUtils.TLS_LISTENER))
                .isTrue();
    }

    @Test
    void testServerConfigRejectsUnsupportedProtocol() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, null);
        conf.setString(ConfigOptions.SERVER_SSL_ENABLED_PROTOCOLS.key(), "TLSv1.2,TLSv1.4");

        // caught here rather than per connection, where the engine reports "Unsupported protocol"
        // without naming the option at fault.
        assertThatThrownBy(() -> SslConfig.fromServerConfig(conf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.SERVER_SSL_ENABLED_PROTOCOLS.key())
                .hasMessageContaining("TLSv1.4");
    }

    @Test
    void testServerConfigRejectsEmptyProtocolList() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, null);
        conf.setString(ConfigOptions.SERVER_SSL_ENABLED_PROTOCOLS.key(), "");

        // an engine built with no enabled protocol starts fine and fails every handshake.
        assertThatThrownBy(() -> SslConfig.fromServerConfig(conf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.SERVER_SSL_ENABLED_PROTOCOLS.key());
    }

    @Test
    void testServerConfigRejectsUnsupportedCipherSuite() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, null);
        conf.setString(
                ConfigOptions.SERVER_SSL_CIPHER_SUITES.key(),
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_BOGUS_CIPHER");

        assertThatThrownBy(() -> SslConfig.fromServerConfig(conf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.SERVER_SSL_CIPHER_SUITES.key())
                .hasMessageContaining("TLS_BOGUS_CIPHER");
    }

    @Test
    void testClientConfigRejectsUnsupportedProtocol() {
        Configuration conf = new Configuration();
        TestSslUtils.setClientSslConfig(conf, trustStore, null);
        conf.setString(ConfigOptions.CLIENT_SSL_ENABLED_PROTOCOLS.key(), "TLSv13");

        assertThatThrownBy(() -> SslConfig.fromClientConfig(conf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.CLIENT_SSL_ENABLED_PROTOCOLS.key())
                .hasMessageContaining("TLSv13");
    }

    @Test
    void testServerConfigEmptyWithoutEnabledListeners() {
        // key material alone does not switch TLS on: no listener is enabled.
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.SERVER_SSL_KEYSTORE_PATH.key(), keyStore.toString());
        conf.setString(ConfigOptions.SERVER_SSL_KEYSTORE_PASSWORD.key(), TestSslUtils.PASSWORD);

        assertThat(SslConfig.fromServerConfig(conf)).isNotPresent();
        assertThat(SslContextFactory.createServerSslContext(conf)).isNotPresent();
    }

    @Test
    void testClientConfigEmptyWhenSslDisabled() {
        Configuration conf = new Configuration();

        assertThat(SslConfig.fromClientConfig(conf)).isNotPresent();
        assertThat(SslContextFactory.createClientSslContext(conf)).isNotPresent();
    }

    @Test
    void testServerConfigExposesEnabledListeners() {
        Configuration conf = new Configuration();
        TestSslUtils.setServerSslConfig(conf, keyStore, null);

        assertThat(SslConfig.fromServerConfig(conf).get().enabledListeners())
                .containsExactly(TestSslUtils.TLS_LISTENER);
        assertThat(SslConfig.fromClientConfig(clientConf()).get().enabledListeners()).isEmpty();
    }

    private Configuration clientConf() {
        Configuration conf = new Configuration();
        TestSslUtils.setClientSslConfig(conf, trustStore, null);
        return conf;
    }
}
