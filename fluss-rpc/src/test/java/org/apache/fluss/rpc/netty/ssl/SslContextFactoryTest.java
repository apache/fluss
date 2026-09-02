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
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.util.SelfSignedCertificate;

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

        SslHandler serverHandler =
                SslContextFactory.createServerSslHandler(
                        SslContextFactory.createServerSslContext(serverConf).get(),
                        ByteBufAllocator.DEFAULT,
                        false);
        // endpoint identification disabled here: this raw handshake test only checks encryption.
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
            // Pump the handshake bytes back and forth until both sides complete.
            for (int i = 0;
                    i < 20
                            && !(clientHandler.handshakeFuture().isDone()
                                    && serverHandler.handshakeFuture().isDone());
                    i++) {
                transferOutbound(clientChannel, serverChannel);
                transferOutbound(serverChannel, clientChannel);
            }

            assertThat(clientHandler.handshakeFuture().isSuccess()).isTrue();
            assertThat(serverHandler.handshakeFuture().isSuccess()).isTrue();
            // Proves real encryption was negotiated (not a NULL/plaintext cipher).
            assertThat(clientHandler.engine().getSession().getProtocol()).startsWith("TLS");
            assertThat(clientHandler.engine().getSession().getCipherSuite()).doesNotContain("NULL");
        } finally {
            clientChannel.finishAndReleaseAll();
            serverChannel.finishAndReleaseAll();
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
