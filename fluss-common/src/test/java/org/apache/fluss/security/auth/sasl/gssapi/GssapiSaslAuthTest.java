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

package org.apache.fluss.security.auth.sasl.gssapi;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.authenticator.SaslClientAuthenticator;
import org.apache.fluss.security.auth.sasl.authenticator.SaslServerAuthenticator;

import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sun.security.krb5.Config;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_JAAS_CONFIG;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_MECHANISM;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for SASL/GSSAPI (Kerberos) authentication using {@link MiniKdc}. */
class GssapiSaslAuthTest {
    private FlussMiniKdc kdc;
    private File workDir;
    private File keytab;

    @BeforeEach
    void setup() throws Exception {
        // Ensure JVM uses IPv4 for localhost to avoid connection issues with MiniKdc
        System.setProperty("java.net.preferIPv4Stack", "true");

        Properties conf = MiniKdc.createConf();
        kdc = new FlussMiniKdc(conf);
        kdc.start();

        Path tempDir = Files.createTempDirectory("fluss-gssapi-test-" + UUID.randomUUID());
        workDir = tempDir.toFile();
        keytab = new File(workDir, "test.keytab");
        File krb5Conf = kdc.getKrb5Conf();

        // Create principals: fluss and client (simple names to avoid hostname issues)
        kdc.createPrincipal(keytab, "fluss", "client");

        if (krb5Conf.exists()) {
            // Rewrite krb5.conf completely to force 127.0.0.1 and correct port
            String krb5Content =
                    "[libdefaults]\n"
                            + "    default_realm = "
                            + kdc.getRealm()
                            + "\n"
                            + "    udp_preference_limit = 1\n"
                            + "    kdc_tcp_port = "
                            + kdc.getPort()
                            + "\n"
                            + "\n"
                            + "[realms]\n"
                            + "    "
                            + kdc.getRealm()
                            + " = {\n"
                            + "        kdc = 127.0.0.1:"
                            + kdc.getPort()
                            + "\n"
                            + "        admin_server = 127.0.0.1:"
                            + kdc.getPort()
                            + "\n"
                            + "    }\n";

            // Write to a NEW unique file to force Config reload
            File customKrb5Conf = new File(workDir, "krb5-custom-" + UUID.randomUUID() + ".conf");
            Files.write(customKrb5Conf.toPath(), krb5Content.getBytes());
            System.setProperty("java.security.krb5.conf", customKrb5Conf.getAbsolutePath());
        }

        refreshKrb5Config();
    }

    @AfterEach
    void teardown() {
        if (kdc != null) {
            kdc.stop();
        }
        System.clearProperty("java.security.krb5.conf");
        System.clearProperty("java.net.preferIPv4Stack");
        deleteDir(workDir);
    }

    @Test
    void testGssapiAuthentication() throws Exception {
        // 1. Configure Server
        Configuration serverConf = new Configuration();
        serverConf.setString(SERVER_SASL_ENABLED_MECHANISMS_CONFIG.key(), "GSSAPI");

        String realm = kdc.getRealm();
        String serverPrincipal = String.format("fluss@%s", realm);
        String clientPrincipal = String.format("client@%s", realm);

        // Set server JAAS config (using keytab)
        String serverJaas =
                String.format(
                        "com.sun.security.auth.module.Krb5LoginModule required "
                                + "useKeyTab=true storeKey=true keyTab=\"%s\" principal=\"%s\";",
                        keytab.getAbsolutePath(), serverPrincipal);
        serverConf.setString("security.sasl.gssapi.jaas.config", serverJaas);

        SaslServerAuthenticator serverAuth = new SaslServerAuthenticator(serverConf);
        ServerAuthenticator.AuthenticateContext serverContext =
                new ServerAuthenticator.AuthenticateContext() {
                    public String ipAddress() {
                        return "127.0.0.1";
                    }

                    public String listenerName() {
                        return "CLIENT";
                    }

                    public String protocol() {
                        return "GSSAPI";
                    }
                };
        serverAuth.initialize(serverContext);

        // 2. Configure Client
        Configuration clientConf = new Configuration();
        clientConf.setString(CLIENT_SASL_MECHANISM, "GSSAPI");
        // Set client JAAS config (using keytab)
        String clientJaas =
                String.format(
                        "com.sun.security.auth.module.Krb5LoginModule required "
                                + "useKeyTab=true storeKey=true keyTab=\"%s\" principal=\"%s\";",
                        keytab.getAbsolutePath(), clientPrincipal);
        clientConf.setString(CLIENT_SASL_JAAS_CONFIG, clientJaas);

        SaslClientAuthenticator clientAuth = new SaslClientAuthenticator(clientConf);
        ClientAuthenticator.AuthenticateContext clientContext = () -> "127.0.0.1";
        clientAuth.initialize(clientContext);

        // 3. Handshake Loop
        byte[] challenge = new byte[0]; // Initial empty challenge for client
        if (clientAuth.hasInitialTokenResponse()) {
            challenge = clientAuth.authenticate(challenge);
        }

        // Simulate network exchange
        while (!clientAuth.isCompleted() && !serverAuth.isCompleted()) {
            // Server evaluates client's token
            if (challenge != null) {
                byte[] response = serverAuth.evaluateResponse(challenge);
                if (serverAuth.isCompleted()) {
                    challenge = null; // Done
                    break;
                }

                // Client evaluates server's challenge
                challenge = clientAuth.authenticate(response);
            } else {
                break;
            }
        }

        // 4. Verification
        assertThat(serverAuth.isCompleted()).isTrue();
        assertThat(clientAuth.isCompleted()).isTrue();
        assertThat(serverAuth.createPrincipal().getName()).startsWith("client");

        serverAuth.close();
        clientAuth.close();
    }

    private void refreshKrb5Config() throws Exception {
        try {
            Class<?> configClass = Class.forName("sun.security.krb5.Config");
            java.lang.reflect.Field singletonField = configClass.getDeclaredField("singleton");
            singletonField.setAccessible(true);
            singletonField.set(null, null);

            java.lang.reflect.Method refreshMethod = configClass.getMethod("refresh");
            refreshMethod.invoke(null);
        } catch (Exception e) {
            // Fallback to standard refresh if reflection fails (e.g. JDK 16+ restrictions)
            Config.refresh();
        }
    }

    private void deleteDir(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    deleteDir(f);
                }
            }
        }
        file.delete();
    }
}
