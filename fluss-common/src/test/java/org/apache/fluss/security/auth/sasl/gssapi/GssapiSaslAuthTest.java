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
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.authenticator.SaslClientAuthenticator;
import org.apache.fluss.security.auth.sasl.authenticator.SaslServerAuthenticator;

import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_JAAS_CONFIG;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_MECHANISM;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for verifying the full flow of Kerberos (GSSAPI) authentication. It spins up a
 * local MiniKdc to simulate ticket issuance and mutual authentication between a Fluss client and
 * server.
 */
class GssapiSaslAuthTest {
    private FlussMiniKdc kdc;
    private File workDir;
    private File keytab;

    @BeforeEach
    void setup() throws Exception {
        // Initialize and start an KDC server to simulate a real Kerberos environment locally.
        Properties conf = MiniKdc.createConf();
        kdc = new FlussMiniKdc(conf);
        kdc.start();

        // Prepare a temporary workspace and define the Keytab file path.
        // Kerberos authentication requires a physical Keytab file for password-less login.
        Path tempDir = Files.createTempDirectory("fluss-gssapi-test-" + UUID.randomUUID());
        workDir = tempDir.toFile();
        keytab = new File(workDir, "fluss.keytab");
        File krb5Conf = kdc.getKrb5Conf();

        // Generate principals for both server ('fluss') bound to 127.0.0.1 and client ('client').
        kdc.createPrincipal(keytab, "fluss/127.0.0.1", "client");

        // Overwrite the default krb5.conf if it exists. MiniKdc defaults to "localhost",
        // but we enforce "127.0.0.1" and TCP (udp_preference_limit=1) to ensure stable connections.
        if (krb5Conf.exists()) {
            String krb5Content =
                    "[libdefaults]\n"
                            + "    default_realm = "
                            + kdc.getRealm()
                            + "\n"
                            + "    udp_preference_limit = 1\n" // Force TCP usage
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

            // Save to a unique filename to bypass JVM's internal configuration caching
            // and force it to recognize the new settings.
            File customKrb5Conf = new File(workDir, "krb5-custom-" + UUID.randomUUID() + ".conf");
            Files.write(customKrb5Conf.toPath(), krb5Content.getBytes());

            // Point the JVM to use our custom krb5.conf for Kerberos operations.
            System.setProperty("java.security.krb5.conf", customKrb5Conf.getAbsolutePath());
        }
    }

    @AfterEach
    void teardown() {
        if (kdc != null) {
            kdc.stop();
        }
        System.clearProperty("java.security.krb5.conf");
        deleteDir(workDir);
    }

    @Test
    void testGssapiAuthentication() throws Exception {
        String realm = kdc.getRealm();
        String serverPrincipal = String.format("fluss/127.0.0.1@%s", realm);
        String clientPrincipal = String.format("client@%s", realm);

        Configuration serverConf = new Configuration();
        serverConf.setString(SERVER_SASL_ENABLED_MECHANISMS_CONFIG.key(), "GSSAPI");

        // Create server jaas config
        String serverJaas =
                String.format(
                        "com.sun.security.auth.module.Krb5LoginModule required "
                                + "useKeyTab=true storeKey=true useTicketCache=false "
                                + "keyTab=\"%s\" principal=\"%s\";",
                        keytab.getAbsolutePath(), serverPrincipal);
        serverConf.setString("security.sasl.gssapi.jaas.config", serverJaas);

        // Initialize Server-Side Authenticator
        SaslServerAuthenticator serverAuth = new SaslServerAuthenticator(serverConf);
        serverAuth.initialize(
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
                });

        // Configure and initialize the client authenticator.
        Configuration clientConf = new Configuration();
        clientConf.setString(CLIENT_SASL_MECHANISM, "GSSAPI");
        String clientJaas =
                String.format(
                        "com.sun.security.auth.module.Krb5LoginModule required "
                                + "useKeyTab=true storeKey=true useTicketCache=false "
                                + "keyTab=\"%s\" principal=\"%s\";",
                        keytab.getAbsolutePath(), clientPrincipal);

        clientConf.setString(CLIENT_SASL_JAAS_CONFIG, clientJaas);
        SaslClientAuthenticator clientAuth = new SaslClientAuthenticator(clientConf);
        clientAuth.initialize(() -> "127.0.0.1");

        byte[] challenge =
                clientAuth.hasInitialTokenResponse() ? clientAuth.authenticate(new byte[0]) : null;

        while (!clientAuth.isCompleted() || !serverAuth.isCompleted()) {
            if (challenge != null) {
                // Server process client's token and generates a response/challenge.
                byte[] response = serverAuth.evaluateResponse(challenge);

                // Client validates server's response (mutual authentication).
                challenge = (response != null) ? clientAuth.authenticate(response) : null;

            } else {
                // If tokens run out but authentication isn't finished, it's a failure scenario.
                break;
            }
        }

        assertThat(serverAuth.isCompleted()).as("Server should be fully authenticated").isTrue();
        assertThat(clientAuth.isCompleted()).as("Client should be fully authenticated").isTrue();
        assertThat(serverAuth.createPrincipal().getName())
                .as("Authenticated principal name should match the client's identity")
                .startsWith("client");

        serverAuth.close();
        clientAuth.close();
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
