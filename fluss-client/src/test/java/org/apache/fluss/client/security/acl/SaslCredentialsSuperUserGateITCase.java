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

package org.apache.fluss.client.security.acl;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.security.acl.AccessControlEntry.WILD_CARD_HOST;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the authorization rule for altering {@code security.*} dynamic cluster configs: changing
 * a security-prefixed cluster-config key at runtime requires super-user privileges, while other
 * keys require only cluster {@code ALTER}.
 *
 * <p>The cluster runs SASL/PLAIN with the credentials map stored in {@code
 * security.sasl.plain.credentials}. The test asserts:
 *
 * <ul>
 *   <li>{@code bob}, holding only cluster {@code ALTER}, is DENIED when altering {@code
 *       security.sasl.plain.credentials} (via SET and via SUBTRACT+APPEND), and the existing {@code
 *       root} credential is unchanged afterwards.
 *   <li>A super-user ({@code root}) CAN still alter {@code security.sasl.plain.credentials}.
 *   <li>{@code bob} CAN still alter a NON-security cluster config with just cluster {@code ALTER}.
 * </ul>
 */
public class SaslCredentialsSuperUserGateITCase {

    private static final String ROOT_ORIG_PASSWORD = "root-pass";
    private static final String BOB_PASSWORD = "bob-pass";
    private static final String CREDENTIALS_KEY = "security.sasl.plain.credentials";
    private static final String NON_SECURITY_KEY = "log.retention.roll-active-segment.enabled";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setClusterConf(initConfig())
                    .build();

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:sasl");
        conf.setString("security.sasl.enabled.mechanisms", "plain");
        // Root's password lives in the dynamically-alterable credentials MAP.
        conf.setString(CREDENTIALS_KEY, "root:" + ROOT_ORIG_PASSWORD + ",bob:" + BOB_PASSWORD);
        conf.setString(ConfigOptions.SUPER_USERS.key(), "User:root");
        conf.setBoolean(ConfigOptions.AUTHORIZER_ENABLED.key(), true);
        return conf;
    }

    /** Grants bob ONLY cluster ALTER once for the whole class (nothing else). */
    @BeforeAll
    static void grantBobClusterAlter() throws Exception {
        try (Connection rootConn = connAs("root", ROOT_ORIG_PASSWORD);
                Admin rootAdmin = rootConn.getAdmin()) {
            rootAdmin
                    .createAcls(
                            Collections.singletonList(
                                    new AclBinding(
                                            Resource.cluster(),
                                            new AccessControlEntry(
                                                    new FlussPrincipal("bob", "User"),
                                                    WILD_CARD_HOST,
                                                    OperationType.ALTER,
                                                    PermissionType.ALLOW))))
                    .all()
                    .get();
        }
    }

    private static Connection connAs(String user, String password) {
        Configuration conf = FLUSS_CLUSTER_EXTENSION.getClientConfig("CLIENT");
        conf.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "sasl");
        conf.set(ConfigOptions.CLIENT_SASL_MECHANISM, "plain");
        conf.setString("client.security.sasl.username", user);
        conf.setString("client.security.sasl.password", password);
        return ConnectionFactory.createConnection(conf);
    }

    /**
     * A principal holding only cluster {@code ALTER} must not be able to alter the SASL credentials
     * map, and the existing {@code root} credential must remain unchanged.
     */
    @Test
    void bobWithClusterAlterCannotAlterSecurityCredentials() throws Exception {
        try (Connection bobConn = connAs("bob", BOB_PASSWORD);
                Admin bobAdmin = bobConn.getAdmin()) {
            // Attempt 1: whole-map SET on the credentials key.
            assertThatThrownBy(
                            () ->
                                    bobAdmin.alterClusterConfigs(
                                                    Collections.singletonList(
                                                            new AlterConfig(
                                                                    CREDENTIALS_KEY,
                                                                    "root:changed,bob:"
                                                                            + BOB_PASSWORD,
                                                                    AlterConfigOpType.SET)))
                                            .get())
                    .as("bob (cluster ALTER only) must be denied altering security configs via SET")
                    .hasCauseInstanceOf(AuthorizationException.class)
                    .hasMessageContaining("not a super user")
                    .hasMessageContaining(CREDENTIALS_KEY);

            // Attempt 2: the SUBTRACT-then-APPEND path on the credentials key must also be denied.
            assertThatThrownBy(
                            () ->
                                    bobAdmin.alterClusterConfigs(
                                                    Arrays.asList(
                                                            new AlterConfig(
                                                                    CREDENTIALS_KEY,
                                                                    "root:" + ROOT_ORIG_PASSWORD,
                                                                    AlterConfigOpType.SUBTRACT),
                                                            new AlterConfig(
                                                                    CREDENTIALS_KEY,
                                                                    "root:changed",
                                                                    AlterConfigOpType.APPEND)))
                                            .get())
                    .as("bob must be denied altering security configs via SUBTRACT+APPEND")
                    .hasCauseInstanceOf(AuthorizationException.class)
                    .hasMessageContaining("not a super user");
        }

        // The existing root credential must be unchanged: connecting as root and performing a
        // super-user-only action (listAcls) must still succeed.
        assertThatCode(
                        () -> {
                            try (Connection rootConn = connAs("root", ROOT_ORIG_PASSWORD);
                                    Admin rootAdmin = rootConn.getAdmin()) {
                                rootAdmin.listAcls(AclBindingFilter.ANY).get();
                            }
                        })
                .as("the existing root credential must be unchanged")
                .doesNotThrowAnyException();
    }

    /** A super-user may still alter the SASL credentials map. */
    @Test
    void superUserRootCanAlterSecurityCredentials() throws Exception {
        try (Connection rootConn = connAs("root", ROOT_ORIG_PASSWORD);
                Admin rootAdmin = rootConn.getAdmin()) {
            assertThatCode(
                            () ->
                                    rootAdmin
                                            .alterClusterConfigs(
                                                    Collections.singletonList(
                                                            new AlterConfig(
                                                                    CREDENTIALS_KEY,
                                                                    "carol:carol-pass",
                                                                    AlterConfigOpType.APPEND)))
                                            .get())
                    .as("a super user must still be allowed to manage SASL credentials")
                    .doesNotThrowAnyException();
        }
    }

    /** Cluster {@code ALTER} still suffices for non-security config keys. */
    @Test
    void bobWithClusterAlterCanAlterNonSecurityConfig() throws Exception {
        try (Connection bobConn = connAs("bob", BOB_PASSWORD);
                Admin bobAdmin = bobConn.getAdmin()) {
            assertThatCode(
                            () ->
                                    bobAdmin.alterClusterConfigs(
                                                    Collections.singletonList(
                                                            new AlterConfig(
                                                                    NON_SECURITY_KEY,
                                                                    "true",
                                                                    AlterConfigOpType.SET)))
                                            .get())
                    .as("cluster ALTER must still allow altering non-security cluster configs")
                    .doesNotThrowAnyException();
        }
    }
}
