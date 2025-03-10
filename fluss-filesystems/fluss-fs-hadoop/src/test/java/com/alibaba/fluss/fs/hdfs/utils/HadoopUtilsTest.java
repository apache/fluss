/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.hdfs.utils;

import com.alibaba.fluss.testutils.common.CommonTestUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import sun.security.krb5.KrbException;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.fs.hdfs.utils.HadoopUtils.HDFS_DELEGATION_TOKEN_KIND;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for Hadoop utils. */
class HadoopUtilsTest {

    @BeforeAll
    static void setPropertiesToEnableKerberosConfigInit() throws KrbException {
        System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
        System.setProperty("java.security.krb5.kdc", "kdc");
        System.setProperty("java.security.krb5.conf", "/dev/null");
        sun.security.krb5.Config.refresh();
    }

    @AfterAll
    static void cleanupHadoopConfigs() {
        UserGroupInformation.setConfiguration(new Configuration());
    }

    @Test
    void testShouldReturnFalseWhenNoKerberosCredentialsOrDelegationTokens() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(UserGroupInformation.AuthenticationMethod.KERBEROS));
        UserGroupInformation userWithoutCredentialsOrTokens =
                createTestUser(UserGroupInformation.AuthenticationMethod.KERBEROS);
        assertThat(userWithoutCredentialsOrTokens.hasKerberosCredentials()).isTrue();

        boolean isKerberosEnabled =
                HadoopUtils.isKerberosSecurityEnabled(userWithoutCredentialsOrTokens);
        boolean result =
                HadoopUtils.areKerberosCredentialsValid(userWithoutCredentialsOrTokens, true);

        assertThat(isKerberosEnabled).isTrue();
        assertThat(result).isTrue();
    }

    @Test
    void testShouldReturnTrueWhenDelegationTokenIsPresent() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(UserGroupInformation.AuthenticationMethod.KERBEROS));
        UserGroupInformation userWithoutCredentialsButHavingToken =
                createTestUser(UserGroupInformation.AuthenticationMethod.KERBEROS);
        userWithoutCredentialsButHavingToken.addToken(getHDFSDelegationToken());

        assertThat(userWithoutCredentialsButHavingToken.hasKerberosCredentials()).isTrue();

        boolean result =
                HadoopUtils.areKerberosCredentialsValid(userWithoutCredentialsButHavingToken, true);

        assertThat(result).isTrue();
    }

    @Test
    void testShouldReturnTrueWhenKerberosCredentialsArePresent() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(UserGroupInformation.AuthenticationMethod.KERBEROS));
        UserGroupInformation userWithCredentials = Mockito.mock(UserGroupInformation.class);
        Mockito.when(userWithCredentials.getAuthenticationMethod())
                .thenReturn(UserGroupInformation.AuthenticationMethod.KERBEROS);
        Mockito.when(userWithCredentials.hasKerberosCredentials()).thenReturn(true);

        boolean result = HadoopUtils.areKerberosCredentialsValid(userWithCredentials, true);

        assertThat(result).isTrue();
    }

    @Test
    void isKerberosSecurityEnabled_NoKerberos_ReturnsFalse() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(UserGroupInformation.AuthenticationMethod.PROXY));
        UserGroupInformation userWithAuthMethodOtherThanKerberos =
                createTestUser(UserGroupInformation.AuthenticationMethod.PROXY);

        boolean result = HadoopUtils.isKerberosSecurityEnabled(userWithAuthMethodOtherThanKerberos);

        assertThat(result).isFalse();
    }

    @Test
    void testShouldReturnTrueIfTicketCacheIsNotUsed() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(UserGroupInformation.AuthenticationMethod.KERBEROS));
        UserGroupInformation user =
                createTestUser(UserGroupInformation.AuthenticationMethod.KERBEROS);

        boolean result = HadoopUtils.areKerberosCredentialsValid(user, false);

        assertThat(result).isTrue();
    }

    @Test
    void testShouldCheckIfTheUserHasHDFSDelegationToken() {
        UserGroupInformation userWithToken =
                createTestUser(UserGroupInformation.AuthenticationMethod.KERBEROS);
        userWithToken.addToken(getHDFSDelegationToken());

        boolean result = HadoopUtils.hasHDFSDelegationToken(userWithToken);

        assertThat(result).isTrue();
    }

    @Test
    void testShouldReturnFalseIfTheUserHasNoHDFSDelegationToken() {
        UserGroupInformation userWithoutToken =
                createTestUser(UserGroupInformation.AuthenticationMethod.KERBEROS);
        assertThat(userWithoutToken.getTokens().isEmpty()).isTrue();

        boolean result = HadoopUtils.hasHDFSDelegationToken(userWithoutToken);

        assertThat(result).isFalse();
    }

    @Test
    void testGetConfigurationFromHadoopEnv() throws Exception {
        String testHadoopHomeDir =
                Paths.get(getClass().getResource("/core-site.xml").toURI())
                        .getParent()
                        .toFile()
                        .getAbsolutePath();

        final Map<String, String> originalEnv = System.getenv();
        final Map<String, String> newEnv = new HashMap<>(originalEnv);
        // test get from HADOOP_HOME
        newEnv.put("HADOOP_HOME", testHadoopHomeDir);
        CommonTestUtils.setEnv(newEnv);
        try {
            com.alibaba.fluss.config.Configuration configuration =
                    new com.alibaba.fluss.config.Configuration();
            Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(configuration);
            assertThat(hadoopConf.get("cp_conf_key")).isEqualTo("oompf!");
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }

        // test get from HADOOP_CONF_DIR
        newEnv.put("HADOOP_CONF_DIR", testHadoopHomeDir);
        CommonTestUtils.setEnv(newEnv);
        try {
            com.alibaba.fluss.config.Configuration configuration =
                    new com.alibaba.fluss.config.Configuration();
            Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(configuration);
            assertThat(hadoopConf.get("cp_conf_key")).isEqualTo("oompf!");
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }
    }

    @Test
    void testGetConfigurationFromFlussConfig() {
        com.alibaba.fluss.config.Configuration configuration =
                new com.alibaba.fluss.config.Configuration();
        configuration.setString("fluss.hadoop.k1", "v1");
        Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(configuration);
        assertThat(hadoopConf.get("k1")).isEqualTo("v1");
    }

    private static Configuration getHadoopConfigWithAuthMethod(
            UserGroupInformation.AuthenticationMethod authenticationMethod) {
        Configuration conf = new Configuration(true);
        conf.set("hadoop.security.authentication", authenticationMethod.name());
        return conf;
    }

    private static UserGroupInformation createTestUser(
            UserGroupInformation.AuthenticationMethod authenticationMethod) {
        UserGroupInformation user = UserGroupInformation.createRemoteUser("test-user");
        user.setAuthenticationMethod(authenticationMethod);
        return user;
    }

    private static Token<DelegationTokenIdentifier> getHDFSDelegationToken() {
        Token<DelegationTokenIdentifier> token = new Token<>();
        token.setKind(HDFS_DELEGATION_TOKEN_KIND);
        return token;
    }
}
