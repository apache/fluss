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

package org.apache.fluss.fs.bos;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.bos.token.BOSSecurityTokenReceiver;
import org.apache.fluss.fs.bos.token.DynamicTemporaryBOSCredentialsProvider;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for server/client mode detection and configuration in {@link BOSFileSystemPlugin}. */
class BOSFileSystemPluginTest {

    @BeforeEach
    @AfterEach
    void resetReceiver() throws Exception {
        set("credentials", null);
        set("additionInfos", Collections.emptyMap());
    }

    private static void set(String field, Object value) throws Exception {
        Field f = BOSSecurityTokenReceiver.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(null, value);
    }

    private static ObtainedSecurityToken token(
            String ak, String sk, String session, Map<String, String> info) {
        return new ObtainedSecurityToken(
                "bos",
                CredentialsJsonSerde.toJson(new Credentials(ak, sk, session)),
                System.currentTimeMillis() + 3600_000L,
                info);
    }

    @Test
    void schemeIsBos() {
        assertThat(new BOSFileSystemPlugin().getScheme()).isEqualTo("bos");
    }

    @Test
    void configPrefixesAreMappedAndNormalised() {
        Configuration c = new Configuration();
        c.setString("bos.endpoint", "http://hkg.bcebos.com");
        c.setString("fs.bos.region", "hkg");
        c.setString("bos.access-key", "AK");
        c.setString("bos.secret.key", "SK");
        c.setString("bos.sts.duration-seconds", "900");
        c.setString("bos.multipart.part-size", "1048576");
        c.setString("s3.endpoint", "http://ignored");
        c.setString("remote.data.dir", "bos://bucket/x");
        Map<String, String> m = BOSFileSystemPlugin.extract(c);
        assertThat(m.get("endpoint")).isEqualTo("http://hkg.bcebos.com");
        assertThat(m.get("region")).isEqualTo("hkg");
        assertThat(m.get("access.key")).isEqualTo("AK");
        assertThat(m.get("secret.key")).isEqualTo("SK");
        assertThat(m.get("sts.duration.seconds")).isEqualTo("900");
        assertThat(m.get("multipart.part.size")).isEqualTo("1048576");
        assertThat(m.size())
                .as("keys without the bos./fs.bos. prefix must be ignored: " + m)
                .isEqualTo(6);
    }

    @Test
    void nullOrEmptyConfigGivesNoOptions() {
        assertThat(BOSFileSystemPlugin.extract(null).isEmpty()).isTrue();
        assertThat(BOSFileSystemPlugin.extract(new Configuration()).isEmpty()).isTrue();
    }

    @Test
    void bucketIsRequired() {
        Configuration c = new Configuration();
        c.setString("bos.endpoint", "http://hkg.bcebos.com");
        c.setString("bos.access.key", "AK");
        c.setString("bos.secret.key", "SK");
        assertThatThrownBy(() -> new BOSFileSystemPlugin().create(URI.create("bos:///path"), c))
                .isInstanceOf(IOException.class);
    }

    @Test
    void endpointIsRequired() {
        Configuration c = new Configuration();
        c.setString("bos.access.key", "AK");
        c.setString("bos.secret.key", "SK");
        assertThatThrownBy(() -> new BOSFileSystemPlugin().create(URI.create("bos://bucket/x"), c))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void malformedNumericOptionNamesTheOption() {
        Configuration c = serverConfig();
        c.setString("bos.multipart.part.size", "16MiB");
        assertThatThrownBy(() -> new BOSFileSystemPlugin().create(URI.create("bos://bucket/x"), c))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bos.multipart.part.size")
                .hasMessageContaining("16MiB");
    }

    @Test
    void nonPositiveNumericOptionsAreRejected() {
        for (String key :
                new String[] {
                    "bos.multipart.part.size", "bos.read.window.size", "bos.max.connections"
                }) {
            Configuration c = serverConfig();
            c.setString(key, "0");
            assertThatThrownBy(
                            () -> new BOSFileSystemPlugin().create(URI.create("bos://bucket/x"), c))
                    .as(key)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(key);
        }
    }

    @Test
    void getLongFallsBackToTheDefault() {
        Map<String, String> conf = new HashMap<>();
        assertThat(BOSFileSystemPlugin.getLong(conf, "sts.duration.seconds", 43200L))
                .isEqualTo(43200L);
        conf.put("sts.duration.seconds", " 600 ");
        assertThat(BOSFileSystemPlugin.getLong(conf, "sts.duration.seconds", 43200L))
                .isEqualTo(600L);
    }

    private static Configuration serverConfig() {
        Configuration c = new Configuration();
        c.setString("bos.endpoint", "http://hkg.bcebos.com");
        c.setString("bos.access.key", "AK");
        c.setString("bos.secret.key", "SK");
        return c;
    }

    @Test
    void serverModeWhenAnAccessKeyIsConfigured() throws Exception {
        Configuration c = new Configuration();
        c.setString("bos.endpoint", "http://hkg.bcebos.com");
        c.setString("bos.access.key", "AK");
        c.setString("bos.secret.key", "SK");
        FileSystem fs = new BOSFileSystemPlugin().create(URI.create("bos://bucket/x"), c);
        assertThat(fs).isInstanceOf(BOSFileSystem.class);
        assertThat(fs.getUri().toString()).isEqualTo("bos://bucket");
        // server mode needs a region to issue tokens
        assertThatThrownBy(fs::obtainSecurityToken).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void clientModeWithoutAnyTokenFailsWithTheCanonicalMessage() {
        assertThatThrownBy(
                        () ->
                                new BOSFileSystemPlugin()
                                        .create(URI.create("bos://bucket/x"), new Configuration()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("No BOS credentials");
    }

    @Test
    void clientModeTakesEndpointFromTheTokenAndCannotIssueTokens() throws Exception {
        Map<String, String> info = new HashMap<>();
        info.put("endpoint", "http://hkg.bcebos.com");
        info.put("region", "hkg");
        new BOSSecurityTokenReceiver().onNewTokensObtained(token("TAK", "TSK", "TTOK", info));
        FileSystem fs =
                new BOSFileSystemPlugin().create(URI.create("bos://bucket/x"), new Configuration());
        assertThat(fs).isInstanceOf(BOSFileSystem.class);
        assertThatThrownBy(fs::obtainSecurityToken).isInstanceOf(IOException.class);
    }

    @Test
    void dynamicCredentialsAlwaysReflectTheLatestToken() throws Exception {
        DynamicTemporaryBOSCredentialsProvider creds = new DynamicTemporaryBOSCredentialsProvider();
        assertThatThrownBy(creds::getAccessKeyId).isInstanceOf(IllegalStateException.class);
        BOSSecurityTokenReceiver r = new BOSSecurityTokenReceiver();
        r.onNewTokensObtained(token("AK1", "SK1", "T1", Collections.emptyMap()));
        assertThat(creds.getAccessKeyId()).isEqualTo("AK1");
        assertThat(creds.getSecretKey()).isEqualTo("SK1");
        assertThat(creds.getSessionToken()).isEqualTo("T1");
        r.onNewTokensObtained(token("AK2", "SK2", "T2", Collections.emptyMap()));
        assertThat(creds.getAccessKeyId()).isEqualTo("AK2");
        assertThat(creds.getSessionToken()).isEqualTo("T2");
        assertThat(r.scheme()).isEqualTo("bos");
    }
}
