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

package org.apache.fluss.fs.gs.token;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the credentials {@link GSSecurityTokenProvider} refuses to mint a token from. None of
 * these cases reach a token server. User credentials are covered by {@code GSSecurityTokenITCase},
 * which controls {@code GOOGLE_APPLICATION_CREDENTIALS}.
 */
class GSSecurityTokenProviderTest {

    @Test
    void nullCredentialsAreRejected() {
        Configuration conf = new Configuration(false);
        conf.setBoolean("fs.gs.auth.service.account.enable", false);
        conf.setBoolean("fs.gs.auth.null.enable", true);

        assertThatThrownBy(() -> new GSSecurityTokenProvider("gs", conf).obtainSecurityToken())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Only a service account");
    }

    @Test
    void impersonationIsRejected() {
        Configuration conf = new Configuration(false);
        conf.set(
                "fs.gs.auth.impersonation.service.account",
                "target@test-project.iam.gserviceaccount.com");

        assertThatThrownBy(() -> new GSSecurityTokenProvider("gs", conf).obtainSecurityToken())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("impersonation");
    }

    @Test
    void userImpersonationIsRejected() {
        Configuration conf = new Configuration(false);
        conf.set(
                "fs.gs.auth.impersonation.service.account.for.user.fluss",
                "target@test-project.iam.gserviceaccount.com");

        assertThatThrownBy(() -> new GSSecurityTokenProvider("gs", conf).obtainSecurityToken())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("impersonation");
    }

    @Test
    void customAccessTokenProviderIsRejected() {
        Configuration conf = new Configuration(false);
        conf.set("fs.gs.auth.access.token.provider.impl", GSAccessTokenProvider.NAME);

        assertThatThrownBy(() -> new GSSecurityTokenProvider("gs", conf).obtainSecurityToken())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(GSAccessTokenProvider.NAME);
    }

    @Test
    void delegationTokenBindingIsRejected() {
        Configuration conf = new Configuration(false);
        conf.set("fs.gs.delegation.token.binding", "org.example.Binding");

        assertThatThrownBy(() -> new GSSecurityTokenProvider("gs", conf).obtainSecurityToken())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fs.gs.delegation.token.binding");
    }
}
