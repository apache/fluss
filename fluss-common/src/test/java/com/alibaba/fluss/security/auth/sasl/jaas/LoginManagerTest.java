/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.security.auth.sasl.jaas;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LoginManager}. */
public class LoginManagerTest {
    String dynamicDigestContext;

    @BeforeEach
    void before() {
        dynamicDigestContext =
                DigestLoginModule.class.getName()
                        + " required username=\"digestuser\" password=\"digest-secret\";";
        TestJaasConfig.createConfiguration("DIGEST-MD5", Collections.singletonList("DIGEST-MD5"));
    }

    @AfterEach
    public void tearDown() {
        LoginManager.closeAll();
    }

    @Test
    public void testClientLoginManager() throws Exception {
        JaasContext dynamicContext = JaasContext.loadClientContext(dynamicDigestContext);
        JaasContext staticContext = JaasContext.loadClientContext(null);

        LoginManager dynamicLogin = LoginManager.acquireLoginManager(dynamicContext);
        assertThat(dynamicLogin.cacheKey()).isEqualTo(dynamicContext.dynamicJaasConfig());
        LoginManager staticLogin = LoginManager.acquireLoginManager(staticContext);
        assertThat(staticLogin.cacheKey()).isNotSameAs(staticLogin);
        assertThat(staticLogin.cacheKey()).isEqualTo("FlussClient");

        assertThat(dynamicLogin.subject().getPublicCredentials()).containsExactly("digestuser");
        assertThat(dynamicLogin.subject().getPrivateCredentials()).containsExactly("digest-secret");

        // test cache
        assertThat(LoginManager.acquireLoginManager(dynamicContext)).isEqualTo(dynamicLogin);
        assertThat(LoginManager.acquireLoginManager(staticContext)).isEqualTo(staticLogin);

        verifyLoginManagerRelease(dynamicLogin, 2, dynamicContext);
        verifyLoginManagerRelease(staticLogin, 2, staticContext);
    }

    @Test
    public void testServerLoginManager() throws Exception {
        String listenerName = "listener1";
        JaasContext digestJaasContext =
                JaasContext.loadServerContext(listenerName, dynamicDigestContext);
        JaasContext plainJaasContext = JaasContext.loadServerContext(listenerName, null);

        LoginManager digestLoginManager = LoginManager.acquireLoginManager(digestJaasContext);
        LoginManager plainLoginManager = LoginManager.acquireLoginManager(plainJaasContext);
        assertThat(digestLoginManager.cacheKey()).isEqualTo(digestJaasContext.dynamicJaasConfig());
        assertThat(plainLoginManager.cacheKey()).isEqualTo("FlussServer");
        assertThat(digestLoginManager.subject().getPublicCredentials())
                .containsExactly("digestuser");
        assertThat(digestLoginManager.subject().getPrivateCredentials())
                .containsExactly("digest-secret");

        // test cache
        assertThat(LoginManager.acquireLoginManager(digestJaasContext))
                .isEqualTo(digestLoginManager);
        assertThat(LoginManager.acquireLoginManager(plainJaasContext)).isEqualTo(plainLoginManager);
        verifyLoginManagerRelease(digestLoginManager, 2, digestJaasContext);
        verifyLoginManagerRelease(plainLoginManager, 2, plainJaasContext);
    }

    private void verifyLoginManagerRelease(
            LoginManager loginManager, int acquireCount, JaasContext jaasContext) throws Exception {
        // Release all except one reference and verify that the loginManager is still cached
        for (int i = 0; i < acquireCount - 1; i++) {
            loginManager.release();
            assertThat(LoginManager.acquireLoginManager(jaasContext)).isEqualTo(loginManager);
        }

        // Release all references and verify that new LoginManager is created on next acquire
        for (int i = 0; i < 2; i++) {
            // release all references
            loginManager.release();
            LoginManager newLoginManager = LoginManager.acquireLoginManager(jaasContext);
            assertThat(newLoginManager).isEqualTo(loginManager);
            newLoginManager.release();
        }
    }
}
