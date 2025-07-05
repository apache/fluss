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

package org.apache.fluss.client.token;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;

import static java.time.Instant.ofEpochMilli;
import static org.apache.fluss.config.ConfigOptions.FILESYSTEM_SECURITY_TOKEN_RENEWAL_RETRY_BACKOFF;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultSecurityTokenManager}. */
class DefaultSecurityTokenManagerTest {

    @Test
    void startTokensUpdateShouldScheduleRenewal() {
        TestingSecurityTokenProvider testingSecurityTokenProvider =
                new TestingSecurityTokenProvider("token1");

        // set small token renew backoff
        Configuration configuration = new Configuration();
        configuration.set(FILESYSTEM_SECURITY_TOKEN_RENEWAL_RETRY_BACKOFF, Duration.ofMillis(100));

        DefaultSecurityTokenManager securityTokenManager =
                new DefaultSecurityTokenManager(configuration, testingSecurityTokenProvider);

        // start token update
        securityTokenManager.startTokensUpdate();

        // token history should be token1
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(testingSecurityTokenProvider.getHistoryTokens())
                                .containsExactly("token1"));

        //  token history should be token1, token2
        testingSecurityTokenProvider.setCurrentToken("token2");
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(testingSecurityTokenProvider.getHistoryTokens())
                                .containsExactly("token1", "token2"));

        //  token history should be token1, token2, token3
        testingSecurityTokenProvider.setCurrentToken("token3");
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(testingSecurityTokenProvider.getHistoryTokens())
                                .containsExactly("token1", "token2", "token3"));

        securityTokenManager.stopTokensUpdate();

        securityTokenManager.stop();
    }

    @Test
    void calculateRenewalDelayShouldConsiderRenewalRatio() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.FILESYSTEM_SECURITY_TOKEN_RENEWAL_TIME_RATIO, 0.5);
        DefaultSecurityTokenManager securityTokenManager =
                new DefaultSecurityTokenManager(configuration, null);

        Clock constantClock = Clock.fixed(ofEpochMilli(100), ZoneId.systemDefault());
        assertThat(securityTokenManager.calculateRenewalDelay(constantClock, 200)).isEqualTo(50);
    }
}
