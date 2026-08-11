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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConfigException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OfflineLeaderRecoveryConfig}. */
class OfflineLeaderRecoveryConfigTest {

    @Test
    void testDefaultAndDynamicCleanRetryCount() {
        OfflineLeaderRecoveryConfig recoveryConfig =
                new OfflineLeaderRecoveryConfig(new Configuration());
        assertThat(recoveryConfig.getCleanRetryCount()).isEqualTo(-1);

        Configuration updatedConfig = new Configuration();
        updatedConfig.set(ConfigOptions.COORDINATOR_OFFLINE_LEADER_CLEAN_RETRY_COUNT, 5);
        recoveryConfig.validate(updatedConfig);
        recoveryConfig.reconfigure(updatedConfig);

        assertThat(recoveryConfig.getCleanRetryCount()).isEqualTo(5);
    }

    @Test
    void testRejectCleanRetryCountBelowMinusOne() {
        Configuration invalidConfig = new Configuration();
        invalidConfig.set(ConfigOptions.COORDINATOR_OFFLINE_LEADER_CLEAN_RETRY_COUNT, -2);

        assertThatThrownBy(() -> new OfflineLeaderRecoveryConfig(invalidConfig))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.COORDINATOR_OFFLINE_LEADER_CLEAN_RETRY_COUNT.key());
    }
}
