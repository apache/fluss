/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ScopeEnumeratorFunction}. */
class ScopeEnumeratorFunctionTest {

    @Test
    void mapsActionFileSystemConfigsIntoClientNamespace() {
        OrphanCleanConfig config =
                configWith(
                        "fs.oss.endpoint=test-endpoint",
                        "fs.oss.region=test-region",
                        "client.security.protocol=SASL");

        Configuration clientConfig = ScopeEnumeratorFunction.createFlussClientConfiguration(config);

        assertThat(clientConfig.toMap())
                .containsEntry("client.fs.oss.endpoint", "test-endpoint")
                .containsEntry("client.fs.oss.region", "test-region")
                .containsEntry("client.security.protocol", "SASL")
                .doesNotContainKeys("fs.oss.endpoint", "fs.oss.region");
    }

    @Test
    void rejectsConflictingActionAndClientFileSystemConfigs() {
        OrphanCleanConfig config =
                configWith(
                        "fs.oss.endpoint=action-endpoint",
                        "client.fs.oss.endpoint=client-endpoint");

        assertThatThrownBy(() -> ScopeEnumeratorFunction.createFlussClientConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fs.oss.endpoint")
                .hasMessageContaining("client.fs.oss.endpoint");
    }

    private static OrphanCleanConfig configWith(String... configs) {
        String[] args = new String[4 + configs.length * 2];
        args[0] = "--bootstrap-server";
        args[1] = "h:9123";
        args[2] = "--all-databases";
        args[3] = "--dry-run";
        for (int i = 0; i < configs.length; i++) {
            args[4 + i * 2] = "--conf";
            args[5 + i * 2] = configs[i];
        }
        return OrphanCleanConfig.fromParams(MultipleParameterToolAdapter.fromArgs(args));
    }
}
