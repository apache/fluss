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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestData;
import org.apache.fluss.rpc.protocol.ApiKeys;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests row TTL client version checks in {@link ReplicaManager}. */
class ReplicaManagerRowTtlClientVersionTest {

    @Test
    void testRowTTLClientVersionRequirements() {
        TableInfo tableInfo = rowTTLTableInfo();

        assertRequiredVersion(
                tableInfo,
                1,
                ApiKeys.GET_LATEST_KV_SNAPSHOTS,
                ApiKeys.GET_KV_SNAPSHOT_METADATA,
                ApiKeys.ACQUIRE_KV_SNAPSHOT_LEASE,
                ApiKeys.LIMIT_SCAN,
                ApiKeys.SCAN_KV);
        assertRequiredVersion(tableInfo, 2, ApiKeys.PUT_KV, ApiKeys.LOOKUP, ApiKeys.PREFIX_LOOKUP);
    }

    private static void assertRequiredVersion(
            TableInfo tableInfo, int requiredVersion, ApiKeys... apiKeys) {
        for (ApiKeys apiKey : apiKeys) {
            assertThat(ReplicaManager.canSkipClientVersionValidation(apiKey, requiredVersion))
                    .isTrue();
            assertThat(ReplicaManager.canSkipClientVersionValidation(apiKey, requiredVersion - 1))
                    .isFalse();
            assertThatThrownBy(
                            () ->
                                    ReplicaManager.validateClientVersionForPkTable(
                                            apiKey, requiredVersion - 1, tableInfo))
                    .isInstanceOf(UnsupportedVersionException.class)
                    .hasMessageContaining("row TTL")
                    .hasMessageContaining("API '" + apiKey + "'")
                    .hasMessageContaining("requires API version " + requiredVersion);

            ReplicaManager.validateClientVersionForPkTable(apiKey, requiredVersion, tableInfo);
        }
    }

    private static TableInfo rowTTLTableInfo() {
        Map<String, String> properties =
                new HashMap<>(TestData.DATA1_TABLE_DESCRIPTOR_PK.getProperties());
        properties.put(ConfigOptions.TABLE_KV_ROW_TTL.key(), "1 h");
        properties.put(
                ConfigOptions.TABLE_KV_FORMAT_VERSION.key(), String.valueOf(KV_FORMAT_VERSION_3));
        TableDescriptor descriptor = TestData.DATA1_TABLE_DESCRIPTOR_PK.withProperties(properties);
        return TableInfo.of(
                TablePath.of("db", "ttl_table"),
                1L,
                1,
                descriptor,
                TestData.DEFAULT_REMOTE_DATA_DIR,
                1L,
                1L);
    }
}
