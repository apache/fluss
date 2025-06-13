/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.rpc.netty.client;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.rpc.messages.PbApiVersion;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.ApiManager;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** The unit test for {@link ServerApiVersions}. */
class ServerApiVersionsTest {

    @Test
    void testClientUnSupportedApiVersions() {
        ApiManager apiManager = new ApiManager(ServerType.TABLET_SERVER);
        Set<ApiKeys> apiKeys = apiManager.enabledApis();
        List<PbApiVersion> apiVersions = new ArrayList<>();
        for (ApiKeys api : apiKeys) {
            apiVersions.add(
                    new PbApiVersion()
                            .setApiKey(api.id)
                            .setMinVersion(api.lowestSupportedVersion)
                            .setMaxVersion(api.highestSupportedVersion));
        }
        // add a api key that client don't support, we use -1 as apiKey to mock
        // the client unsupported api key
        apiVersions.add(new PbApiVersion().setApiKey(-1).setMinVersion(0).setMaxVersion(0));

        ServerApiVersions serverApiVersions = new ServerApiVersions(apiVersions);
        // verify we get get highest available version for supported api key
        assertThat(serverApiVersions.highestAvailableVersion(ApiKeys.API_VERSIONS))
                .isEqualTo(ApiKeys.API_VERSIONS.highestSupportedVersion);
    }
}
