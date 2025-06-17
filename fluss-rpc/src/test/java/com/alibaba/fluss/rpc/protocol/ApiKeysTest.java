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

package com.alibaba.fluss.rpc.protocol;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ApiKeysTest {

    @Test
    void testAllApiKeys() {
        Set<Short> keys = new HashSet<>();
        for (ApiKeys api : ApiKeys.values()) {
            assertThat(keys.add(api.id)).isTrue();
            // reserve 0~999 for kafka protocol compatibility
            assertThat(api.id).isGreaterThanOrEqualTo((short) 1000);
            assertThat(api.lowestSupportedVersion).isLessThanOrEqualTo(api.highestSupportedVersion);
            assertThat(api.lowestSupportedVersion).isGreaterThanOrEqualTo((short) 0);
            assertThat(api.highestSupportedVersion).isGreaterThanOrEqualTo((short) 0);
        }
    }
}
