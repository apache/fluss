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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LookupClient}. */
class LookupClientTest {

    @Test
    void testInvalidRetryBackoffConfiguration() {
        TestingMetadataUpdater metadataUpdater =
                TestingMetadataUpdater.builder(Collections.emptyMap()).build();
        Duration[][] invalid = {
            {Duration.ofMillis(-1), Duration.ofSeconds(1)},
            {Duration.ofSeconds(2), Duration.ofSeconds(1)},
            {Duration.ZERO, Duration.ofMillis((long) Integer.MAX_VALUE + 1)},
            {Duration.ZERO, Duration.ofSeconds(Long.MAX_VALUE)}
        };
        for (Duration[] values : invalid) {
            Configuration conf = new Configuration();
            conf.set(ConfigOptions.CLIENT_LOOKUP_RETRY_BACKOFF, values[0]);
            conf.set(ConfigOptions.CLIENT_LOOKUP_RETRY_BACKOFF_MAX, values[1]);
            assertThatThrownBy(() -> new LookupClient(conf, metadataUpdater))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("client.lookup.retry-backoff");
        }
    }
}
