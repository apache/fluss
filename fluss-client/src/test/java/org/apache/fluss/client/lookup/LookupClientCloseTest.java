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
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link LookupClient} close behavior with configurable timeout. */
class LookupClientCloseTest {

    @Test
    void testCloseWithVeryShortTimeoutReturnsNormally() {
        // Create a lookup client with default config (no pending lookups)
        LookupClient lookupClient = createLookupClient();

        // Closing with a 1ms timeout should return normally without throwing
        // InterruptedException, even though there are no pending requests to wait for.
        assertThatCode(() -> lookupClient.close(Duration.ofMillis(1))).doesNotThrowAnyException();
    }

    @Test
    void testCloseWithZeroTimeoutReturnsNormally() {
        LookupClient lookupClient = createLookupClient();

        // A zero-duration close should also return without error.
        assertThatCode(() -> lookupClient.close(Duration.ZERO)).doesNotThrowAnyException();
    }

    @Test
    void testCloseWithLargeTimeoutReturnsNormally() {
        LookupClient lookupClient = createLookupClient();

        // Closing with a large timeout should also work fine when there are no pending requests.
        assertThatCode(() -> lookupClient.close(Duration.ofSeconds(5))).doesNotThrowAnyException();
    }

    @Test
    void testCloseIsIdempotent() {
        LookupClient lookupClient = createLookupClient();

        // First close should succeed
        lookupClient.close(Duration.ofMillis(100));

        // Second close should also return without throwing
        assertThatCode(() -> lookupClient.close(Duration.ofMillis(100))).doesNotThrowAnyException();
    }

    @Test
    void testDefaultCloseTimeoutConfigValue() {
        // Verify the default value of CLIENT_LOOKUP_CLOSE_TIMEOUT is Long.MAX_VALUE
        // which preserves the previous behavior of waiting indefinitely.
        Configuration conf = new Configuration();
        assertThat(conf.get(ConfigOptions.CLIENT_LOOKUP_CLOSE_TIMEOUT))
                .isEqualTo(Duration.ofMillis(Long.MAX_VALUE));
    }

    private LookupClient createLookupClient() {
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        TestingMetadataUpdater metadataUpdater = TestingMetadataUpdater.builder(tableInfos).build();
        Configuration conf = new Configuration();
        // Use small queue to avoid resource waste
        conf.set(ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE, 5);
        conf.set(ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE, 10);
        return new LookupClient(conf, metadataUpdater);
    }
}
