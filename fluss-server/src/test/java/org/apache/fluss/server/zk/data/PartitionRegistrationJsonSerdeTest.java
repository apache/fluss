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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.utils.json.JsonSerdeTestBase;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PartitionRegistrationJsonSerde}. */
class PartitionRegistrationJsonSerdeTest extends JsonSerdeTestBase<PartitionRegistration> {

    PartitionRegistrationJsonSerdeTest() {
        super(PartitionRegistrationJsonSerde.INSTANCE);
    }

    @Override
    protected PartitionRegistration[] createObjects() {
        PartitionRegistration[] partitionRegistrations = new PartitionRegistration[2];

        partitionRegistrations[0] = new PartitionRegistration(1234L, 5678L, "file://local/remote");
        partitionRegistrations[1] = new PartitionRegistration(246L, 135L, null);

        return partitionRegistrations;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"table_id\":1234,\"partition_id\":5678,\"remote_data_dir\":\"file://local/remote\"}",
            "{\"version\":1,\"table_id\":246,\"partition_id\":135}"
        };
    }

    @Test
    void testTablePartitionCompatibility() throws IOException {
        // Test that old TablePartition format (without remote_data_dir) can be deserialized
        // correctly as PartitionRegistration
        String tablePartitionJson = "{\"version\":1,\"table_id\":1234,\"partition_id\":5678}";
        PartitionRegistration actual =
                JsonSerdeUtils.readValue(
                        tablePartitionJson.getBytes(StandardCharsets.UTF_8),
                        PartitionRegistrationJsonSerde.INSTANCE);

        PartitionRegistration expected = new PartitionRegistration(1234L, 5678L, null);
        assertThat(actual).isEqualTo(expected);
    }
}
