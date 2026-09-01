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

package org.apache.fluss.server;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestData;
import org.apache.fluss.row.encode.KvValueLayout;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.server.RpcServiceBase.validateKvSnapshotMetadataVersion;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests KV snapshot metadata API compatibility. */
class RpcServiceBaseTest {

    @Test
    void testValidateKvSnapshotMetadataVersion() {
        TableInfo taggedTableInfo = tableInfo(KvValueLayout.TAGGED);
        TableInfo plainTableInfo = tableInfo(KvValueLayout.PLAIN);

        assertThatThrownBy(() -> validateKvSnapshotMetadataVersion((short) 0, taggedTableInfo))
                .isInstanceOf(UnsupportedVersionException.class);
        validateKvSnapshotMetadataVersion((short) 1, taggedTableInfo);
        validateKvSnapshotMetadataVersion((short) 0, plainTableInfo);
    }

    private static TableInfo tableInfo(KvValueLayout layout) {
        Map<String, String> properties =
                new HashMap<>(TestData.DATA1_TABLE_DESCRIPTOR_PK.getProperties());
        properties.put(
                ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION.key(),
                String.valueOf(layout.version()));
        TableDescriptor descriptor = TestData.DATA1_TABLE_DESCRIPTOR_PK.withProperties(properties);
        return TableInfo.of(
                TablePath.of("db", "table_" + layout.version()),
                layout.version(),
                1,
                descriptor,
                TestData.DEFAULT_REMOTE_DATA_DIR,
                1L,
                1L);
    }
}
