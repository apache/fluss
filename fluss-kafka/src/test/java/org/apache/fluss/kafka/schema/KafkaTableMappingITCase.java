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

package org.apache.fluss.kafka.schema;

import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.assertj.core.api.Assertions.assertThat;

/** Verifies Kafka mapping properties survive the native Fluss table creation path. */
public class KafkaTableMappingITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @Test
    public void testResolveMappingFromCreatedTableMetadata() throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("event_key", DataTypes.STRING())
                                        .column("event_body", DataTypes.BYTES())
                                        .build())
                        .distributedBy(2)
                        .logFormat(LogFormat.ARROW)
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "event_key")
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "raw")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                        .build();
        KafkaTopicMapper mapper = new KafkaTopicMapper("kafka_ddl");
        TablePath tablePath = mapper.toTablePath("events");
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, descriptor);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        MetadataRequest request = new MetadataRequest();
        request.addTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        PbTableMetadata metadata =
                FLUSS_CLUSTER_EXTENSION
                        .newCoordinatorClient()
                        .metadata(request)
                        .get()
                        .getTableMetadatasList()
                        .get(0);
        TableDescriptor persisted = TableDescriptor.fromJsonBytes(metadata.getTableJson());
        KafkaTopicSchema mapping = new KafkaTopicSchemaResolver().resolve(persisted);

        assertThat(metadata.getTableId()).isEqualTo(tableId);
        assertThat(mapper.toTableId(mapper.toTopicId(metadata.getTableId()))).isEqualTo(tableId);
        assertThat(metadata.getBucketMetadatasList()).hasSize(2);
        assertThat(persisted.getCustomProperties())
                .containsAllEntriesOf(descriptor.getCustomProperties());
        assertThat(mapping.keyProjection().positions()).containsExactly(0);
        assertThat(mapping.keyFormat()).isEqualTo(KafkaDataFormat.STRING);
        assertThat(mapping.valueProjection().positions()).containsExactly(1);
        assertThat(mapping.valueFormat()).isEqualTo(KafkaDataFormat.RAW);
    }
}
