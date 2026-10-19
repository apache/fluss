/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.format.KafkaFieldDecoder;
import org.apache.fluss.kafka.format.KafkaFormatFactoryRegistry;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaResolver;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.bytesview.BytesView;

import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Converts schema-mapped Kafka values and fixed system columns into owned Arrow log bytes. */
@Internal
public final class ArrowKafkaRecordTranscoder implements KafkaRecordTranscoder {
    private final KafkaFormatFactoryRegistry formats = new KafkaFormatFactoryRegistry();
    private final KafkaTopicSchemaResolver schemaResolver = new KafkaTopicSchemaResolver();
    private final FlussArrowRecordEncoder arrowRecordEncoder = new FlussArrowRecordEncoder();
    private final KafkaCompiledWritePlanCache writePlanCache = new KafkaCompiledWritePlanCache();

    @Override
    public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
        return writePlanCache.getOrCompile(
                tableInfo,
                () ->
                        new KafkaTopicWritePlan(
                                tableInfo, schemaResolver.resolve(tableInfo.toTableDescriptor())));
    }

    @Override
    public BytesView transcode(int partitionId, List<Record> records, TableInfo tableInfo)
            throws Exception {
        return transcode(partitionId, records, prepare(tableInfo));
    }

    @Override
    public BytesView transcode(int partitionId, List<Record> records, KafkaTopicWritePlan plan)
            throws Exception {
        checkArgument(partitionId >= 0, "Kafka partition ID must be non-negative.");
        checkArgument(!records.isEmpty(), "Cannot transcode an empty Kafka partition.");
        KafkaTableSchema schema = plan.topicSchema();
        KafkaFieldDecoder valueDecoder = formats.createDecoder(schema);
        KafkaRowAssembler assembler = plan.rowAssembler();
        return arrowRecordEncoder.encodeStreaming(
                consumer -> {
                    for (Record record : records) {
                        consumer.append(
                                assembler.assemble(
                                        valueDecoder.decode(record.borrowedValue()),
                                        record,
                                        partitionId));
                    }
                },
                plan.tableInfo());
    }
}
