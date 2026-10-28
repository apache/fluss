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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.getField;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.writeIcebergJsonField;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Json serializer and deserializer for {@link IcebergSplit}, used by version 2 of {@link
 * IcebergSplitSerializer}.
 *
 * <p>The serialized format includes:
 *
 * <ul>
 *   <li>"bucket": the Fluss bucket, -1 if the table is not bucketed
 *   <li>"partition": array of the Fluss partition values, each a string or null; null if the
 *       partition is null
 *   <li>"file_scan_task": the file scan task, written by Iceberg's {@link ScanTaskParser}
 * </ul>
 *
 * <p>For example: {@code {"bucket": 2, "partition": ["2026-10-15"], "file_scan_task": {"task-type":
 * "file-scan-task", ...}}}. The task is parsed case-sensitively, which matches {@link
 * IcebergSplitPlanner}.
 */
public class IcebergSplitJsonSerde
        implements JsonSerializer<IcebergSplit>, JsonDeserializer<IcebergSplit> {

    public static final IcebergSplitJsonSerde INSTANCE = new IcebergSplitJsonSerde();

    private static final String BUCKET_FIELD = "bucket";
    private static final String PARTITION_FIELD = "partition";
    private static final String FILE_SCAN_TASK_FIELD = "file_scan_task";

    private static final boolean CASE_SENSITIVE = true;

    @Override
    public void serialize(IcebergSplit split, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(BUCKET_FIELD, split.bucket());

        List<String> partition = split.partition();
        if (partition == null) {
            generator.writeNullField(PARTITION_FIELD);
        } else {
            generator.writeArrayFieldStart(PARTITION_FIELD);
            for (String value : partition) {
                generator.writeString(value);
            }
            generator.writeEndArray();
        }

        writeIcebergJsonField(
                generator, FILE_SCAN_TASK_FIELD, ScanTaskParser.toJson(split.fileScanTask()));
        generator.writeEndObject();
    }

    @Override
    public IcebergSplit deserialize(JsonNode node) {

        JsonNode bucketNode = getField(node, BUCKET_FIELD);
        checkArgument(bucketNode.isInt(), "Field %s must be an integer.", BUCKET_FIELD);

        JsonNode partitionNode = getField(node, PARTITION_FIELD);
        List<String> partition;
        if (partitionNode.isNull()) {
            partition = null;
        } else {
            checkArgument(partitionNode.isArray(), "Field %s must be an array.", PARTITION_FIELD);
            partition = new ArrayList<>(partitionNode.size());
            for (JsonNode valueNode : partitionNode) {
                checkArgument(
                        valueNode.isNull() || valueNode.isTextual(),
                        "Field %s must contain strings.",
                        PARTITION_FIELD);
                partition.add(valueNode.isNull() ? null : valueNode.asText());
            }
        }

        FileScanTask fileScanTask =
                ScanTaskParser.fromJson(
                        getField(node, FILE_SCAN_TASK_FIELD).toString(), CASE_SENSITIVE);
        return new IcebergSplit(fileScanTask, bucketNode.intValue(), partition);
    }
}
