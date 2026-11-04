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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.WriteResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.getArrayField;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.getField;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readDataFiles;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readDeleteFiles;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readRewriteDataFileResult;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readSpecs;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.writeContentFiles;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.writeRewriteDataFileResult;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.writeSpecs;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Json serializer and deserializer for {@link IcebergWriteResult}, used by version 2 of {@link
 * IcebergWriteResultSerializer}.
 *
 * <p>The serialized format includes:
 *
 * <ul>
 *   <li>"specs": the partition specs of the table
 *   <li>"data_files": the data files of the {@link WriteResult}
 *   <li>"delete_files": the delete files of the {@link WriteResult}
 *   <li>"referenced_data_files": array of the locations of the data files referenced by the delete
 *       files
 *   <li>"rewritten_delete_files": the delete files rewritten by the {@link WriteResult}
 *   <li>"rewrite_result": the {@link RewriteDataFileResult}, or null if there is none
 * </ul>
 *
 * <p>Specs, files and rewrite results are written as described in {@link IcebergJsonSerdeUtils}.
 */
public class IcebergWriteResultJsonSerde
        implements JsonSerializer<IcebergWriteResult>, JsonDeserializer<IcebergWriteResult> {

    public static final IcebergWriteResultJsonSerde INSTANCE = new IcebergWriteResultJsonSerde();

    private static final String DATA_FILES_FIELD = "data_files";
    private static final String DELETE_FILES_FIELD = "delete_files";
    private static final String REFERENCED_DATA_FILES_FIELD = "referenced_data_files";
    private static final String REWRITTEN_DELETE_FILES_FIELD = "rewritten_delete_files";
    private static final String REWRITE_RESULT_FIELD = "rewrite_result";

    @Override
    public void serialize(IcebergWriteResult icebergWriteResult, JsonGenerator generator)
            throws IOException {
        Map<Integer, PartitionSpec> specs = icebergWriteResult.specs();
        if (specs == null) {
            throw new IOException(
                    "Cannot serialize an IcebergWriteResult without partition specs, "
                            + "it was restored from serializer version 1.");
        }
        WriteResult writeResult = icebergWriteResult.getWriteResult();

        generator.writeStartObject();
        writeSpecs(generator, specs);
        writeContentFiles(
                generator, DATA_FILES_FIELD, Arrays.asList(writeResult.dataFiles()), specs);
        writeContentFiles(
                generator, DELETE_FILES_FIELD, Arrays.asList(writeResult.deleteFiles()), specs);
        generator.writeArrayFieldStart(REFERENCED_DATA_FILES_FIELD);
        for (CharSequence referencedDataFile : writeResult.referencedDataFiles()) {
            generator.writeString(referencedDataFile.toString());
        }
        generator.writeEndArray();
        writeContentFiles(
                generator,
                REWRITTEN_DELETE_FILES_FIELD,
                Arrays.asList(writeResult.rewrittenDeleteFiles()),
                specs);

        RewriteDataFileResult rewriteDataFileResult = icebergWriteResult.rewriteDataFileResult();
        if (rewriteDataFileResult == null) {
            generator.writeNullField(REWRITE_RESULT_FIELD);
        } else {
            generator.writeFieldName(REWRITE_RESULT_FIELD);
            writeRewriteDataFileResult(generator, rewriteDataFileResult, specs);
        }
        generator.writeEndObject();
    }

    @Override
    public IcebergWriteResult deserialize(JsonNode node) {
        Map<Integer, PartitionSpec> specs = readSpecs(node);

        List<CharSequence> referencedDataFiles = new ArrayList<>();
        for (JsonNode locationNode : getArrayField(node, REFERENCED_DATA_FILES_FIELD)) {
            checkArgument(
                    locationNode.isTextual(),
                    "Field %s must contain strings.",
                    REFERENCED_DATA_FILES_FIELD);
            referencedDataFiles.add(locationNode.asText());
        }
        WriteResult writeResult =
                WriteResult.builder()
                        .addDataFiles(readDataFiles(node, DATA_FILES_FIELD, specs))
                        .addDeleteFiles(readDeleteFiles(node, DELETE_FILES_FIELD, specs))
                        .addReferencedDataFiles(referencedDataFiles)
                        .addRewrittenDeleteFiles(
                                readDeleteFiles(node, REWRITTEN_DELETE_FILES_FIELD, specs))
                        .build();

        JsonNode rewriteNode = getField(node, REWRITE_RESULT_FIELD);
        RewriteDataFileResult rewriteDataFileResult =
                rewriteNode.isNull() ? null : readRewriteDataFileResult(rewriteNode, specs);
        return new IcebergWriteResult(writeResult, specs, rewriteDataFileResult);
    }
}
