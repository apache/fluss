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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;

import java.io.IOException;
import java.util.Map;

import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.getArrayField;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readDataFiles;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readDeleteFiles;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readRewriteDataFileResult;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.readSpecs;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.writeContentFiles;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.writeRewriteDataFileResult;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.writeSpecs;

/**
 * Json serializer and deserializer for {@link IcebergCommittable}, used by version 2 of {@link
 * IcebergCommittableSerializer}.
 *
 * <p>The serialized format includes:
 *
 * <ul>
 *   <li>"specs": the partition specs of the table
 *   <li>"data_files": the data files to commit
 *   <li>"delete_files": the delete files to commit
 *   <li>"rewrite_results": array of the {@link RewriteDataFileResult}s to commit
 * </ul>
 *
 * <p>Specs, files and rewrite results are written as described in {@link IcebergJsonSerdeUtils}.
 */
public class IcebergCommittableJsonSerde
        implements JsonSerializer<IcebergCommittable>, JsonDeserializer<IcebergCommittable> {

    public static final IcebergCommittableJsonSerde INSTANCE = new IcebergCommittableJsonSerde();

    private static final String DATA_FILES_FIELD = "data_files";
    private static final String DELETE_FILES_FIELD = "delete_files";
    private static final String REWRITE_RESULTS_FIELD = "rewrite_results";

    @Override
    public void serialize(IcebergCommittable committable, JsonGenerator generator)
            throws IOException {
        Map<Integer, PartitionSpec> specs = committable.specs();
        if (specs == null) {
            throw new IOException(
                    "Cannot serialize an IcebergCommittable without partition specs, "
                            + "it was restored from serializer version 1.");
        }

        generator.writeStartObject();
        writeSpecs(generator, specs);
        writeContentFiles(generator, DATA_FILES_FIELD, committable.getDataFiles(), specs);
        writeContentFiles(generator, DELETE_FILES_FIELD, committable.getDeleteFiles(), specs);
        generator.writeArrayFieldStart(REWRITE_RESULTS_FIELD);
        for (RewriteDataFileResult rewriteDataFileResult : committable.rewriteDataFileResults()) {
            writeRewriteDataFileResult(generator, rewriteDataFileResult, specs);
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }

    @Override
    public IcebergCommittable deserialize(JsonNode node) {
        Map<Integer, PartitionSpec> specs = readSpecs(node);

        IcebergCommittable.Builder builder = IcebergCommittable.builder().addSpecs(specs);
        for (DataFile dataFile : readDataFiles(node, DATA_FILES_FIELD, specs)) {
            builder.addDataFile(dataFile);
        }
        for (DeleteFile deleteFile : readDeleteFiles(node, DELETE_FILES_FIELD, specs)) {
            builder.addDeleteFile(deleteFile);
        }
        for (JsonNode rewriteNode : getArrayField(node, REWRITE_RESULTS_FIELD)) {
            builder.addRewriteDataFileResult(readRewriteDataFileResult(rewriteNode, specs));
        }
        return builder.build();
    }
}
