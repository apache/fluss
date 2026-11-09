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

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.JsonUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Utilities for the JSON serdes of the Iceberg lake plugin. Iceberg objects are nested as the JSON
 * of Iceberg's own parsers, which is also used by the Iceberg REST catalog and is stable across
 * Iceberg versions.
 *
 * <p>The shared elements are:
 *
 * <ul>
 *   <li>Specs: {@code "specs": [{"schema": {...}, "spec": {...}}, ...]}, ordered by spec id, where
 *       "schema" is written by {@link SchemaParser} and "spec" by {@link PartitionSpecParser}.
 *   <li>Files: {@code "<field>": [{...}, ...]}, where each data or delete file is written by {@link
 *       ContentFileParser} with the spec of the file.
 *   <li>Rewrite result: {@code {"snapshot_id": 42, "deleted_data_files": [...], "added_data_files":
 *       [...]}}.
 * </ul>
 */
public class IcebergJsonSerdeUtils {

    private static final String SPECS_FIELD = "specs";
    private static final String SCHEMA_FIELD = "schema";
    private static final String SPEC_FIELD = "spec";
    private static final String SNAPSHOT_ID_FIELD = "snapshot_id";
    private static final String DELETED_DATA_FILES_FIELD = "deleted_data_files";
    private static final String ADDED_DATA_FILES_FIELD = "added_data_files";

    private IcebergJsonSerdeUtils() {}

    /** Serializes the value to UTF-8 JSON bytes with the given serializer. */
    public static <T> byte[] toJsonBytes(T value, JsonSerializer<T> serializer) throws IOException {
        try {
            return JsonSerdeUtils.writeValueAsBytes(value, serializer);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /** Deserializes the value from UTF-8 JSON bytes with the given deserializer. */
    public static <T> T fromJsonBytes(byte[] json, JsonDeserializer<T> deserializer, String name)
            throws IOException {
        try {
            return JsonSerdeUtils.readValue(json, deserializer);
        } catch (UncheckedIOException e) {
            throw new IOException("Failed to deserialize " + name + " from JSON.", e.getCause());
        } catch (RuntimeException e) {
            throw new IOException("Failed to deserialize " + name + " from JSON.", e);
        }
    }

    /** Returns the field of the node, failing if it is missing. */
    public static JsonNode getField(JsonNode node, String field) {
        JsonNode fieldNode = node.get(field);
        checkArgument(fieldNode != null, "Missing field %s.", field);
        return fieldNode;
    }

    /** Returns the array field of the node, failing if it is missing or not an array. */
    public static JsonNode getArrayField(JsonNode node, String field) {
        JsonNode fieldNode = getField(node, field);
        checkArgument(fieldNode.isArray(), "Field %s must be an array.", field);
        return fieldNode;
    }

    /** Writes the given Iceberg JSON as the value of the field. */
    public static void writeIcebergJsonField(JsonGenerator generator, String field, String json)
            throws IOException {
        generator.writeFieldName(field);
        generator.writeRawValue(json);
    }

    /** Writes the partition specs, ordered by spec id. */
    public static void writeSpecs(JsonGenerator generator, Map<Integer, PartitionSpec> specs)
            throws IOException {
        generator.writeArrayFieldStart(SPECS_FIELD);
        for (PartitionSpec spec : new TreeMap<>(specs).values()) {
            generator.writeStartObject();
            writeIcebergJsonField(generator, SCHEMA_FIELD, SchemaParser.toJson(spec.schema()));
            writeIcebergJsonField(generator, SPEC_FIELD, PartitionSpecParser.toJson(spec));
            generator.writeEndObject();
        }
        generator.writeEndArray();
    }

    /** Reads the partition specs written by {@link #writeSpecs}, keyed by spec id. */
    public static Map<Integer, PartitionSpec> readSpecs(JsonNode node) {
        Map<Integer, PartitionSpec> specs = new HashMap<>();
        for (JsonNode specNode : getArrayField(node, SPECS_FIELD)) {
            PartitionSpec spec =
                    PartitionSpecParser.fromJson(
                            SchemaParser.fromJson(getField(specNode, SCHEMA_FIELD).toString()),
                            getField(specNode, SPEC_FIELD).toString());
            specs.put(spec.specId(), spec);
        }
        return specs;
    }

    /** Writes the data or delete files as an array field. The spec of each file must be known. */
    public static void writeContentFiles(
            JsonGenerator generator,
            String field,
            Collection<? extends ContentFile<?>> files,
            Map<Integer, PartitionSpec> specs)
            throws IOException {
        generator.writeArrayFieldStart(field);
        for (ContentFile<?> file : files) {
            PartitionSpec spec = specs.get(file.specId());
            if (spec == null) {
                throw new IOException(
                        String.format(
                                "Missing partition spec %s of file %s.",
                                file.specId(), file.location()));
            }
            generator.writeRawValue(ContentFileParser.toJson(file, spec));
        }
        generator.writeEndArray();
    }

    /** Reads the data files written by {@link #writeContentFiles}. */
    public static List<DataFile> readDataFiles(
            JsonNode node, String field, Map<Integer, PartitionSpec> specs) {
        List<DataFile> dataFiles = new ArrayList<>();
        for (ContentFile<?> file : readContentFiles(node, field, specs)) {
            checkArgument(
                    file instanceof DataFile,
                    "Field %s must contain data files, but got %s.",
                    field,
                    file.content());
            dataFiles.add((DataFile) file);
        }
        return dataFiles;
    }

    /** Reads the delete files written by {@link #writeContentFiles}. */
    public static List<DeleteFile> readDeleteFiles(
            JsonNode node, String field, Map<Integer, PartitionSpec> specs) {
        List<DeleteFile> deleteFiles = new ArrayList<>();
        for (ContentFile<?> file : readContentFiles(node, field, specs)) {
            checkArgument(
                    file instanceof DeleteFile,
                    "Field %s must contain delete files, but got %s.",
                    field,
                    file.content());
            deleteFiles.add((DeleteFile) file);
        }
        return deleteFiles;
    }

    /** Writes the rewrite result as an object. */
    public static void writeRewriteDataFileResult(
            JsonGenerator generator,
            RewriteDataFileResult result,
            Map<Integer, PartitionSpec> specs)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(SNAPSHOT_ID_FIELD, result.snapshotId());
        writeContentFiles(generator, DELETED_DATA_FILES_FIELD, result.deletedDataFiles(), specs);
        writeContentFiles(generator, ADDED_DATA_FILES_FIELD, result.addedDataFiles(), specs);
        generator.writeEndObject();
    }

    /** Reads the rewrite result written by {@link #writeRewriteDataFileResult}. */
    public static RewriteDataFileResult readRewriteDataFileResult(
            JsonNode node, Map<Integer, PartitionSpec> specs) {
        JsonNode snapshotIdNode = getField(node, SNAPSHOT_ID_FIELD);
        checkArgument(
                snapshotIdNode.canConvertToLong(), "Field %s must be a long.", SNAPSHOT_ID_FIELD);
        return new RewriteDataFileResult(
                snapshotIdNode.asLong(),
                DataFileSet.of(readDataFiles(node, DELETED_DATA_FILES_FIELD, specs)),
                DataFileSet.of(readDataFiles(node, ADDED_DATA_FILES_FIELD, specs)));
    }

    private static List<ContentFile<?>> readContentFiles(
            JsonNode node, String field, Map<Integer, PartitionSpec> specs) {
        List<ContentFile<?>> files = new ArrayList<>();
        for (JsonNode fileNode : getArrayField(node, field)) {
            files.add(
                    JsonUtil.parse(
                            fileNode.toString(),
                            icebergNode -> ContentFileParser.fromJson(icebergNode, specs)));
        }
        return files;
    }
}
