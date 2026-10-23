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

package org.apache.fluss.server.coordinator.compaction;

import org.apache.fluss.cluster.compaction.CompactionInfo;
import org.apache.fluss.cluster.compaction.CompactionStatus;
import org.apache.fluss.cluster.compaction.CompactionType;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Versioned JSON encoding for task summaries and immutable remote metadata blocks. */
public final class CompactionTaskJsonSerde {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CompactionTaskJsonSerde() {}

    /** Encodes a bounded summary; the ZooKeeper version is not part of its data. */
    public static byte[] serialize(CompactionTask task) throws IOException {
        ObjectNode node = object();
        CompactionInfo info = task.info();
        node.put("operation_id", info.operationId());
        node.put("type", info.type().name());
        node.put("status", info.status().name());
        node.put("create_time", info.createTime());
        node.put("database", info.databaseName().orElse(null));
        node.put("table_database", info.tablePath().map(TablePath::getDatabaseName).orElse(null));
        node.put("table", info.tablePath().map(TablePath::getTableName).orElse(null));
        node.put("partition", info.partitionName().orElse(null));
        node.put("deadline", task.deadline());
        node.put("total", task.total());
        node.put("succeeded", task.succeeded());
        node.put("failed", task.failed());
        node.put("cancelled", task.cancelled());
        node.put("finish_time", task.finishTime());
        node.put("persisted_time", task.persistedTime());
        node.put("plan", path(task.planHandle()));
        node.put("progress", path(task.progressHandle()));
        node.put("first_error", task.firstError());
        return encode(node);
    }

    /** Decodes a summary with its observed ZooKeeper version. */
    public static CompactionTask deserialize(byte[] bytes, int version) throws IOException {
        JsonNode n = decode(bytes);
        String database = text(n, "database");
        String table = text(n, "table");
        return new CompactionTask(
                new CompactionInfo(
                        n.required("operation_id").asText(),
                        CompactionType.valueOf(n.required("type").asText()),
                        CompactionStatus.valueOf(n.required("status").asText()),
                        n.required("create_time").asLong(),
                        database,
                        table == null ? null : new TablePath(text(n, "table_database"), table),
                        text(n, "partition")),
                n.required("deadline").asLong(),
                n.required("total").asLong(),
                n.required("succeeded").asLong(),
                n.required("failed").asLong(),
                n.required("cancelled").asLong(),
                n.required("finish_time").asLong(),
                n.required("persisted_time").asLong(),
                handle(n, "plan"),
                handle(n, "progress"),
                text(n, "first_error"),
                version);
    }

    static ObjectNode object() {
        return MAPPER.createObjectNode().put("version", 1);
    }

    static byte[] encode(JsonNode node) throws IOException {
        return MAPPER.writeValueAsBytes(node);
    }

    static JsonNode decode(byte[] bytes) throws IOException {
        JsonNode node = MAPPER.readTree(bytes);
        if (node == null || node.path("version").asInt() != 1) {
            throw new IOException("Unsupported compaction metadata version");
        }
        return node;
    }

    @Nullable
    static String path(@Nullable FsPath path) {
        return path == null ? null : path.toString();
    }

    @Nullable
    static String text(JsonNode n, String key) {
        return n.required(key).isNull() ? null : n.required(key).asText();
    }

    @Nullable
    static FsPath handle(JsonNode n, String key) {
        String value = text(n, key);
        return value == null ? null : new FsPath(value);
    }

    static ObjectNode bucket(TableBucket bucket) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("table_id", bucket.getTableId());
        if (bucket.getPartitionId() == null) {
            n.putNull("partition_id");
        } else {
            n.put("partition_id", bucket.getPartitionId());
        }
        n.put("bucket_id", bucket.getBucket());
        return n;
    }

    static TableBucket bucket(JsonNode n) {
        return new TableBucket(
                n.required("table_id").asLong(),
                n.required("partition_id").isNull() ? null : n.required("partition_id").asLong(),
                n.required("bucket_id").asInt());
    }

    static ObjectNode progress(@Nullable FsPath previous, Map<TableBucket, Long> snapshots) {
        ObjectNode n = object();
        n.put("previous", path(previous));
        n.putArray("snapshots");
        snapshots.forEach(
                (b, id) -> n.withArray("snapshots").add(bucket(b).put("snapshot_id", id)));
        return n;
    }

    static Map<TableBucket, Long> progress(JsonNode n) {
        Map<TableBucket, Long> snapshots = new LinkedHashMap<>();
        for (JsonNode b : n.required("snapshots")) {
            if (snapshots.put(bucket(b), b.required("snapshot_id").asLong()) != null) {
                throw new IllegalArgumentException("Duplicate compaction success bucket");
            }
        }
        return snapshots;
    }
}
