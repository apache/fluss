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

package org.apache.fluss.server.index;

import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IndexReplicationEncoder}. */
class IndexReplicationEncoderTest {

    private static final TablePath MAIN_PATH = TablePath.of("db", "main");
    private static final TablePath INDEX_PATH =
            TablePath.of("db", IndexTableUtils.indexTableName("main", "idx_name"));

    @Test
    void testMainTableRescalePreservesExplicitIndexRouting() {
        TableDescriptor original = mainDescriptor(2);
        TableDescriptor index =
                IndexTableDescriptorFactory.derive(
                        original, 1L, original.getSchema().getIndexes().get(0));
        TabletServerMetadataCache cache = indexCache(index);
        TableBucket source = new TableBucket(1L, 10L, 0);
        IndexReplicationEncoder before =
                IndexReplicationEncoder.createAll(tableInfo(MAIN_PATH, 1L, original), source, cache)
                        .get(0);
        TableDescriptor rescaled = TableDescriptor.builder(original).distributedBy(5, "id").build();
        IndexReplicationEncoder after =
                IndexReplicationEncoder.createAll(tableInfo(MAIN_PATH, 1L, rescaled), source, cache)
                        .get(0);

        for (int id = 0; id < 20; id++) {
            GenericRow row = GenericRow.of(id, fromString("name" + id), fromString("p"));
            IndexReplicationEncoder.IndexEntry expected = before.encodeDataEntry(row);
            IndexReplicationEncoder.IndexEntry actual = after.encodeDataEntry(row);
            assertThat(actual.targetBucket()).isEqualTo(expected.targetBucket());
            assertThat(actual.key()).isEqualTo(expected.key());
        }
    }

    @Test
    void testExplicitIndexBucketCountMismatchIsRejected() {
        TableDescriptor main = mainDescriptor(2);
        TableDescriptor index =
                IndexTableDescriptorFactory.derive(main, 1L, main.getSchema().getIndexes().get(0));
        TabletServerMetadataCache cache =
                indexCache(
                        TableDescriptor.builder(index)
                                .distributedBy(3, index.getBucketKeys())
                                .build());

        assertThatThrownBy(
                        () ->
                                IndexReplicationEncoder.createAll(
                                        tableInfo(MAIN_PATH, 1L, main),
                                        new TableBucket(1L, 10L, 0),
                                        cache))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bucket count 2 but found 3");
    }

    private static TableDescriptor mainDescriptor(Integer indexBucketCount) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("p", DataTypes.STRING())
                        .primaryKey("id", "p")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Collections.singletonList("name"),
                                IndexVisibility.SYNC,
                                indexBucketCount)
                        .build();
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(3, "id")
                .partitionedBy("p")
                .build()
                .withReplicationFactor(3);
    }

    private static TabletServerMetadataCache indexCache(TableDescriptor descriptor) {
        TableMetadata metadata =
                new TableMetadata(tableInfo(INDEX_PATH, 2L, descriptor), Collections.emptyList());
        TabletServerMetadataCache cache =
                new TabletServerMetadataCache(null) {
                    @Override
                    public Optional<TableMetadata> getTableMetadata(TablePath tablePath) {
                        return INDEX_PATH.equals(tablePath)
                                ? Optional.of(metadata)
                                : Optional.empty();
                    }
                };
        cache.updateTableMetadata(metadata);
        return cache;
    }

    private static TableInfo tableInfo(TablePath path, long id, TableDescriptor descriptor) {
        return TableInfo.of(path, id, 1, descriptor, "file:///remote", 1L, 1L);
    }
}
