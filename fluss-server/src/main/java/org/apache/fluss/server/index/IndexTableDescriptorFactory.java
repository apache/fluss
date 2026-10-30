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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Factory for descriptors of system-managed secondary index tables. */
@Internal
public final class IndexTableDescriptorFactory {

    private IndexTableDescriptorFactory() {}

    /** Derives the {@link TableDescriptor} for a system-managed secondary index table. */
    public static TableDescriptor derive(
            TableDescriptor mainDescriptor, long mainTableId, Schema.Index index) {
        boolean partitioned = mainDescriptor.isPartitioned();
        Optional<Schema.PrimaryKey> mainPk = mainDescriptor.getSchema().getPrimaryKey();
        checkState(
                mainPk.isPresent(), "Indexed main table %s must have a primary key", mainTableId);
        List<String> basePk = mainPk.get().getColumnNames();

        Set<String> seen = new LinkedHashSet<>();
        List<Schema.Column> columns = new ArrayList<>();
        for (String c : index.getColumnNames()) {
            if (seen.add(c)) {
                columns.add(
                        new Schema.Column(
                                c, lookupColumnType(mainDescriptor.getSchema(), c).copy(true)));
            }
        }
        for (String c : basePk) {
            if (seen.add(c)) {
                columns.add(
                        new Schema.Column(
                                c, lookupColumnType(mainDescriptor.getSchema(), c).copy(true)));
            }
        }
        if (partitioned) {
            columns.add(
                    new Schema.Column(
                            IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN,
                            DataTypes.BIGINT().copy(false)));
        }
        columns.add(
                new Schema.Column(
                        IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN,
                        DataTypes.TINYINT().copy(false)));
        columns.add(
                new Schema.Column(
                        IndexTableUtils.ROUTING_KEY_SYSTEM_COLUMN, DataTypes.BYTES().copy(false)));
        columns.add(
                new Schema.Column(
                        IndexTableUtils.ROW_KEY_SYSTEM_COLUMN, DataTypes.BYTES().copy(false)));
        columns.add(
                new Schema.Column(
                        IndexTableUtils.SOURCE_PROGRESS_SYSTEM_COLUMN, DataTypes.BIGINT()));
        List<String> physicalPrimaryKey = new ArrayList<>();
        physicalPrimaryKey.add(IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN);
        physicalPrimaryKey.add(IndexTableUtils.ROUTING_KEY_SYSTEM_COLUMN);
        if (partitioned) {
            physicalPrimaryKey.add(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        }
        physicalPrimaryKey.add(IndexTableUtils.ROW_KEY_SYSTEM_COLUMN);
        Schema derivedSchema =
                Schema.newBuilder().fromColumns(columns).primaryKey(physicalPrimaryKey).build();

        Integer bucketCount =
                index.getBucketCount()
                        .orElseGet(
                                () ->
                                        mainDescriptor
                                                .getTableDistribution()
                                                .flatMap(
                                                        TableDescriptor.TableDistribution
                                                                ::getBucketCount)
                                                .orElse(null));

        TableDescriptor.Builder b =
                TableDescriptor.builder()
                        .schema(derivedSchema)
                        .kvFormat(KvFormat.COMPACTED)
                        .logFormat(LogFormat.COMPACTED)
                        .property(ConfigOptions.TABLE_CHANGELOG_IMAGE, ChangelogImage.WAL)
                        .property(ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_ID, mainTableId)
                        .property(
                                ConfigOptions.TABLE_KV_FORMAT_VERSION,
                                ConfigOptions.KV_FORMAT_VERSION_2);
        if (partitioned) {
            b.property(ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION, KvValueLayout.TAGGED.version());
        }
        if (bucketCount != null) {
            b.distributedBy(
                    bucketCount,
                    java.util.Arrays.asList(
                            IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN,
                            IndexTableUtils.ROUTING_KEY_SYSTEM_COLUMN));
        }
        return b.build().withReplicationFactor(mainDescriptor.getReplicationFactor());
    }

    private static DataType lookupColumnType(Schema schema, String column) {
        Schema.Column col =
                schema.getColumns().stream()
                        .filter(c -> c.getName().equals(column))
                        .findFirst()
                        .orElse(null);
        checkArgument(col != null, "Unknown column '%s'", column);
        return col.getDataType();
    }
}
