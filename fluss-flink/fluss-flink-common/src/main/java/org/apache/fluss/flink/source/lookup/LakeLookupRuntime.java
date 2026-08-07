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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.DataLakeUtils;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.FixedSchemaDecoder;
import org.apache.fluss.row.encode.KeyEncoder;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Runtime for blocking point lookups against a lake table. */
final class LakeLookupRuntime implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final org.apache.fluss.types.RowType flussFullRowType;
    private final int[] primaryKeyIndexes;
    private final Map<String, String> tableOptions;

    @Nullable private transient LakeTableLookuper lakeTableLookuper;
    @Nullable private transient FixedSchemaDecoder lakeValueDecoder;
    @Nullable private transient KeyEncoder lakePrimaryKeyEncoder;
    @Nullable private transient KeyEncoder lakeBucketKeyEncoder;
    @Nullable private transient BucketingFunction bucketingFunction;

    @Nullable
    private transient org.apache.fluss.client.table.getter.PartitionGetter partitionGetter;

    private transient short lakeSchemaId;
    private transient int numBuckets;

    LakeLookupRuntime(
            Configuration flussConfig,
            TablePath tablePath,
            org.apache.fluss.types.RowType flussFullRowType,
            int[] primaryKeyIndexes,
            Map<String, String> tableOptions) {
        this.flussConfig = checkNotNull(flussConfig, "flussConfig must not be null.");
        this.tablePath = checkNotNull(tablePath, "tablePath must not be null.");
        this.flussFullRowType =
                checkNotNull(flussFullRowType, "flussFullRowType must not be null.");
        this.primaryKeyIndexes =
                checkNotNull(primaryKeyIndexes, "primaryKeyIndexes must not be null.");
        this.tableOptions = checkNotNull(tableOptions, "tableOptions must not be null.");
    }

    void open(TableInfo tableInfo) {
        TableInfo resolvedTableInfo = checkNotNull(tableInfo, "tableInfo must not be null.");
        DataLakeFormat dataLakeFormat = validateAndGetDataLakeFormat(resolvedTableInfo);
        org.apache.fluss.types.RowType lookupRowType = flussFullRowType.project(primaryKeyIndexes);
        lakePrimaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        lookupRowType,
                        resolvedTableInfo.getPhysicalPrimaryKeys(),
                        resolvedTableInfo.getTableConfig(),
                        resolvedTableInfo.isDefaultBucketKey());
        lakeBucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        lookupRowType,
                        resolvedTableInfo.getBucketKeys(),
                        resolvedTableInfo.getTableConfig(),
                        resolvedTableInfo.isDefaultBucketKey(),
                        lakePrimaryKeyEncoder);
        bucketingFunction = BucketingFunction.of(dataLakeFormat);
        partitionGetter =
                new org.apache.fluss.client.table.getter.PartitionGetter(
                        lookupRowType, resolvedTableInfo.getPartitionKeys());
        numBuckets = resolvedTableInfo.getNumBuckets();
        lakeSchemaId = (short) resolvedTableInfo.getSchemaId();
        lakeValueDecoder =
                new FixedSchemaDecoder(
                        resolvedTableInfo.getTableConfig().getKvFormat(),
                        resolvedTableInfo.getSchema());
        lakeTableLookuper = createLakeTableLookuper(dataLakeFormat, resolvedTableInfo);
    }

    LakeLookupKey createLookupKey(RowData normalizedKeyRow) {
        InternalRow lookupRow = new FlinkAsFlussRow(normalizedKeyRow);
        KeyEncoder primaryKeyEncoder =
                checkNotNull(
                        lakePrimaryKeyEncoder, "Lake primary-key encoder must be initialized.");
        byte[] keyBytes = primaryKeyEncoder.encodeKey(lookupRow);
        byte[] bucketKeyBytes =
                lakeBucketKeyEncoder == primaryKeyEncoder
                        ? keyBytes
                        : checkNotNull(
                                        lakeBucketKeyEncoder,
                                        "Lake bucket-key encoder must be initialized.")
                                .encodeKey(lookupRow);
        int bucketId =
                checkNotNull(bucketingFunction, "Bucketing function must be initialized.")
                        .bucketing(bucketKeyBytes, numBuckets);
        ResolvedPartitionSpec partitionSpec =
                checkNotNull(partitionGetter, "Partition getter must be initialized.")
                        .getResolvedPartitionSpec(lookupRow);
        LakeTableLookuper.LookupContext lookupContext =
                new LakeTableLookuper.LookupContext(
                        partitionSpec, bucketId, lakeSchemaId, flussFullRowType);
        return new LakeLookupKey(keyBytes, lookupContext);
    }

    @Nullable
    InternalRow lookup(LakeLookupKey lakeLookupKey) throws Exception {
        byte[] value =
                checkNotNull(lakeTableLookuper, "Lake table lookuper must be initialized.")
                        .lookup(lakeLookupKey.keyBytes, lakeLookupKey.lookupContext);
        if (value == null) {
            return null;
        }
        return checkNotNull(lakeValueDecoder, "Lake value decoder must be initialized.")
                .decode(MemorySegment.wrap(value));
    }

    void close() throws Exception {
        if (lakeTableLookuper != null) {
            lakeTableLookuper.close();
        }
    }

    private DataLakeFormat validateAndGetDataLakeFormat(TableInfo tableInfo) {
        DataLakeFormat dataLakeFormat =
                checkNotNull(
                        tableInfo.getTableConfig().getDataLakeFormat().orElse(null),
                        "Data lake format must be configured for lake fallback lookup.");
        if (dataLakeFormat != DataLakeFormat.PAIMON) {
            throw new TableException(
                    "Hybrid lake lookup currently only supports Paimon, but table "
                            + tablePath
                            + " uses "
                            + dataLakeFormat
                            + ".");
        }
        return dataLakeFormat;
    }

    private LakeTableLookuper createLakeTableLookuper(
            DataLakeFormat dataLakeFormat, TableInfo tableInfo) {
        Configuration tableConfiguration = Configuration.fromMap(tableOptions);
        Map<String, String> lakeCatalogProperties =
                DataLakeUtils.extractLakeCatalogProperties(tableConfiguration);
        LakeStoragePlugin lakeStoragePlugin =
                LakeStoragePluginSetUp.fromDataLakeFormat(dataLakeFormat.toString(), null);
        LakeStorage lakeStorage =
                checkNotNull(lakeStoragePlugin, "Lake storage plugin must not be null.")
                        .createLakeStorage(Configuration.fromMap(lakeCatalogProperties));
        return checkNotNull(
                lakeStorage.createLakeTableLookuper(
                        tablePath,
                        new LakeStorage.LookuperContext(
                                flussConfig.get(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR),
                                tableInfo.getTableConfig())),
                "Lake table lookuper must not be null.");
    }

    /** The encoded lake lookup key and its lake lookup context. */
    static final class LakeLookupKey {
        private final byte[] keyBytes;
        private final LakeTableLookuper.LookupContext lookupContext;

        private LakeLookupKey(byte[] keyBytes, LakeTableLookuper.LookupContext lookupContext) {
            this.keyBytes = keyBytes;
            this.lookupContext = lookupContext;
        }

        ResolvedPartitionSpec getPartitionSpec() {
            return lookupContext.partitionSpec();
        }
    }
}
