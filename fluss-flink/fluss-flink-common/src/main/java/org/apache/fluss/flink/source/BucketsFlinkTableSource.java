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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.BucketInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/** A bounded Flink table source for the read-only {@code $buckets} virtual table. */
public class BucketsFlinkTableSource implements ScanTableSource {

    private final TablePath tablePath;
    private final Configuration flussConfig;
    private final RowType producedDataType;

    /** Creates a source for the bucket metadata of the given base table. */
    public BucketsFlinkTableSource(
            TablePath tablePath, Configuration flussConfig, RowType producedDataType) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        TypeInformation<RowData> resultTypeInfo =
                scanContext.createTypeInformation(producedDataType);
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                return execEnv.fromCollection(loadBucketRows(), resultTypeInfo)
                        .name("FlussBuckets-" + tablePath)
                        .setParallelism(1);
            }

            @Override
            public boolean isBounded() {
                return true;
            }
        };
    }

    private List<RowData> loadBucketRows() {
        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Admin admin = connection.getAdmin()) {
            return admin.describeBuckets(tablePath).get().stream()
                    .map(BucketsFlinkTableSource::toRowData)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to describe buckets for table %s.", tablePath),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    static RowData toRowData(BucketInfo bucketInfo) {
        return GenericRowData.of(
                StringData.fromString(bucketInfo.getTablePath().toString()),
                bucketInfo.getTableId(),
                optionalLong(bucketInfo.getPartitionId()),
                bucketInfo.getPartitionName() == null
                        ? null
                        : StringData.fromString(bucketInfo.getPartitionName()),
                bucketInfo.getBucketId(),
                optionalInt(bucketInfo.getLeaderId()),
                optionalInt(bucketInfo.getLeaderEpoch()),
                optionalInt(bucketInfo.getBucketEpoch()),
                new GenericArrayData(bucketInfo.getReplicas().toArray(new Integer[0])),
                new GenericArrayData(bucketInfo.getIsr().toArray(new Integer[0])));
    }

    private static Long optionalLong(OptionalLong value) {
        return value.isPresent() ? value.getAsLong() : null;
    }

    private static Integer optionalInt(OptionalInt value) {
        return value.isPresent() ? value.getAsInt() : null;
    }

    @Override
    public DynamicTableSource copy() {
        return new BucketsFlinkTableSource(tablePath, flussConfig, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Fluss Buckets Table Source";
    }
}
