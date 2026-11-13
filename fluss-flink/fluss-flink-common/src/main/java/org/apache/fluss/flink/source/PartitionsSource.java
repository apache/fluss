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
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.util.List;
import java.util.stream.Collectors;

/** A bounded source that loads partition metadata once for each execution. */
final class PartitionsSource extends SingleSplitMetadataSource {

    private static final long serialVersionUID = 1L;

    PartitionsSource(
            TablePath tablePath, Configuration flussConfig, TypeInformation<RowData> producedType) {
        this(new AdminPartitionMetadataLoader(tablePath, flussConfig), producedType);
    }

    PartitionsSource(MetadataLoader metadataLoader, TypeInformation<RowData> producedType) {
        super(metadataLoader, producedType);
    }

    static RowData toRowData(PartitionInfo partitionInfo) {
        return GenericRowData.of(
                partitionInfo.getPartitionId(),
                StringData.fromString(partitionInfo.getPartitionName()),
                partitionInfo.getBucketCount());
    }

    private static final class AdminPartitionMetadataLoader implements MetadataLoader {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;

        private AdminPartitionMetadataLoader(TablePath tablePath, Configuration flussConfig) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
        }

        @Override
        public List<RowData> load() {
            try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                    Admin admin = connection.getAdmin()) {
                return admin.listPartitionInfos(tablePath).get().stream()
                        .map(PartitionsSource::toRowData)
                        .collect(Collectors.toList());
            } catch (Exception e) {
                throw new FlussRuntimeException(
                        String.format("Failed to list partitions for table %s.", tablePath),
                        ExceptionUtils.stripExecutionException(e));
            }
        }
    }
}
