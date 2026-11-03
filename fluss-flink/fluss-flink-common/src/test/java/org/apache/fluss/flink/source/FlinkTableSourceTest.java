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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KvBatchStrategy;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkTableSource}. */
class FlinkTableSourceTest {

    @Test
    void testServerScanDoesNotRequireLakePlugin() {
        Configuration scanConf =
                new Configuration()
                        .set(
                                ConfigOptions.CLIENT_SCANNER_KV_BATCH_STRATEGY,
                                KvBatchStrategy.SERVER_SCAN);
        FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                new FlinkConnectorOptionsUtils.StartupOptions();
        startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.FULL;

        FlinkTableSource source =
                new FlinkTableSource(
                        TablePath.of("db", "pk_table"),
                        scanConf,
                        new TableConfig(new Configuration()),
                        (RowType)
                                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()))
                                        .getLogicalType(),
                        new int[] {0},
                        new int[] {0},
                        new int[0],
                        false,
                        startupOptions,
                        false,
                        false,
                        null,
                        -1L,
                        true,
                        null,
                        Collections.singletonMap(
                                ConfigOptions.TABLE_DATALAKE_FORMAT.key(),
                                DataLakeFormat.PAIMON.toString()),
                        LeaseContext.DEFAULT);

        assertThat(source.getScanRuntimeProvider(null).isBounded()).isTrue();
        assertThat(((FlinkTableSource) source.copy()).getScanRuntimeProvider(null).isBounded())
                .isTrue();
    }
}
