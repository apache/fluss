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

package org.apache.fluss.lake.hudi.tiering;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.catalog.CatalogOptions;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.fluss.lake.hudi.utils.HudiConversions.toHudiObjectPath;

/** Resolved Hudi table metadata and write client used by the Hudi lake writer. */
public class HudiWriteTableInfo implements AutoCloseable {

    private static final String DEFAULT_HUDI_WAREHOUSE = "/tmp/hudi_warehouse";
    private static final String FLUSS_CONF_PREFIX = "fluss.";

    private final TablePath tablePath;
    private final Configuration hudiConfig;
    private final HoodieFlinkWriteClient writeClient;
    private final HoodieTableMetaClient metaClient;
    private final HoodieEngineContext engineContext;
    private final HoodieWriteConfig writeConfig;
    private final org.apache.flink.configuration.Configuration flinkConfig;
    private final Schema avroSchema;
    private final RowType rowType;
    private final List<String> logicalTypes;
    private final String basePath;
    private boolean closed;

    private HudiWriteTableInfo(
            TablePath tablePath,
            Configuration hudiConfig,
            HoodieFlinkWriteClient writeClient,
            HoodieTableMetaClient metaClient,
            HoodieEngineContext engineContext,
            HoodieWriteConfig writeConfig,
            org.apache.flink.configuration.Configuration flinkConfig,
            Schema avroSchema,
            RowType rowType,
            List<String> logicalTypes,
            String basePath) {
        this.tablePath = tablePath;
        this.hudiConfig = hudiConfig;
        this.writeClient = writeClient;
        this.metaClient = metaClient;
        this.engineContext = engineContext;
        this.writeConfig = writeConfig;
        this.flinkConfig = flinkConfig;
        this.avroSchema = avroSchema;
        this.rowType = rowType;
        this.logicalTypes = logicalTypes;
        this.basePath = basePath;
    }

    public static HudiWriteTableInfo create(
            HudiCatalogProvider hudiCatalogProvider, TablePath tablePath) throws IOException {
        Catalog hudiCatalog = hudiCatalogProvider.get();
        try {
            CatalogBaseTable hudiTable = hudiCatalog.getTable(toHudiObjectPath(tablePath));
            Map<String, String> hudiOptions = new HashMap<>(hudiTable.getOptions());
            hudiOptions.putAll(hudiCatalogProvider.getHudiConfig().toMap());

            String basePath = resolveBasePath(tablePath, hudiOptions);
            hudiOptions.put(FlinkOptions.TABLE_NAME.key(), tablePath.getTableName());
            hudiOptions.put(FlinkOptions.PATH.key(), basePath);

            boolean isCowTable =
                    Objects.equals(
                            hudiOptions.get(FlinkOptions.TABLE_TYPE.key()),
                            HoodieTableType.COPY_ON_WRITE.name());
            hudiOptions.putIfAbsent(
                    FlinkOptions.OPERATION.key(),
                    isCowTable
                            ? WriteOperationType.INSERT.value()
                            : WriteOperationType.UPSERT.value());
            hudiOptions.putIfAbsent(
                    FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(),
                    hudiOptions.getOrDefault(
                            FLUSS_CONF_PREFIX + ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(),
                            "false"));

            HoodieTableMetaClient metaClient =
                    createMetaClient(basePath, hudiCatalogProvider.getHudiConfig());
            org.apache.flink.configuration.Configuration flinkConfig =
                    org.apache.flink.configuration.Configuration.fromMap(hudiOptions);
            try {
                CompactionUtil.setAvroSchema(flinkConfig, metaClient);
            } catch (Exception e) {
                throw new IOException("Failed to resolve Hudi Avro schema for " + tablePath, e);
            }

            HoodieWriteConfig writeConfig =
                    FlinkWriteClients.getHoodieClientConfig(flinkConfig, false, false);
            HoodieEngineContext engineContext =
                    new HoodieLocalEngineContext(metaClient.getStorageConf());
            HoodieFlinkWriteClient writeClient =
                    new HoodieFlinkWriteClient<>(HoodieFlinkEngineContext.DEFAULT, writeConfig);
            Schema avroSchema = Schema.parse(writeConfig.getSchema());
            RowType rowType =
                    (RowType) AvroSchemaConverter.convertToDataType(avroSchema).getLogicalType();

            List<String> logicalTypes = new ArrayList<>();
            for (RowType.RowField rowField : rowType.getFields()) {
                logicalTypes.add(rowField.getType().getTypeRoot().name());
            }

            return new HudiWriteTableInfo(
                    tablePath,
                    hudiCatalogProvider.getHudiConfig(),
                    writeClient,
                    metaClient,
                    engineContext,
                    writeConfig,
                    flinkConfig,
                    avroSchema,
                    rowType,
                    logicalTypes,
                    basePath);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("Failed to resolve Hudi write table info for " + tablePath, e);
        } finally {
            closeCatalog(hudiCatalog);
        }
    }

    private static HoodieTableMetaClient createMetaClient(String basePath, Configuration hudiConfig)
            throws IOException {
        try {
            return HoodieTableMetaClient.builder()
                    .setBasePath(basePath)
                    .setConf(new HadoopStorageConfiguration(getHadoopConfiguration(hudiConfig)))
                    .build();
        } catch (TableNotFoundException e) {
            throw new IOException("Hudi table not found at " + basePath + ".", e);
        }
    }

    public static org.apache.hadoop.conf.Configuration getHadoopConfiguration(
            Configuration hudiConfig) {
        org.apache.flink.configuration.Configuration flinkConfig =
                org.apache.flink.configuration.Configuration.fromMap(hudiConfig.toMap());
        return HadoopConfigurations.getHadoopConf(flinkConfig);
    }

    private static String resolveBasePath(TablePath tablePath, Map<String, String> tableOptions) {
        String path = tableOptions.get(FlinkOptions.PATH.key());
        if (path != null && !path.trim().isEmpty()) {
            return path;
        }
        String catalogPath =
                tableOptions.getOrDefault(
                        CatalogOptions.CATALOG_PATH.key(), DEFAULT_HUDI_WAREHOUSE);
        return ensureEndsWithSlash(catalogPath)
                + tablePath.getDatabaseName()
                + "/"
                + tablePath.getTableName();
    }

    private static String ensureEndsWithSlash(String path) {
        return path.endsWith("/") ? path : path + "/";
    }

    private static void closeCatalog(Catalog hudiCatalog) {
        IOUtils.closeQuietly(hudiCatalog::close, "hudi catalog");
    }

    public HoodieTableMetaClient getMetaClient() {
        return metaClient;
    }

    public HoodieEngineContext getEngineContext() {
        return engineContext;
    }

    public HoodieTableFileSystemView getFileSystemView() {
        return HoodieTableFileSystemView.fileListingBasedFileSystemView(
                engineContext, metaClient, getCompletedCommitsTimeline());
    }

    public HoodieTimeline getCompletedCommitsTimeline() {
        return metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants();
    }

    public HoodieTableType getTableType() {
        return metaClient.getTableType();
    }

    public String getBasePath() {
        return metaClient.getBasePath().toString();
    }

    public String getTableBasePath() {
        return basePath;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public Configuration getHudiConfig() {
        return hudiConfig;
    }

    public HoodieWriteConfig getWriteConfig() {
        return writeConfig;
    }

    public org.apache.flink.configuration.Configuration getFlinkConfig() {
        return flinkConfig;
    }

    public Schema getAvroSchema() {
        return avroSchema;
    }

    public HoodieFlinkWriteClient getWriteClient() {
        return writeClient;
    }

    public RowType getRowType() {
        return rowType;
    }

    public List<String> getLogicalTypes() {
        return logicalTypes;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        IOUtils.closeQuietly(writeClient::close, "hudi write client");
    }
}
