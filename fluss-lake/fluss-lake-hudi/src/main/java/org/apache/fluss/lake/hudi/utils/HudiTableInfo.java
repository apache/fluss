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

package org.apache.fluss.lake.hudi.utils;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.hudi.utils.catalog.HudiCatalogUtils;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.catalog.CatalogOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.hudi.utils.HudiConversions.FLUSS_BUCKET_AWARE_OPTION;
import static org.apache.fluss.lake.hudi.utils.HudiConversions.FLUSS_BUCKET_KEYS_OPTION;
import static org.apache.fluss.lake.hudi.utils.HudiConversions.FLUSS_PARTITION_KEYS_OPTION;
import static org.apache.fluss.lake.hudi.utils.HudiConversions.toHudiObjectPath;

/** Resolved Hudi table metadata used by lake source planning. */
public class HudiTableInfo implements AutoCloseable {

    private static final String DEFAULT_HUDI_WAREHOUSE = "/tmp/hudi_warehouse";
    private static final String DELIMITER = ",";

    private final TablePath tablePath;
    private final Catalog hudiCatalog;
    private final CatalogBaseTable hudiTable;
    private final Map<String, String> tableOptions;
    private final HoodieTableMetaClient metaClient;
    private final HoodieEngineContext engineContext;
    private final HoodieTimeline completedTimeline;
    private final HoodieTableFileSystemView fileSystemView;
    private final HoodieTableType tableType;
    private final String basePath;
    private final List<String> partitionFields;
    private final boolean bucketAware;
    private boolean closed;

    private HudiTableInfo(
            TablePath tablePath,
            Catalog hudiCatalog,
            CatalogBaseTable hudiTable,
            Map<String, String> tableOptions,
            HoodieTableMetaClient metaClient,
            HoodieEngineContext engineContext,
            HoodieTimeline completedTimeline,
            HoodieTableFileSystemView fileSystemView,
            HoodieTableType tableType,
            String basePath,
            List<String> partitionFields,
            boolean bucketAware) {
        this.tablePath = tablePath;
        this.hudiCatalog = hudiCatalog;
        this.hudiTable = hudiTable;
        this.tableOptions = tableOptions;
        this.metaClient = metaClient;
        this.engineContext = engineContext;
        this.completedTimeline = completedTimeline;
        this.fileSystemView = fileSystemView;
        this.tableType = tableType;
        this.basePath = basePath;
        this.partitionFields = partitionFields;
        this.bucketAware = bucketAware;
    }

    public static HudiTableInfo create(TablePath tablePath, Configuration hudiConfig)
            throws IOException {
        Catalog hudiCatalog = HudiCatalogUtils.createHudiCatalog(hudiConfig);
        hudiCatalog.open();
        try {
            CatalogBaseTable hudiTable = hudiCatalog.getTable(toHudiObjectPath(tablePath));
            Map<String, String> tableOptions = new HashMap<>(hudiTable.getOptions());
            tableOptions.putAll(hudiConfig.toMap());

            String basePath = resolveBasePath(tablePath, tableOptions);
            tableOptions.put(FlinkOptions.PATH.key(), basePath);

            HoodieTableFileSystemView fileSystemView = null;
            HoodieTableMetaClient metaClient = createMetaClient(basePath, hudiConfig);
            HoodieTimeline completedTimeline =
                    metaClient.getCommitsTimeline().filterCompletedInstants();
            HoodieEngineContext engineContext =
                    new HoodieLocalEngineContext(metaClient.getStorageConf());
            try {
                HoodieTimeline fileSystemViewTimeline =
                        metaClient
                                .getCommitsAndCompactionTimeline()
                                .filterCompletedAndCompactionInstants();
                fileSystemView =
                        HoodieTableFileSystemView.fileListingBasedFileSystemView(
                                engineContext, metaClient, fileSystemViewTimeline);
                HoodieTableType tableType = metaClient.getTableType();
                List<String> partitionFields =
                        splitCommaSeparated(
                                tableOptions.getOrDefault(
                                        FLUSS_PARTITION_KEYS_OPTION,
                                        tableOptions.get(FlinkOptions.PARTITION_PATH_FIELD.key())));
                boolean bucketAware = resolveBucketAware(tableOptions, tableType);

                return new HudiTableInfo(
                        tablePath,
                        hudiCatalog,
                        hudiTable,
                        Collections.unmodifiableMap(new HashMap<>(tableOptions)),
                        metaClient,
                        engineContext,
                        completedTimeline,
                        fileSystemView,
                        tableType,
                        basePath,
                        Collections.unmodifiableList(new ArrayList<>(partitionFields)),
                        bucketAware);
            } catch (Exception e) {
                closeFileSystemView(fileSystemView);
                throw e;
            }
        } catch (Exception e) {
            closeCatalog(hudiCatalog);
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("Failed to resolve Hudi table info for " + tablePath + ".", e);
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

    @VisibleForTesting
    public static boolean resolveBucketAware(
            Map<String, String> tableOptions, HoodieTableType tableType) {
        String bucketAware = tableOptions.get(FLUSS_BUCKET_AWARE_OPTION);
        if (bucketAware != null) {
            return Boolean.parseBoolean(bucketAware.trim());
        }
        String bucketKeys = tableOptions.get(FLUSS_BUCKET_KEYS_OPTION);
        if (bucketKeys != null) {
            return !bucketKeys.trim().isEmpty();
        }
        return tableType == HoodieTableType.MERGE_ON_READ;
    }

    @VisibleForTesting
    public static List<String> extractPartitionValues(
            String partitionPath, List<String> partitionFields) {
        if (partitionFields.isEmpty() || partitionPath == null || partitionPath.isEmpty()) {
            return Collections.emptyList();
        }

        String[] pathSegments = partitionPath.split("/");
        if (pathSegments.length != partitionFields.size()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Hudi partition path '%s' does not match partition fields %s.",
                            partitionPath, partitionFields));
        }

        boolean hiveStyle = false;
        boolean rawStyle = false;
        Map<String, String> hivePartitionValues = new HashMap<>();
        List<String> rawPartitionValues = new ArrayList<>(pathSegments.length);
        for (String pathSegment : pathSegments) {
            int valueStart = pathSegment.indexOf('=');
            if (valueStart > 0) {
                hiveStyle = true;
                String field = pathSegment.substring(0, valueStart);
                if (!partitionFields.contains(field)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Hudi partition path '%s' contains unknown partition field '%s'.",
                                    partitionPath, field));
                }
                if (hivePartitionValues.put(field, pathSegment.substring(valueStart + 1)) != null) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Hudi partition path '%s' contains duplicate partition field '%s'.",
                                    partitionPath, field));
                }
            } else {
                rawStyle = true;
                rawPartitionValues.add(pathSegment);
            }
        }

        if (hiveStyle && rawStyle) {
            throw new IllegalArgumentException(
                    String.format(
                            "Hudi partition path '%s' mixes hive-style and raw partition segments.",
                            partitionPath));
        }

        if (!hiveStyle) {
            return rawPartitionValues;
        }

        List<String> partitionValues = new ArrayList<>(partitionFields.size());
        for (String partitionField : partitionFields) {
            String partitionValue = hivePartitionValues.get(partitionField);
            if (partitionValue == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Hudi partition path '%s' does not contain partition field '%s'.",
                                partitionPath, partitionField));
            }
            partitionValues.add(partitionValue);
        }
        return partitionValues;
    }

    private static List<String> splitCommaSeparated(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        String[] parts = value.split(DELIMITER);
        List<String> values = new ArrayList<>(parts.length);
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }
        return values;
    }

    private static String ensureEndsWithSlash(String path) {
        return path.endsWith("/") ? path : path + "/";
    }

    private static void closeCatalog(Catalog hudiCatalog) {
        if (hudiCatalog instanceof AutoCloseable) {
            IOUtils.closeQuietly((AutoCloseable) hudiCatalog, "hudi catalog");
        }
    }

    private static void closeFileSystemView(HoodieTableFileSystemView fileSystemView) {
        if (fileSystemView != null) {
            IOUtils.closeQuietly(fileSystemView::close, "hudi table file system view");
        }
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public CatalogBaseTable getHudiTable() {
        return hudiTable;
    }

    public Map<String, String> getTableOptions() {
        return tableOptions;
    }

    public HoodieTableMetaClient getMetaClient() {
        return metaClient;
    }

    public HoodieEngineContext getEngineContext() {
        return engineContext;
    }

    public HoodieTimeline getCompletedTimeline() {
        return completedTimeline;
    }

    public HoodieTableFileSystemView getFileSystemView() {
        return fileSystemView;
    }

    public HoodieTableType getTableType() {
        return tableType;
    }

    public String getBasePath() {
        return basePath;
    }

    public boolean isBucketAware() {
        return bucketAware;
    }

    public boolean isPartitioned() {
        return !partitionFields.isEmpty();
    }

    public List<String> partitionValues(String partitionPath) {
        return extractPartitionValues(partitionPath, partitionFields);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeFileSystemView(fileSystemView);
        closeCatalog(hudiCatalog);
    }
}
