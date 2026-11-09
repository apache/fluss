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

package org.apache.fluss.lake.hudi.source;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.FlinkRowDataReaderContext;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.RecordIterators;
import org.apache.hudi.util.AvroSchemaConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.configuration.HadoopConfigurations.getParquetConf;

/** Unified reader for both Copy-On-Write and Merge-On-Read Hudi file slices. */
public class UnifiedHudiTableReader implements AutoCloseable {

    private final HoodieTableMetaClient metaClient;
    private final org.apache.flink.configuration.Configuration flinkHudiOptions;
    private final Schema tableSchema;
    private final Schema requiredSchema;
    private final int[] selectedFields;
    private final InternalSchemaManager internalSchemaManager;
    private final boolean caseSensitive;
    private final int batchSize;
    private final boolean emitDelete;
    private final List<Predicate> predicates;
    private final String latestCommitTime;
    private final Option<InstantRange> instantRangeOpt;

    private ClosableIterator<RowData> currentIterator;
    private boolean closed;

    private UnifiedHudiTableReader(
            HoodieTableMetaClient metaClient,
            org.apache.flink.configuration.Configuration flinkHudiOptions,
            Schema tableSchema,
            Schema requiredSchema,
            int[] selectedFields,
            InternalSchemaManager internalSchemaManager,
            boolean caseSensitive,
            int batchSize,
            boolean emitDelete,
            List<Predicate> predicates,
            String latestCommitTime) {
        this.metaClient = metaClient;
        this.flinkHudiOptions = flinkHudiOptions;
        this.tableSchema = tableSchema;
        this.requiredSchema = requiredSchema;
        this.selectedFields = selectedFields;
        this.internalSchemaManager = internalSchemaManager;
        this.caseSensitive = caseSensitive;
        this.batchSize = batchSize;
        this.emitDelete = emitDelete;
        this.predicates = predicates;
        this.latestCommitTime = latestCommitTime;
        this.instantRangeOpt = Option.empty();
    }

    public ClosableIterator<RowData> readFileSlice(FileSlice fileSlice) throws IOException {
        if (closed) {
            throw new IllegalStateException("Reader is already closed.");
        }
        closeCurrentIterator();
        if (fileSlice.getLogFiles().findAny().isPresent()) {
            currentIterator = readMorFileSlice(fileSlice);
        } else {
            currentIterator = readCowFileSlice(fileSlice);
        }
        return currentIterator;
    }

    private ClosableIterator<RowData> readCowFileSlice(FileSlice fileSlice) throws IOException {
        if (!fileSlice.getBaseFile().isPresent()) {
            throw new IllegalArgumentException("COW file slice must have a base file.");
        }

        HoodieBaseFile baseFile = fileSlice.getBaseFile().get();
        String filePath = baseFile.getPath();
        Path hadoopPath = new Path(filePath);

        List<String> fieldNames =
                tableSchema.getFields().stream()
                        .map(Schema.Field::name)
                        .collect(Collectors.toList());
        List<DataType> fieldTypes =
                tableSchema.getFields().stream()
                        .map(field -> AvroSchemaConverter.convertToDataType(field.schema()))
                        .collect(Collectors.toList());
        LinkedHashMap<String, Object> partitionSpec =
                FilePathUtils.generatePartitionSpecs(
                        filePath,
                        fieldNames,
                        fieldTypes,
                        flinkHudiOptions.get(FlinkOptions.PARTITION_DEFAULT_NAME),
                        flinkHudiOptions.get(FlinkOptions.PARTITION_PATH_FIELD),
                        flinkHudiOptions.get(FlinkOptions.HIVE_STYLE_PARTITIONING));

        return RecordIterators.getParquetRecordIterator(
                internalSchemaManager,
                flinkHudiOptions.get(FlinkOptions.READ_UTC_TIMEZONE),
                caseSensitive,
                getParquetConf(
                        flinkHudiOptions, HadoopConfigurations.getHadoopConf(flinkHudiOptions)),
                fieldNames.toArray(new String[0]),
                fieldTypes.toArray(new DataType[0]),
                partitionSpec,
                selectedFields,
                batchSize,
                new org.apache.flink.core.fs.Path(hadoopPath.toUri()),
                0L,
                baseFile.getFileLen(),
                predicates);
    }

    private ClosableIterator<RowData> readMorFileSlice(FileSlice fileSlice) throws IOException {
        FlinkRowDataReaderContext readerContext =
                new FlinkRowDataReaderContext(
                        metaClient.getStorageConf(),
                        () -> internalSchemaManager,
                        predicates,
                        metaClient.getTableConfig(),
                        instantRangeOpt);

        TypedProperties typedProperties = getReadProps(metaClient, flinkHudiOptions);
        typedProperties.put(
                HoodieReaderConfig.MERGE_TYPE.key(), flinkHudiOptions.get(FlinkOptions.MERGE_TYPE));

        return HoodieFileGroupReader.<RowData>newBuilder()
                .withReaderContext(readerContext)
                .withHoodieTableMetaClient(metaClient)
                .withLatestCommitTime(latestCommitTime)
                .withFileSlice(fileSlice)
                .withDataSchema(tableSchema)
                .withRequestedSchema(requiredSchema)
                .withInternalSchema(Option.ofNullable(internalSchemaManager.getQuerySchema()))
                .withProps(typedProperties)
                .withShouldUseRecordPosition(false)
                .withEmitDelete(emitDelete)
                .build()
                .getClosableIterator();
    }

    public static TypedProperties getReadProps(
            HoodieTableMetaClient metaClient,
            org.apache.flink.configuration.Configuration flinkHudiOptions) {
        TypedProperties properties = new TypedProperties();
        properties.putAll(metaClient.getTableConfig().getProps());
        properties.putAll(flinkHudiOptions.toMap());
        return properties;
    }

    private void closeCurrentIterator() {
        if (currentIterator != null) {
            currentIterator.close();
            currentIterator = null;
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closeCurrentIterator();
        closed = true;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for {@link UnifiedHudiTableReader}. */
    public static class Builder {
        private HoodieTableMetaClient metaClient;
        private org.apache.flink.configuration.Configuration flinkHudiOptions;
        private Schema tableSchema;
        private Schema requiredSchema;
        private int[] selectedFields;
        private InternalSchemaManager internalSchemaManager = InternalSchemaManager.DISABLED;
        private boolean caseSensitive = true;
        private int batchSize = 2048;
        private boolean emitDelete;
        private List<Predicate> predicates = Collections.emptyList();
        private String latestCommitTime;

        public Builder withMetaClient(HoodieTableMetaClient metaClient) {
            this.metaClient = metaClient;
            return this;
        }

        public Builder withProps(org.apache.flink.configuration.Configuration flinkHudiOptions) {
            this.flinkHudiOptions = flinkHudiOptions;
            return this;
        }

        public Builder withTableSchema(Schema tableSchema) {
            this.tableSchema = tableSchema;
            return this;
        }

        public Builder withRequiredSchema(Schema requiredSchema) {
            this.requiredSchema = requiredSchema;
            return this;
        }

        public Builder withSelectedFields(int[] selectedFields) {
            this.selectedFields = selectedFields;
            return this;
        }

        public Builder withInternalSchemaManager(InternalSchemaManager internalSchemaManager) {
            this.internalSchemaManager = internalSchemaManager;
            return this;
        }

        public Builder withCaseSensitive(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
            return this;
        }

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder withEmitDelete(boolean emitDelete) {
            this.emitDelete = emitDelete;
            return this;
        }

        public Builder withPredicates(List<Predicate> predicates) {
            this.predicates = predicates;
            return this;
        }

        public Builder withLatestCommitTime(String latestCommitTime) {
            this.latestCommitTime = latestCommitTime;
            return this;
        }

        public UnifiedHudiTableReader build() {
            if (metaClient == null) {
                throw new IllegalArgumentException("metaClient is required.");
            }
            if (flinkHudiOptions == null) {
                throw new IllegalArgumentException("flinkHudiOptions is required.");
            }
            if (tableSchema == null) {
                throw new IllegalArgumentException("tableSchema is required.");
            }
            if (requiredSchema == null) {
                requiredSchema = tableSchema;
            }
            if (selectedFields == null) {
                throw new IllegalArgumentException("selectedFields is required.");
            }
            if (latestCommitTime == null) {
                throw new IllegalArgumentException("latestCommitTime is required.");
            }
            return new UnifiedHudiTableReader(
                    metaClient,
                    flinkHudiOptions,
                    tableSchema,
                    requiredSchema,
                    selectedFields,
                    internalSchemaManager,
                    caseSensitive,
                    batchSize,
                    emitDelete,
                    predicates,
                    latestCommitTime);
        }
    }
}
