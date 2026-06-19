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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.hudi.utils.HudiTableInfo;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.lake.hudi.HudiLakeCatalog.SYSTEM_COLUMNS;
import static org.apache.fluss.lake.hudi.utils.HudiConversions.toChangeType;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** Record reader for Hudi tables. */
public class HudiRecordReader implements RecordReader {

    private static final String HUDI_METADATA_COLUMN_PREFIX = "_hoodie_";

    private final HudiRecordAsFlussRecordIterator iterator;

    public HudiRecordReader(
            Configuration hudiConfig,
            TablePath tablePath,
            HudiSplit hudiSplit,
            @Nullable int[][] project)
            throws Exception {
        this(hudiConfig, tablePath, hudiSplit, project, java.util.Collections.emptyList());
    }

    public HudiRecordReader(
            Configuration hudiConfig,
            TablePath tablePath,
            HudiSplit hudiSplit,
            @Nullable int[][] project,
            List<ExpressionPredicates.Predicate> predicates)
            throws Exception {
        FileSlice fileSlice = hudiSplit.getFileSlice();
        try (HudiTableInfo hudiTableInfo = HudiTableInfo.create(tablePath, hudiConfig)) {
            Schema avroSchema =
                    StreamerUtil.getTableAvroSchema(hudiTableInfo.getMetaClient(), true);
            org.apache.flink.configuration.Configuration flinkHudiOptions =
                    buildFlinkHudiOptions(hudiTableInfo, tablePath, avroSchema);

            int metadataFieldCount = metadataFieldCount(avroSchema);
            int[] selectedFields = selectedFields(avroSchema, project, metadataFieldCount);
            Schema requiredSchema = createRequiredSchema(avroSchema, selectedFields);

            InternalSchemaManager internalSchemaManager =
                    InternalSchemaManager.get(
                            new HadoopStorageConfiguration(
                                    HadoopConfigurations.getHadoopConf(flinkHudiOptions)),
                            hudiTableInfo.getMetaClient());

            boolean emitDelete =
                    hudiTableInfo.getTableType() == HoodieTableType.MERGE_ON_READ
                            && (flinkHudiOptions
                                            .getString(FlinkOptions.QUERY_TYPE)
                                            .equals(FlinkOptions.QUERY_TYPE_SNAPSHOT)
                                    || flinkHudiOptions
                                            .getString(FlinkOptions.QUERY_TYPE)
                                            .equals(FlinkOptions.QUERY_TYPE_INCREMENTAL));

            UnifiedHudiTableReader unifiedHudiTableReader =
                    UnifiedHudiTableReader.newBuilder()
                            .withMetaClient(hudiTableInfo.getMetaClient())
                            .withInternalSchemaManager(internalSchemaManager)
                            .withProps(flinkHudiOptions)
                            .withTableSchema(avroSchema)
                            .withRequiredSchema(requiredSchema)
                            .withSelectedFields(selectedFields)
                            .withPredicates(predicates)
                            .withEmitDelete(emitDelete)
                            .withLatestCommitTime(fileSlice.getLatestInstantTime())
                            .build();

            ClosableIterator<RowData> hudiRecordIterator =
                    unifiedHudiTableReader.readFileSlice(fileSlice);
            this.iterator =
                    new HudiRecordAsFlussRecordIterator(
                            hudiRecordIterator,
                            requiredSchema,
                            metadataFieldCount,
                            SYSTEM_COLUMNS.size());
        }
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        return iterator;
    }

    private static org.apache.flink.configuration.Configuration buildFlinkHudiOptions(
            HudiTableInfo hudiTableInfo, TablePath tablePath, Schema avroSchema) {
        Map<String, String> hudiOptions = new HashMap<>(hudiTableInfo.getTableOptions());
        hudiOptions.put(FlinkOptions.PATH.key(), hudiTableInfo.getBasePath());
        hudiOptions.put(FlinkOptions.TABLE_NAME.key(), tablePath.getTableName());
        hudiOptions.put(FlinkOptions.SOURCE_AVRO_SCHEMA.key(), avroSchema.toString());
        return org.apache.flink.configuration.Configuration.fromMap(hudiOptions);
    }

    private static int metadataFieldCount(Schema schema) {
        int metadataFieldCount = 0;
        for (Schema.Field field : schema.getFields()) {
            if (!field.name().startsWith(HUDI_METADATA_COLUMN_PREFIX)) {
                break;
            }
            metadataFieldCount++;
        }
        return metadataFieldCount;
    }

    private static int[] selectedFields(
            Schema schema, @Nullable int[][] project, int metadataFieldCount) {
        if (project == null) {
            return IntStream.range(0, schema.getFields().size()).toArray();
        }

        int[] hudiMetadataFields = IntStream.range(0, metadataFieldCount).toArray();
        int[] projectedDataFields =
                Arrays.stream(project)
                        .filter(projectPath -> projectPath.length > 0)
                        .mapToInt(projectPath -> projectPath[0] + metadataFieldCount)
                        .toArray();
        int[] systemFields =
                SYSTEM_COLUMNS.keySet().stream()
                        .mapToInt(systemColumn -> schema.getField(systemColumn).pos())
                        .toArray();

        return IntStream.concat(
                        IntStream.concat(
                                IntStream.of(hudiMetadataFields),
                                IntStream.of(projectedDataFields)),
                        IntStream.of(systemFields))
                .toArray();
    }

    private static Schema createRequiredSchema(Schema avroSchema, int[] selectedFields) {
        DataType rowDataType = AvroSchemaConverter.convertToDataType(avroSchema);
        RowType rowType = (RowType) rowDataType.getLogicalType();
        return AvroSchemaCache.intern(
                new Schema.Parser()
                        .parse(
                                AvroSchemaConverter.convertToSchema(
                                                producedDataType(rowType, selectedFields)
                                                        .notNull()
                                                        .getLogicalType())
                                        .toString()));
    }

    private static DataType producedDataType(RowType rowType, int[] selectedFields) {
        List<String> fieldNames = rowType.getFieldNames();
        List<LogicalType> fieldLogicalTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .collect(Collectors.toList());
        List<DataType> fieldDataTypes =
                fieldLogicalTypes.stream().map(DataTypes::of).collect(Collectors.toList());

        return DataTypes.ROW(
                        Arrays.stream(selectedFields)
                                .mapToObj(
                                        pos ->
                                                DataTypes.FIELD(
                                                        fieldNames.get(pos),
                                                        fieldDataTypes.get(pos)))
                                .toArray(DataTypes.Field[]::new))
                .bridgedTo(RowData.class);
    }

    /** Iterator for Hudi {@link RowData} as Fluss {@link LogRecord}. */
    public static class HudiRecordAsFlussRecordIterator implements CloseableIterator<LogRecord> {

        private final ClosableIterator<RowData> hudiRecordIterator;
        private final ProjectedRow projectedRow;
        private final HudiRowAsFlussRow hudiRowAsFlussRow;
        private final int logOffsetColIndex;
        private final int timestampColIndex;

        private boolean closed;

        public HudiRecordAsFlussRecordIterator(
                ClosableIterator<RowData> hudiRecordIterator,
                Schema schema,
                int metadataFieldCount,
                int systemFieldCount) {
            this.hudiRecordIterator = hudiRecordIterator;
            this.logOffsetColIndex = schema.getField(OFFSET_COLUMN_NAME).pos();
            this.timestampColIndex = schema.getField(TIMESTAMP_COLUMN_NAME).pos();
            this.hudiRowAsFlussRow = new HudiRowAsFlussRow();
            this.projectedRow =
                    ProjectedRow.from(
                            IntStream.range(
                                            metadataFieldCount,
                                            schema.getFields().size() - systemFieldCount)
                                    .toArray());
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            try {
                hudiRecordIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            } finally {
                closed = true;
            }
        }

        @Override
        public boolean hasNext() {
            return !closed && hudiRecordIterator.hasNext();
        }

        @Override
        public LogRecord next() {
            RowData rowData = hudiRecordIterator.next();
            ChangeType changeType = toChangeType(rowData.getRowKind());
            long offset = rowData.getLong(logOffsetColIndex);
            long timestamp = rowData.getTimestamp(timestampColIndex, 6).getMillisecond();

            return new GenericRecord(
                    offset,
                    timestamp,
                    changeType,
                    projectedRow.replaceRow(hudiRowAsFlussRow.replaceRow(rowData)));
        }
    }
}
