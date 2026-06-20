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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.hudi.utils.HudiTableInfo;
import org.apache.fluss.lake.source.SortedRecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.InternalRowUtils;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Hudi record reader that exposes primary-key ordering for Fluss union read. */
public class HudiSortedRecordReader implements SortedRecordReader {

    private final Comparator<InternalRow> comparator;
    private final @Nullable HudiRecordReader delegate;

    public HudiSortedRecordReader(
            Configuration hudiConfig,
            TablePath tablePath,
            @Nullable HudiSplit hudiSplit,
            @Nullable int[][] project,
            List<ExpressionPredicates.Predicate> predicates)
            throws Exception {
        try (HudiTableInfo hudiTableInfo = HudiTableInfo.create(tablePath, hudiConfig)) {
            Schema avroSchema =
                    StreamerUtil.getTableAvroSchema(hudiTableInfo.getMetaClient(), true);
            this.comparator =
                    createPrimaryKeyComparator(
                            avroSchema, primaryKeyFields(hudiTableInfo, tablePath));
        }
        this.delegate =
                hudiSplit == null
                        ? null
                        : new HudiRecordReader(
                                hudiConfig, tablePath, hudiSplit, project, predicates);
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        return delegate == null ? CloseableIterator.emptyIterator() : delegate.read();
    }

    @Override
    public Comparator<InternalRow> order() {
        return comparator;
    }

    private static String[] primaryKeyFields(HudiTableInfo hudiTableInfo, TablePath tablePath) {
        Option<String[]> recordKeyFields =
                hudiTableInfo.getMetaClient().getTableConfig().getRecordKeyFields();
        if (!recordKeyFields.isPresent()
                || recordKeyFields.get() == null
                || recordKeyFields.get().length == 0) {
            throw new IllegalStateException(
                    "No Hudi record key fields configured for primary-key table "
                            + tablePath
                            + ".");
        }
        return recordKeyFields.get();
    }

    @VisibleForTesting
    static Comparator<InternalRow> createPrimaryKeyComparator(
            Schema avroSchema, String[] primaryKeyFields) {
        List<KeyFieldComparator> fieldComparators = new ArrayList<>(primaryKeyFields.length);
        for (int i = 0; i < primaryKeyFields.length; i++) {
            String primaryKeyField = primaryKeyFields[i];
            Schema.Field field = avroSchema.getField(primaryKeyField);
            if (field == null) {
                throw new IllegalArgumentException(
                        "Primary key field '"
                                + primaryKeyField
                                + "' does not exist in Hudi schema.");
            }
            DataType dataType =
                    toFlussDataType(
                            AvroSchemaConverter.convertToDataType(field.schema()).getLogicalType());
            fieldComparators.add(new KeyFieldComparator(i, dataType));
        }
        return new HudiPrimaryKeyComparator(fieldComparators);
    }

    private static DataType toFlussDataType(LogicalType logicalType) {
        boolean nullable = logicalType.isNullable();
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return DataTypes.BOOLEAN().copy(nullable);
            case TINYINT:
                return DataTypes.TINYINT().copy(nullable);
            case SMALLINT:
                return DataTypes.SMALLINT().copy(nullable);
            case INTEGER:
                return DataTypes.INT().copy(nullable);
            case BIGINT:
                return DataTypes.BIGINT().copy(nullable);
            case FLOAT:
                return DataTypes.FLOAT().copy(nullable);
            case DOUBLE:
                return DataTypes.DOUBLE().copy(nullable);
            case CHAR:
                return DataTypes.CHAR(
                                ((org.apache.flink.table.types.logical.CharType) logicalType)
                                        .getLength())
                        .copy(nullable);
            case VARCHAR:
                return DataTypes.STRING().copy(nullable);
            case BINARY:
                return DataTypes.BINARY(
                                ((org.apache.flink.table.types.logical.BinaryType) logicalType)
                                        .getLength())
                        .copy(nullable);
            case VARBINARY:
                return DataTypes.BYTES().copy(nullable);
            case DECIMAL:
                org.apache.flink.table.types.logical.DecimalType decimalType =
                        (org.apache.flink.table.types.logical.DecimalType) logicalType;
                return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale())
                        .copy(nullable);
            case DATE:
                return DataTypes.DATE().copy(nullable);
            case TIME_WITHOUT_TIME_ZONE:
                return DataTypes.TIME(
                                ((org.apache.flink.table.types.logical.TimeType) logicalType)
                                        .getPrecision())
                        .copy(nullable);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return DataTypes.TIMESTAMP(
                                ((org.apache.flink.table.types.logical.TimestampType) logicalType)
                                        .getPrecision())
                        .copy(nullable);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return DataTypes.TIMESTAMP_LTZ(
                                ((org.apache.flink.table.types.logical.LocalZonedTimestampType)
                                                logicalType)
                                        .getPrecision())
                        .copy(nullable);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Hudi primary key type for sorted reader: "
                                + logicalType.getTypeRoot()
                                + ".");
        }
    }

    private static class HudiPrimaryKeyComparator implements Comparator<InternalRow>, Serializable {

        private static final long serialVersionUID = 1L;

        private final List<KeyFieldComparator> fieldComparators;

        private HudiPrimaryKeyComparator(List<KeyFieldComparator> fieldComparators) {
            this.fieldComparators = fieldComparators;
        }

        @Override
        public int compare(InternalRow first, InternalRow second) {
            for (KeyFieldComparator fieldComparator : fieldComparators) {
                int result = fieldComparator.compare(first, second);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
    }

    private static class KeyFieldComparator implements Serializable {

        private static final long serialVersionUID = 1L;

        private final InternalRow.FieldGetter fieldGetter;
        private final DataTypeRoot typeRoot;

        private KeyFieldComparator(int fieldPosition, DataType dataType) {
            this.fieldGetter = InternalRow.createFieldGetter(dataType, fieldPosition);
            this.typeRoot = dataType.getTypeRoot();
        }

        private int compare(InternalRow first, InternalRow second) {
            Object firstValue = fieldGetter.getFieldOrNull(first);
            Object secondValue = fieldGetter.getFieldOrNull(second);

            if (firstValue == null && secondValue == null) {
                return 0;
            }
            if (firstValue == null) {
                return -1;
            }
            if (secondValue == null) {
                return 1;
            }
            return InternalRowUtils.compare(firstValue, secondValue, typeRoot);
        }
    }
}
