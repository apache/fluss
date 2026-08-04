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

package org.apache.fluss.utils;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionTransform;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TransformType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Computes a final partition spec from physical row fields and implicit partition expressions. */
public class PartitionComputer {

    private final List<String> partitionKeys;
    private final List<PartitionFieldComputer> partitionFieldComputers;

    public PartitionComputer(TableInfo tableInfo, RowType inputRowType) {
        this(tableInfo.getPartitionKeys(), tableInfo.getPartitionExpressions(), inputRowType);
    }

    public PartitionComputer(
            List<String> partitionKeys,
            List<PartitionExpression> partitionExpressions,
            RowType inputRowType) {
        this.partitionKeys = partitionKeys;
        this.partitionFieldComputers = new ArrayList<>(partitionKeys.size());

        Map<String, PartitionExpression> expressionByKey = new HashMap<>();
        for (PartitionExpression partitionExpression : partitionExpressions) {
            checkArgument(
                    partitionExpression.getVirtualPartitionSpecKey().isPresent(),
                    "Virtual partition expression must have a resolved partition spec key.");
            String virtualPartitionKey = partitionExpression.getVirtualPartitionSpecKey().get();
            checkArgument(
                    partitionKeys.contains(virtualPartitionKey),
                    "Virtual partition spec key '%s' is not present in partition keys %s.",
                    virtualPartitionKey,
                    partitionKeys);
            checkArgument(
                    !expressionByKey.containsKey(virtualPartitionKey),
                    "Duplicate virtual partition spec key '%s'.",
                    virtualPartitionKey);
            expressionByKey.put(virtualPartitionKey, partitionExpression);
        }

        for (String partitionKey : partitionKeys) {
            PartitionExpression partitionExpression = expressionByKey.get(partitionKey);
            if (partitionExpression == null) {
                partitionFieldComputers.add(
                        PhysicalPartitionFieldComputer.create(partitionKey, inputRowType));
            } else {
                partitionFieldComputers.add(
                        TransformPartitionFieldComputer.create(partitionExpression, inputRowType));
            }
        }
    }

    /** Computes the internal partition name from the given row. */
    public String getPartition(InternalRow row) {
        return getResolvedPartitionSpec(row).getPartitionName();
    }

    /** Computes the resolved partition spec from the given row. */
    public ResolvedPartitionSpec getResolvedPartitionSpec(InternalRow row) {
        List<String> partitionValues = new ArrayList<>(partitionFieldComputers.size());
        for (PartitionFieldComputer partitionFieldComputer : partitionFieldComputers) {
            partitionValues.add(partitionFieldComputer.compute(row));
        }
        return new ResolvedPartitionSpec(partitionKeys, partitionValues);
    }

    private interface PartitionFieldComputer {
        String compute(InternalRow row);
    }

    private static class PhysicalPartitionFieldComputer implements PartitionFieldComputer {
        private final String partitionKey;
        private final DataType dataType;
        private final InternalRow.FieldGetter fieldGetter;

        private PhysicalPartitionFieldComputer(
                String partitionKey, DataType dataType, InternalRow.FieldGetter fieldGetter) {
            this.partitionKey = partitionKey;
            this.dataType = dataType;
            this.fieldGetter = fieldGetter;
        }

        private static PhysicalPartitionFieldComputer create(
                String partitionKey, RowType inputRowType) {
            int fieldIndex = inputRowType.getFieldIndex(partitionKey);
            checkArgument(
                    fieldIndex >= 0,
                    "The partition column %s is not in the row %s.",
                    partitionKey,
                    inputRowType);
            DataType dataType = inputRowType.getTypeAt(fieldIndex);
            return new PhysicalPartitionFieldComputer(
                    partitionKey, dataType, InternalRow.createFieldGetter(dataType, fieldIndex));
        }

        @Override
        public String compute(InternalRow row) {
            Object partitionValue = fieldGetter.getFieldOrNull(row);
            checkNotNull(
                    partitionValue, "Partition value for '%s' shouldn't be null.", partitionKey);
            return PartitionUtils.convertValueOfType(partitionValue, dataType.getTypeRoot());
        }
    }

    private static class TransformPartitionFieldComputer implements PartitionFieldComputer {
        private final PartitionTransform transform;
        private final DataType sourceDataType;
        private final InternalRow.FieldGetter sourceFieldGetter;

        private TransformPartitionFieldComputer(
                PartitionTransform transform,
                DataType sourceDataType,
                InternalRow.FieldGetter sourceFieldGetter) {
            this.transform = transform;
            this.sourceDataType = sourceDataType;
            this.sourceFieldGetter = sourceFieldGetter;
        }

        private static TransformPartitionFieldComputer create(
                PartitionExpression partitionExpression, RowType inputRowType) {
            PartitionTransform transform = partitionExpression.getTransform();
            checkArgument(
                    transform.getType() == TransformType.DATE_TRUNC,
                    "Unsupported partition transform type: %s.",
                    transform.getType());
            DateTruncPartitionTransform dateTruncTransform =
                    (DateTruncPartitionTransform) transform;
            int sourceFieldIndex = inputRowType.getFieldIndex(dateTruncTransform.getSourceColumn());
            checkArgument(
                    sourceFieldIndex >= 0,
                    "The partition transform source column %s is not in the row %s.",
                    dateTruncTransform.getSourceColumn(),
                    inputRowType);
            DataType sourceDataType = inputRowType.getTypeAt(sourceFieldIndex);
            return new TransformPartitionFieldComputer(
                    transform,
                    sourceDataType,
                    InternalRow.createFieldGetter(sourceDataType, sourceFieldIndex));
        }

        @Override
        public String compute(InternalRow row) {
            if (transform.getType() == TransformType.DATE_TRUNC) {
                return computeDateTrunc((DateTruncPartitionTransform) transform, row);
            }
            throw new IllegalArgumentException(
                    "Unsupported partition transform type: " + transform.getType());
        }

        private String computeDateTrunc(DateTruncPartitionTransform transform, InternalRow row) {
            Object sourceValue = sourceFieldGetter.getFieldOrNull(row);
            checkNotNull(
                    sourceValue,
                    "Partition transform source value for '%s' shouldn't be null.",
                    transform.getSourceColumn());
            ZonedDateTime zonedDateTime = toZonedDateTime(sourceValue, sourceDataType, transform);
            return PartitionUtils.generateAutoPartitionTime(
                    zonedDateTime, 0, transform.getTimeUnit());
        }

        private ZonedDateTime toZonedDateTime(
                Object sourceValue,
                DataType sourceDataType,
                DateTruncPartitionTransform transform) {
            DataTypeRoot typeRoot = sourceDataType.getTypeRoot();
            AutoPartitionTimeUnit timeUnit = transform.getTimeUnit();
            switch (typeRoot) {
                case DATE:
                    checkArgument(
                            timeUnit != AutoPartitionTimeUnit.HOUR,
                            "DATE_TRUNC partition transform does not support DATE + HOUR.");
                    // DATE is represented as epoch-day int in InternalRow.
                    return LocalDate.ofEpochDay((Integer) sourceValue).atStartOfDay(ZoneOffset.UTC);
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return ((TimestampNtz) sourceValue).toLocalDateTime().atZone(ZoneOffset.UTC);
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    ZoneId timeZone =
                            transform
                                    .getTimeZone()
                                    .orElseThrow(
                                            () ->
                                                    new IllegalArgumentException(
                                                            "DateTruncPartitionTransform must contain a resolved time zone."));
                    return ((TimestampLtz) sourceValue).toInstant().atZone(timeZone);
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "DATE_TRUNC partition transform does not support source type %s.",
                                    sourceDataType));
            }
        }
    }
}
