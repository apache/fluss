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

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.lake.iceberg.source.FlussRowAsIcebergRecord;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

import static org.apache.fluss.metadata.ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR;

/** Utility class for static conversions between Fluss and Iceberg types. */
public class IcebergConversions {

    private static final DateTimeFormatter HOUR_FORMATTER =
            DateTimeFormatter.ofPattern("uuuuMMddHH", Locale.ROOT);

    /** Convert Fluss TablePath to Iceberg TableIdentifier. */
    public static TableIdentifier toIceberg(TablePath tablePath) {
        return TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    public static PartitionKey toPartition(
            Table table, @Nullable String partitionName, int bucket) {
        PartitionSpec partitionSpec = table.spec();
        Schema schema = table.schema();
        PartitionKey partitionKey = new PartitionKey(partitionSpec, schema);
        int pos = 0;
        if (partitionName != null) {
            String[] partitionArr = partitionName.split("\\" + PARTITION_SPEC_SEPARATOR);
            for (String partition : partitionArr) {
                partitionKey.set(
                        pos, toIcebergPartitionValue(partitionSpec.fields().get(pos), partition));
                pos++;
            }
        }
        partitionKey.set(pos, bucket);
        return partitionKey;
    }

    public static Expression toFilterExpression(
            Table table, @Nullable String partitionName, int bucket) {
        List<PartitionField> partitionFields = table.spec().fields();
        Expression expression = Expressions.alwaysTrue();
        int partitionIndex = 0;
        if (partitionName != null) {
            String[] partitionArr = partitionName.split("\\" + PARTITION_SPEC_SEPARATOR);
            for (String partition : partitionArr) {
                PartitionField partitionField = partitionFields.get(partitionIndex++);
                String sourceColumnName = table.schema().findColumnName(partitionField.sourceId());
                Object icebergPartitionValue = toIcebergPartitionValue(partitionField, partition);
                String transformName = partitionField.transform().toString();
                Expression partitionExpression;
                if (partitionField.transform().isIdentity()) {
                    partitionExpression =
                            Expressions.equal(sourceColumnName, icebergPartitionValue);
                } else if ("hour".equals(transformName)) {
                    partitionExpression =
                            Expressions.equal(
                                    Expressions.hour(sourceColumnName),
                                    (Integer) icebergPartitionValue);
                } else if ("day".equals(transformName)) {
                    partitionExpression =
                            Expressions.equal(
                                    Expressions.day(sourceColumnName),
                                    (Integer) icebergPartitionValue);
                } else if ("month".equals(transformName)) {
                    partitionExpression =
                            Expressions.equal(
                                    Expressions.month(sourceColumnName),
                                    (Integer) icebergPartitionValue);
                } else if ("year".equals(transformName)) {
                    partitionExpression =
                            Expressions.equal(
                                    Expressions.year(sourceColumnName),
                                    (Integer) icebergPartitionValue);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Iceberg partition transform: " + transformName);
                }
                expression = Expressions.and(expression, partitionExpression);
            }
        }
        PartitionField bucketField = partitionFields.get(partitionFields.size() - 1);
        String bucketSourceName = table.schema().findColumnName(bucketField.sourceId());
        if (bucketField.transform().isIdentity()) {
            expression = Expressions.and(expression, Expressions.equal(bucketSourceName, bucket));
        } else {
            String transformName = bucketField.transform().toString();
            int start = transformName.indexOf('[');
            int end = transformName.indexOf(']');
            if (!transformName.startsWith("bucket") || start < 0 || end <= start) {
                throw new IllegalArgumentException(
                        "Unsupported Iceberg bucket transform: " + transformName);
            }
            int bucketCount = Integer.parseInt(transformName.substring(start + 1, end));
            expression =
                    Expressions.and(
                            expression,
                            Expressions.equal(
                                    Expressions.bucket(bucketSourceName, bucketCount), bucket));
        }
        return expression;
    }

    /** Converts a canonical Fluss partition value to the typed Iceberg partition value. */
    public static Object toIcebergPartitionValue(
            PartitionField partitionField, String flussPartitionValue) {
        if (partitionField.transform().isIdentity()) {
            return flussPartitionValue;
        }
        String transformName = partitionField.transform().toString();
        if ("hour".equals(transformName)) {
            long epochHour =
                    LocalDateTime.parse(flussPartitionValue, HOUR_FORMATTER)
                                    .toEpochSecond(ZoneOffset.UTC)
                            / 3600L;
            return Math.toIntExact(epochHour);
        } else if ("day".equals(transformName)) {
            return Math.toIntExact(
                    LocalDate.parse(flussPartitionValue, DateTimeFormatter.BASIC_ISO_DATE)
                            .toEpochDay());
        } else if ("month".equals(transformName)) {
            int year = Integer.parseInt(flussPartitionValue.substring(0, 4));
            int month = Integer.parseInt(flussPartitionValue.substring(4, 6));
            return Math.toIntExact(
                    YearMonth.from(YearMonth.of(1970, 1))
                            .until(
                                    YearMonth.of(year, month),
                                    java.time.temporal.ChronoUnit.MONTHS));
        } else if ("year".equals(transformName)) {
            return Integer.parseInt(flussPartitionValue) - 1970;
        }
        throw new IllegalArgumentException(
                "Unsupported Iceberg partition transform: " + transformName);
    }

    /** Converts a typed Iceberg partition value to the canonical Fluss partition value. */
    public static String toFlussPartitionValue(
            PartitionField partitionField, Object icebergPartitionValue) {
        if (partitionField.transform().isIdentity()) {
            return String.valueOf(icebergPartitionValue);
        }
        int transformedValue = (Integer) icebergPartitionValue;
        String transformName = partitionField.transform().toString();
        if ("hour".equals(transformName)) {
            return LocalDateTime.ofEpochSecond(transformedValue * 3600L, 0, ZoneOffset.UTC)
                    .format(HOUR_FORMATTER);
        } else if ("day".equals(transformName)) {
            return LocalDate.ofEpochDay(transformedValue).format(DateTimeFormatter.BASIC_ISO_DATE);
        } else if ("month".equals(transformName)) {
            YearMonth yearMonth = YearMonth.of(1970, 1).plusMonths(transformedValue);
            return String.format(
                    Locale.ROOT, "%04d%02d", yearMonth.getYear(), yearMonth.getMonthValue());
        } else if ("year".equals(transformName)) {
            return String.format(Locale.ROOT, "%04d", transformedValue + 1970);
        }
        throw new IllegalArgumentException(
                "Unsupported Iceberg partition transform: " + transformName);
    }

    public static Object toIcebergLiteral(
            Types.NestedField icebergField, DataType flussFieldType, Object flussLiteral) {
        InternalRow flussRow = GenericRow.of(flussLiteral);
        FlussRowAsIcebergRecord flussRowAsIcebergRecord =
                new FlussRowAsIcebergRecord(
                        Types.StructType.of(icebergField), RowType.of(flussFieldType), flussRow);
        return flussRowAsIcebergRecord.get(0, icebergField.type().typeId().javaClass());
    }
}
