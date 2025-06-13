/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.ListOffsetsResult;
import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.client.admin.OffsetSpec.EarliestSpec;
import com.alibaba.fluss.client.admin.OffsetSpec.LatestSpec;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.Scan;
import com.alibaba.fluss.client.table.scanner.batch.BatchScanUtils;
import com.alibaba.fluss.client.table.scanner.batch.BatchScanner;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.flink.source.lookup.FlinkLookupFunction;
import com.alibaba.fluss.flink.source.lookup.LookupNormalizer;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.fluss.flink.source.lookup.LookupNormalizer.createPrimaryKeyLookupNormalizer;

/** Utilities for pushdown abilities. */
public class PushdownUtils {
    private static final int MAX_LIMIT_PUSHDOWN = 2048;

    /** Extract field equality information from expressions. */
    public static List<FieldEqual> extractFieldEquals(
            List<ResolvedExpression> expressions,
            Map<Integer, LogicalType> fieldIndexToType,
            List<ResolvedExpression> acceptedFiltersResult,
            List<ResolvedExpression> remainingFiltersResult,
            ValueConversion valueConversion) {
        List<FieldEqual> fieldEquals = new ArrayList<>();
        for (ResolvedExpression expr : expressions) {
            if (expr instanceof CallExpression && expr.getChildren().size() == 2) {
                CallExpression callExpress = (CallExpression) expr;

                // Only support equals now.
                if (callExpress.getFunctionDefinition() == BuiltInFunctionDefinitions.EQUALS) {

                    ResolvedExpression left = expr.getResolvedChildren().get(0);
                    ResolvedExpression right = expr.getResolvedChildren().get(1);

                    FieldEqual fieldEqual = null;
                    if (left instanceof FieldReferenceExpression
                            && right instanceof ValueLiteralExpression) {
                        FieldReferenceExpression leftFieldRef = (FieldReferenceExpression) left;
                        ValueLiteralExpression rightValue = (ValueLiteralExpression) right;
                        fieldEqual =
                                extractFieldEqual(
                                        leftFieldRef,
                                        rightValue,
                                        fieldIndexToType,
                                        valueConversion);
                    } else if (left instanceof ValueLiteralExpression
                            && right instanceof FieldReferenceExpression) {
                        ValueLiteralExpression leftValue = (ValueLiteralExpression) left;
                        FieldReferenceExpression rightFieldRef = (FieldReferenceExpression) right;
                        fieldEqual =
                                extractFieldEqual(
                                        rightFieldRef,
                                        leftValue,
                                        fieldIndexToType,
                                        valueConversion);
                    }

                    if (fieldEqual != null) {
                        fieldEquals.add(fieldEqual);
                        acceptedFiltersResult.add(expr);
                    } else {
                        remainingFiltersResult.add(expr);
                    }
                } else {
                    remainingFiltersResult.add(expr);
                }
            } else {
                remainingFiltersResult.add(expr);
            }
        }
        return fieldEquals;
    }

    @Nullable
    private static FieldEqual extractFieldEqual(
            FieldReferenceExpression fieldsRef,
            ValueLiteralExpression valueLiteral,
            Map<Integer, LogicalType> fieldIndexToType,
            ValueConversion valueConversion) {
        int columnIndex = fieldsRef.getFieldIndex();
        if (fieldIndexToType.containsKey(columnIndex)) {
            LogicalType expectedType = fieldIndexToType.get(columnIndex);
            if (expectedType.getTypeRoot()
                    != valueLiteral.getOutputDataType().getLogicalType().getTypeRoot()) {
                return null;
            }

            final Object value;
            if (valueConversion == ValueConversion.FLINK_INTERNAL_VALUE) {
                value = toFlinkInternalValue(valueLiteral);
            } else {
                value = toFlussInternalValue(valueLiteral);
            }

            if (value == null) {
                return null;
            } else {
                return new FieldEqual(columnIndex, value);
            }
        }
        return null;
    }

    @Nullable
    private static Object toFlussInternalValue(ValueLiteralExpression valueExp) {
        LogicalType type = valueExp.getOutputDataType().getLogicalType();
        Object value;
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                value = BinaryString.fromString(valueExp.getValueAs(String.class).get());
                break;
            case BOOLEAN:
                value = valueExp.getValueAs(Boolean.class).get();
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                value =
                        Decimal.fromBigDecimal(
                                valueExp.getValueAs(BigDecimal.class).get(),
                                decimalType.getPrecision(),
                                decimalType.getScale());
                break;
            case INTEGER:
                value = valueExp.getValueAs(Integer.class).get();
                break;
            case BIGINT:
                value = valueExp.getValueAs(Long.class).get();
                break;
            case DOUBLE:
                value = valueExp.getValueAs(Double.class).get();
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                value =
                        TimestampNtz.fromLocalDateTime(
                                valueExp.getValueAs(LocalDateTime.class).get());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                value = TimestampLtz.fromInstant(valueExp.getValueAs(Instant.class).get());
                break;
            default:
                value = null;
                break;
        }
        return value;
    }

    @Nullable
    private static Object toFlinkInternalValue(ValueLiteralExpression valueExp) {
        LogicalType type = valueExp.getOutputDataType().getLogicalType();
        Object value;
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                value = BinaryStringData.fromString(valueExp.getValueAs(String.class).get());
                break;
            case BOOLEAN:
                value = valueExp.getValueAs(Boolean.class).get();
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                value =
                        DecimalData.fromBigDecimal(
                                valueExp.getValueAs(BigDecimal.class).get(),
                                decimalType.getPrecision(),
                                decimalType.getScale());
                break;
            case INTEGER:
                value = valueExp.getValueAs(Integer.class).get();
                break;
            case BIGINT:
                value = valueExp.getValueAs(Long.class).get();
                break;
            case DOUBLE:
                value = valueExp.getValueAs(Double.class).get();
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                value =
                        TimestampData.fromLocalDateTime(
                                valueExp.getValueAs(LocalDateTime.class).get());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                value = TimestampData.fromInstant(valueExp.getValueAs(Instant.class).get());
                break;
            default:
                value = null;
                break;
        }
        return value;
    }

    public static Collection<RowData> querySingleRow(
            GenericRowData lookupRow,
            TablePath tablePath,
            Configuration flussConfig,
            RowType sourceOutputType,
            int[] primaryKeyIndexes,
            int lookupMaxRetryTimes,
            @Nullable int[] projectedFields) {
        LookupNormalizer lookupNormalizer =
                createPrimaryKeyLookupNormalizer(primaryKeyIndexes, sourceOutputType);
        LookupFunction lookupFunction =
                new FlinkLookupFunction(
                        flussConfig,
                        tablePath,
                        sourceOutputType,
                        lookupMaxRetryTimes,
                        lookupNormalizer,
                        projectedFields);
        try {
            // it's fine to pass null here, as we don't use it in it
            lookupFunction.open(null);
            return lookupFunction.lookup(lookupRow);
        } catch (Exception e) {
            throw new FlussRuntimeException(e);
        }
    }

    public static void deleteSingleRow(
            GenericRow deleteRow, TablePath tablePath, Configuration flussConfig) {
        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Table table = connection.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            upsertWriter.delete(deleteRow).get();
        } catch (Exception e) {
            throw new TableException("Failed execute DELETE statement on Fluss table.", e);
        }
    }

    public static Collection<RowData> limitScan(
            TablePath tablePath,
            Configuration flussConfig,
            RowType sourceOutputType,
            @Nullable int[] projectedFields,
            long limitRowNum) {
        if (limitRowNum > MAX_LIMIT_PUSHDOWN) {
            throw new UnsupportedOperationException(
                    String.format(
                            "LIMIT statement doesn't support greater than %s", MAX_LIMIT_PUSHDOWN));
        }
        int limit = (int) limitRowNum;
        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Table table = connection.getTable(tablePath);
                Admin flussAdmin = connection.getAdmin()) {
            TableInfo tableInfo = flussAdmin.getTableInfo(tablePath).get();
            int bucketCount = tableInfo.getNumBuckets();
            List<TableBucket> tableBuckets;
            if (tableInfo.isPartitioned()) {
                List<PartitionInfo> partitionInfos = flussAdmin.listPartitionInfos(tablePath).get();
                tableBuckets =
                        partitionInfos.stream()
                                .flatMap(
                                        partitionInfo ->
                                                IntStream.range(0, bucketCount)
                                                        .mapToObj(
                                                                bucketId ->
                                                                        new TableBucket(
                                                                                tableInfo
                                                                                        .getTableId(),
                                                                                partitionInfo
                                                                                        .getPartitionId(),
                                                                                bucketId)))
                                .collect(Collectors.toList());
            } else {
                tableBuckets =
                        IntStream.range(0, bucketCount)
                                .mapToObj(
                                        bucketId ->
                                                new TableBucket(tableInfo.getTableId(), bucketId))
                                .collect(Collectors.toList());
            }

            Scan scan = table.newScan().limit(limit).project(projectedFields);
            List<BatchScanner> scanners =
                    tableBuckets.stream()
                            .map(scan::createBatchScanner)
                            .collect(Collectors.toList());
            List<InternalRow> scannedRows = BatchScanUtils.collectLimitedRows(scanners, limit);

            // convert fluss row into flink row
            List<RowData> flinkRows = new ArrayList<>();
            FlussRowToFlinkRowConverter flussRowToFlinkRowConverter =
                    new FlussRowToFlinkRowConverter(
                            projectedFields != null
                                    ? FlinkConversions.toFlussRowType(sourceOutputType)
                                            .project(projectedFields)
                                    : FlinkConversions.toFlussRowType(sourceOutputType));
            int count = 0;
            for (InternalRow row : scannedRows) {
                flinkRows.add(flussRowToFlinkRowConverter.toFlinkRowData(row));
                if (++count >= limit) {
                    break;
                }
            }

            return flinkRows;
        } catch (Exception e) {
            throw new FlussRuntimeException(e);
        }
    }

    public static long countLogTable(TablePath tablePath, Configuration flussConfig) {
        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Admin flussAdmin = connection.getAdmin()) {
            TableInfo tableInfo = flussAdmin.getTableInfo(tablePath).get();
            int bucketCount = tableInfo.getNumBuckets();
            Collection<Integer> buckets =
                    IntStream.range(0, bucketCount).boxed().collect(Collectors.toList());
            List<PartitionInfo> partitionInfos;
            if (tableInfo.isPartitioned()) {
                partitionInfos = flussAdmin.listPartitionInfos(tablePath).get();
            } else {
                partitionInfos = Collections.singletonList(null);
            }

            List<CompletableFuture<Long>> countFutureList =
                    offsetLengthes(flussAdmin, tablePath, partitionInfos, buckets);
            // wait for all the response
            CompletableFuture.allOf(countFutureList.toArray(new CompletableFuture[0])).join();
            long count = 0;
            for (CompletableFuture<Long> countFuture : countFutureList) {
                count += countFuture.get();
            }
            return count;
        } catch (Exception e) {
            throw new FlussRuntimeException(e);
        }
    }

    private static List<CompletableFuture<Long>> offsetLengthes(
            Admin flussAdmin,
            TablePath tablePath,
            List<PartitionInfo> partitionInfos,
            Collection<Integer> buckets) {
        List<CompletableFuture<Long>> list = new ArrayList<>();
        for (@Nullable PartitionInfo info : partitionInfos) {
            String partitionName = info != null ? info.getPartitionName() : null;
            ListOffsetsResult earliestOffsets =
                    listOffsets(flussAdmin, tablePath, buckets, new EarliestSpec(), partitionName);
            ListOffsetsResult latestOffsets =
                    listOffsets(flussAdmin, tablePath, buckets, new LatestSpec(), partitionName);
            CompletableFuture<Long> apply =
                    earliestOffsets
                            .all()
                            .thenCombine(
                                    latestOffsets.all(),
                                    (earliestOffsetsMap, latestOffsetsMap) -> {
                                        long count = 0;
                                        for (int bucket : earliestOffsetsMap.keySet()) {
                                            count +=
                                                    latestOffsetsMap.get(bucket)
                                                            - earliestOffsetsMap.get(bucket);
                                        }
                                        return count;
                                    });
            list.add(apply);
        }
        return list;
    }

    private static ListOffsetsResult listOffsets(
            Admin flussAdmin,
            TablePath tablePath,
            Collection<Integer> buckets,
            OffsetSpec offsetSpec,
            @Nullable String partitionName) {
        return partitionName == null
                ? flussAdmin.listOffsets(tablePath, buckets, offsetSpec)
                : flussAdmin.listOffsets(tablePath, partitionName, buckets, offsetSpec);
    }

    // ------------------------------------------------------------------------------------------

    /** A structure represents a source field equal literal expression. */
    public static class FieldEqual implements Serializable {
        private static final long serialVersionUID = 1L;
        public final int fieldIndex;
        public final Object equalValue;

        public FieldEqual(int fieldIndex, Object equalValue) {
            this.fieldIndex = fieldIndex;
            this.equalValue = equalValue;
        }
    }

    /** The value conversion type between Flink internal value and Fluss internal value. */
    public enum ValueConversion {
        FLUSS_INTERNAL_VALUE,
        FLINK_INTERNAL_VALUE
    }
}
