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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.lookup.LookupType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.utils.ArrayUtils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A utility class to normalize the lookup key row to match the Fluss key fields order and drop the
 * lookup result that doesn't match remaining conditions.
 *
 * <p>For example, if we have a Fluss table with the following schema: <code>
 * [id: int, name: string, age: int, score: double]</code> with primary key (name, id). And a lookup
 * condition <code>dim.id = src.id AND dim.name = src.name AND dim.age = 32</code>. The lookup key
 * row will be <code>[1001, "Alice", 32]</code>. We need to normalize the lookup key row into <code>
 * ["Alice", 1001]</code>, and construct a remaining filter for <code>{age == 32}</code>.
 */
public class LookupNormalizer implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Mapping from normalized key index to the lookup key index (in the lookup row). */
    @Nullable private final FieldGetter[] normalizedKeyGetters;

    /** The field getter to get remaining condition value from the lookup key row. */
    @Nullable private final FieldGetter[] conditionFieldGetters;

    /** The field getter to get the remaining condition result from the lookup result row. */
    @Nullable private final FieldGetter[] resultFieldGetters;

    /** The logical types of the remaining conditions. */
    @Nullable private final LogicalTypeRoot[] conditionTypeRoots;

    /**
     * The lookup request type (primary key lookup or prefix lookup) requested from Flink to Fluss.
     */
    private final LookupType lookupType;

    /** The indexes of the lookup keys in the lookup key row. */
    private final int[] lookupKeyIndexes;

    private LookupNormalizer(
            LookupType lookupType,
            int[] lookupKeyIndexes,
            @Nullable FieldGetter[] normalizedKeyGetters,
            @Nullable FieldGetter[] conditionFieldGetters,
            @Nullable FieldGetter[] resultFieldGetters,
            @Nullable LogicalTypeRoot[] conditionTypeRoots) {
        this.lookupType = lookupType;
        this.lookupKeyIndexes = lookupKeyIndexes;
        this.normalizedKeyGetters = normalizedKeyGetters;
        this.conditionFieldGetters = conditionFieldGetters;
        this.resultFieldGetters = resultFieldGetters;
        this.conditionTypeRoots = conditionTypeRoots;
        if (conditionFieldGetters != null) {
            checkState(resultFieldGetters != null, "The resultFieldGetters should not be null.");
            checkState(conditionTypeRoots != null, "The conditionTypeRoots should not be null.");
            checkState(
                    conditionFieldGetters.length == resultFieldGetters.length
                            && conditionFieldGetters.length == conditionTypeRoots.length,
                    "The remaining condition getters and types should have the same length.");
        }
    }

    /**
     * Returns the lookup type (primary key lookup, or prefix key lookup) requested from Flink to
     * Fluss.
     */
    public LookupType getLookupType() {
        return lookupType;
    }

    /** Returns the indexes of the normalized lookup keys. */
    public int[] getLookupKeyIndexes() {
        return lookupKeyIndexes;
    }

    /** Normalize the lookup key row to match the request key and the key fields order. */
    public RowData normalizeLookupKey(RowData lookupKey) {
        if (normalizedKeyGetters == null) {
            return lookupKey;
        }

        GenericRowData normalizedKey = new GenericRowData(normalizedKeyGetters.length);
        for (int i = 0; i < normalizedKeyGetters.length; i++) {
            normalizedKey.setField(i, normalizedKeyGetters[i].getFieldOrNull(lookupKey));
        }
        return normalizedKey;
    }

    @Nullable
    public RemainingFilter createRemainingFilter(RowData lookupKey) {
        if (conditionFieldGetters == null
                || resultFieldGetters == null
                || conditionTypeRoots == null) {
            return null;
        }

        FieldCondition[] fieldConditions = new FieldCondition[conditionFieldGetters.length];
        for (int i = 0; i < conditionFieldGetters.length; i++) {
            fieldConditions[i] =
                    new FieldCondition(
                            copyConditionValue(conditionFieldGetters[i].getFieldOrNull(lookupKey)),
                            resultFieldGetters[i],
                            conditionTypeRoots[i]);
        }
        return new RemainingFilter(fieldConditions);
    }

    private static Object copyConditionValue(Object value) {
        if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        if (value instanceof StringData) {
            return StringData.fromString(value.toString());
        }
        return value;
    }

    /** A filter to check if the lookup result matches the remaining conditions. */
    public static class RemainingFilter {
        private final FieldCondition[] fieldConditions;

        private RemainingFilter(FieldCondition[] fieldConditions) {
            this.fieldConditions = fieldConditions;
        }

        public boolean isMatch(RowData result) {
            for (FieldCondition condition : fieldConditions) {
                if (!condition.fieldMatches(result)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class FieldCondition {
        private final Object expectedValue;
        private final FieldGetter resultFieldGetter;
        private final LogicalTypeRoot typeRoot;

        private FieldCondition(
                Object expectedValue, FieldGetter resultFieldGetter, LogicalTypeRoot typeRoot) {
            this.expectedValue = expectedValue;
            this.resultFieldGetter = resultFieldGetter;
            this.typeRoot = typeRoot;
        }

        public boolean fieldMatches(RowData result) {
            Object fieldValue = resultFieldGetter.getFieldOrNull(result);
            if (typeRoot == LogicalTypeRoot.BINARY || typeRoot == LogicalTypeRoot.VARBINARY) {
                return Arrays.equals((byte[]) expectedValue, (byte[]) fieldValue);
            }
            return Objects.equals(expectedValue, fieldValue);
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Create a {@link LookupNormalizer} for primary key lookup. */
    public static LookupNormalizer createPrimaryKeyLookupNormalizer(
            int[] primaryKeys, RowType schema) {
        List<String> primaryKeyNames = fieldNames(primaryKeys, schema);
        return createLookupNormalizer(
                primaryKeyNames, primaryKeyNames, primaryKeys, schema, LookupType.LOOKUP);
    }

    /**
     * Validate the lookup key indexes and primary keys, and create a {@link LookupNormalizer}.
     *
     * <p>This is a backward-compatible overload that delegates to the 7-param version with {@code
     * null} for secondary indexes.
     *
     * @param lookupKeyIndexes the indexes of the lookup keys in the table row
     * @param primaryKeys the indexes of the primary keys of the table
     * @param bucketKeys the indexes of the bucket keys of the table, must be a part of primary keys
     * @param partitionKeys the indexes of the partition keys of the table, maybe empty if the table
     *     is not partitioned
     * @param schema the schema of the table
     * @param projectedFields the projected fields, or null if no projection pushdown
     */
    public static LookupNormalizer validateAndCreateLookupNormalizer(
            int[][] lookupKeyIndexes,
            int[] primaryKeys,
            int[] bucketKeys,
            int[] partitionKeys,
            RowType schema,
            @Nullable int[] projectedFields) {
        return validateAndCreateLookupNormalizer(
                lookupKeyIndexes,
                primaryKeys,
                bucketKeys,
                partitionKeys,
                schema,
                projectedFields,
                null);
    }

    /**
     * Validate the lookup key indexes and primary keys, and create a {@link LookupNormalizer}.
     *
     * @param lookupKeyIndexes the indexes of the lookup keys in the table row
     * @param primaryKeys the indexes of the primary keys of the table
     * @param bucketKeys the indexes of the bucket keys of the table, must be a part of primary keys
     * @param partitionKeys the indexes of the partition keys of the table, maybe empty if the table
     *     is not partitioned
     * @param schema the schema of the table
     * @param projectedFields the projected fields, or null if no projection pushdown
     * @param secondaryIndexes the column indexes of secondary indexes, or null if none defined
     */
    public static LookupNormalizer validateAndCreateLookupNormalizer(
            int[][] lookupKeyIndexes,
            int[] primaryKeys,
            int[] bucketKeys,
            int[] partitionKeys,
            RowType schema,
            @Nullable int[] projectedFields,
            @Nullable int[][] secondaryIndexes) {
        validateTableStructure(primaryKeys, bucketKeys, partitionKeys);
        LookupKeyInfo lookupKeyInfo = resolveLookupKeys(lookupKeyIndexes, projectedFields, schema);
        return createNormalizerByLookupType(
                lookupKeyInfo, primaryKeys, bucketKeys, partitionKeys, schema, secondaryIndexes);
    }

    private static void validateTableStructure(
            int[] primaryKeys, int[] bucketKeys, int[] partitionKeys) {
        if (primaryKeys.length == 0) {
            throw new UnsupportedOperationException(
                    "Fluss lookup function only support lookup table with primary key.");
        }
        // bucket keys must not be empty
        if (bucketKeys.length == 0 || !ArrayUtils.isSubset(primaryKeys, bucketKeys)) {
            throw new IllegalArgumentException(
                    "Illegal bucket keys: "
                            + Arrays.toString(bucketKeys)
                            + ", must be a part of primary keys: "
                            + Arrays.toString(primaryKeys));
        }
        // partition keys can be empty
        if (partitionKeys.length != 0 && !ArrayUtils.isSubset(primaryKeys, partitionKeys)) {
            throw new IllegalArgumentException(
                    "Illegal partition keys: "
                            + Arrays.toString(partitionKeys)
                            + ", must be a part of primary keys: "
                            + Arrays.toString(primaryKeys));
        }
    }

    private static LookupKeyInfo resolveLookupKeys(
            int[][] lookupKeyIndexes, @Nullable int[] projectedFields, RowType schema) {
        int[] lookupKeysBeforeProjection = new int[lookupKeyIndexes.length];
        int[] lookupKeys = new int[lookupKeyIndexes.length];
        for (int i = 0; i < lookupKeysBeforeProjection.length; i++) {
            int[] innerKeyArr = lookupKeyIndexes[i];
            checkArgument(innerKeyArr.length == 1, "Do not support nested lookup keys");
            // lookupKeyIndexes passed by Flink is key indexed after projection pushdown,
            // we restore the lookup key indexes before pushdown to easier compare with primary
            // keys.
            if (projectedFields != null) {
                lookupKeysBeforeProjection[i] = projectedFields[innerKeyArr[0]];
            } else {
                lookupKeysBeforeProjection[i] = innerKeyArr[0];
            }
            lookupKeys[i] = innerKeyArr[0];
        }
        List<String> lookupKeyNames = fieldNames(lookupKeysBeforeProjection, schema);
        return new LookupKeyInfo(lookupKeysBeforeProjection, lookupKeys, lookupKeyNames);
    }

    private static LookupNormalizer createNormalizerByLookupType(
            LookupKeyInfo lookupKeyInfo,
            int[] primaryKeys,
            int[] bucketKeys,
            int[] partitionKeys,
            RowType schema,
            @Nullable int[][] secondaryIndexes) {
        List<String> lookupKeyNames = lookupKeyInfo.names;
        List<String> primaryKeyNames = fieldNames(primaryKeys, schema);

        if (new HashSet<>(lookupKeyNames).containsAll(primaryKeyNames)) {
            // primary key lookup.
            return createLookupNormalizer(
                    lookupKeyNames,
                    primaryKeyNames,
                    lookupKeyInfo.indexes,
                    schema,
                    LookupType.LOOKUP);
        }

        // try secondary index lookup (supports remaining filters when lookup keys are superset)
        int[] matchedIndex =
                findMatchingSecondaryIndex(lookupKeyInfo.beforeProjection, secondaryIndexes);
        if (matchedIndex != null) {
            List<String> indexKeyNames = fieldNames(matchedIndex, schema);
            return createLookupNormalizer(
                    lookupKeyNames,
                    indexKeyNames,
                    lookupKeyInfo.indexes,
                    schema,
                    LookupType.SECONDARY_INDEX_LOOKUP);
        }

        // try prefix lookup
        return tryCreatePrefixLookupNormalizer(
                lookupKeyInfo, primaryKeys, bucketKeys, partitionKeys, schema, primaryKeyNames);
    }

    /**
     * Find a secondary index whose columns are a subset of (or equal to) the lookup keys. Returns
     * the matched index columns, or null if no match.
     */
    @Nullable
    private static int[] findMatchingSecondaryIndex(
            int[] lookupKeyIndexes, @Nullable int[][] secondaryIndexes) {
        if (secondaryIndexes == null) {
            return null;
        }
        HashSet<Integer> lookupSet = new HashSet<>();
        for (int idx : lookupKeyIndexes) {
            lookupSet.add(idx);
        }
        int[] bestMatch = null;
        for (int[] indexColumns : secondaryIndexes) {
            boolean allContained = true;
            for (int col : indexColumns) {
                if (!lookupSet.contains(col)) {
                    allContained = false;
                    break;
                }
            }
            if (allContained && isBetterIndexMatch(indexColumns, bestMatch)) {
                bestMatch = indexColumns;
            }
        }
        return bestMatch;
    }

    private static boolean isBetterIndexMatch(int[] candidate, @Nullable int[] current) {
        if (current == null || candidate.length != current.length) {
            return current == null || candidate.length > current.length;
        }
        for (int i = 0; i < candidate.length; i++) {
            if (candidate[i] != current[i]) {
                return candidate[i] < current[i];
            }
        }
        return false;
    }

    static String findMatchingSecondaryIndexName(Schema schema, List<String> lookupColumns) {
        for (Schema.Index index : schema.getIndexes()) {
            if (index.getColumnNames().equals(lookupColumns)) {
                return index.getIndexName();
            }
        }
        throw new IllegalStateException(
                "No secondary index found matching lookup columns: " + lookupColumns);
    }

    private static LookupNormalizer tryCreatePrefixLookupNormalizer(
            LookupKeyInfo lookupKeyInfo,
            int[] primaryKeys,
            int[] bucketKeys,
            int[] partitionKeys,
            RowType schema,
            List<String> primaryKeyNames) {
        // the encoding primary key is the primary key without partition keys.
        int[] encodedPrimaryKeys = ArrayUtils.removeSet(primaryKeys, partitionKeys);
        // the table support prefix lookup iff the bucket key is a prefix of the encoding pk
        boolean supportPrefixLookup = ArrayUtils.isPrefix(encodedPrimaryKeys, bucketKeys);
        if (supportPrefixLookup) {
            // try to create prefix lookup normalizer
            // TODO: support prefix lookup with arbitrary part of prefix of primary key
            int[] expectedLookupKeys =
                    ArrayUtils.intersection(
                            primaryKeys, ArrayUtils.concat(bucketKeys, partitionKeys));
            return createLookupNormalizer(
                    lookupKeyInfo.names,
                    fieldNames(expectedLookupKeys, schema),
                    lookupKeyInfo.indexes,
                    schema,
                    LookupType.PREFIX_LOOKUP);
        } else {
            // throw exception for tables that doesn't support prefix lookup
            throw new TableException(
                    "The Fluss lookup function supports lookup tables where the lookup keys "
                            + "include all primary keys, all bucket keys, or match a secondary "
                            + "index. The primary keys are "
                            + primaryKeyNames
                            + ", but the lookup keys are "
                            + lookupKeyInfo.names);
        }
    }

    /** Resolved lookup key information after projection pushdown. */
    private static class LookupKeyInfo {
        final int[] beforeProjection;
        final int[] indexes;
        final List<String> names;

        LookupKeyInfo(int[] beforeProjection, int[] indexes, List<String> names) {
            this.beforeProjection = beforeProjection;
            this.indexes = indexes;
            this.names = names;
        }
    }

    /**
     * Create a {@link LookupNormalizer}.
     *
     * <p>Note: We compare string names rather than int index for better error message and
     * readability, the length of lookup key and keys (primary key or index key) shouldn't be large,
     * so the overhead is low.
     *
     * @param originalLookupKeys the original lookup keys that is determined by Flink.
     * @param expectedLookupKeys the expected lookup keys to lookup Fluss.
     */
    private static LookupNormalizer createLookupNormalizer(
            List<String> originalLookupKeys,
            List<String> expectedLookupKeys,
            int[] lookupKeyIndexes,
            RowType schema,
            LookupType lookupType) {
        if (originalLookupKeys.equals(expectedLookupKeys)) {
            int[] normalizedLookupKeys = fieldIndexes(expectedLookupKeys, schema);
            // Always create identity field getters to produce a defensive copy in
            // normalizeLookupKey(). Flink's temporal join may reuse or mutate the underlying
            // RowData buffer between successive lookup() calls, so returning the caller's object
            // without copying can corrupt reference-typed fields (e.g., StringData backed by a
            // shared MemorySegment).
            FieldGetter[] identityGetters = new FieldGetter[expectedLookupKeys.size()];
            for (int i = 0; i < expectedLookupKeys.size(); i++) {
                String key = expectedLookupKeys.get(i);
                LogicalType fieldType = schema.getTypeAt(schema.getFieldIndex(key));
                identityGetters[i] = RowData.createFieldGetter(fieldType, i);
            }
            return new LookupNormalizer(
                    lookupType, normalizedLookupKeys, identityGetters, null, null, null);
        }

        FieldGetter[] normalizedKeyGetters = new FieldGetter[expectedLookupKeys.size()];
        for (int i = 0; i < expectedLookupKeys.size(); i++) {
            String expectedKey = expectedLookupKeys.get(i);
            LogicalType fieldType = schema.getTypeAt(schema.getFieldIndex(expectedKey));
            int idxInLookupKey = findIndex(originalLookupKeys, expectedKey);
            normalizedKeyGetters[i] = RowData.createFieldGetter(fieldType, idxInLookupKey);
        }

        List<FieldGetter> conditionFieldGetters = new ArrayList<>();
        List<FieldGetter> resultFieldGetters = new ArrayList<>();
        List<LogicalTypeRoot> conditionTypeRoots = new ArrayList<>();
        for (int i = 0; i < originalLookupKeys.size(); i++) {
            String originalKey = originalLookupKeys.get(i);
            if (!expectedLookupKeys.contains(originalKey)) {
                LogicalType fieldType = schema.getTypeAt(schema.getFieldIndex(originalKey));
                // get the condition field from the original lookup key row
                conditionFieldGetters.add(RowData.createFieldGetter(fieldType, i));
                // get the result field from the lookup result row (projected)
                resultFieldGetters.add(RowData.createFieldGetter(fieldType, lookupKeyIndexes[i]));
                conditionTypeRoots.add(fieldType.getTypeRoot());
            }
        }

        return new LookupNormalizer(
                lookupType,
                fieldIndexes(expectedLookupKeys, schema),
                normalizedKeyGetters,
                conditionFieldGetters.toArray(new FieldGetter[0]),
                resultFieldGetters.toArray(new FieldGetter[0]),
                conditionTypeRoots.toArray(new LogicalTypeRoot[0]));
    }

    private static int findIndex(List<String> columnNames, String key) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equals(key)) {
                return i;
            }
        }
        throw new TableException(
                "The Fluss lookup function supports lookup tables where the lookup keys "
                        + "include all primary keys, all bucket keys, or match a secondary index."
                        + " Can't find expected key '"
                        + key
                        + "' in lookup keys "
                        + columnNames);
    }

    private static List<String> fieldNames(int[] fieldIndexes, RowType schema) {
        return Arrays.stream(fieldIndexes)
                .mapToObj(i -> schema.getFields().get(i).getName())
                .collect(Collectors.toList());
    }

    private static int[] fieldIndexes(List<String> fieldNames, RowType schema) {
        return fieldNames.stream().mapToInt(schema::getFieldIndex).toArray();
    }
}
