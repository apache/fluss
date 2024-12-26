/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.source.lookup;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkState;
import static org.apache.flink.util.Preconditions.checkArgument;

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

    public static final LookupNormalizer NOOP_NORMALIZER = new LookupNormalizer(null, null, null);

    /** Mapping from normalized key index to the lookup key index (in the lookup row). */
    @Nullable private final FieldGetter[] normalizedKeyGetters;

    /** The field getter to get remaining condition value from the lookup key row. */
    @Nullable private final FieldGetter[] conditionFieldGetters;

    /** The field getter to get the remaining condition result from the lookup result row. */
    @Nullable private final FieldGetter[] resultFieldGetters;

    private LookupNormalizer(
            @Nullable FieldGetter[] normalizedKeyGetters,
            @Nullable FieldGetter[] conditionFieldGetters,
            @Nullable FieldGetter[] resultFieldGetters) {
        this.normalizedKeyGetters = normalizedKeyGetters;
        this.conditionFieldGetters = conditionFieldGetters;
        this.resultFieldGetters = resultFieldGetters;
        if (conditionFieldGetters != null) {
            checkState(resultFieldGetters != null, "The resultFieldGetters should not be null.");
            checkState(
                    conditionFieldGetters.length == resultFieldGetters.length,
                    "The length of conditionFieldGetters and resultFieldGetters should be equal.");
        }
    }

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
        if (conditionFieldGetters == null || resultFieldGetters == null) {
            return null;
        }

        FieldCondition[] fieldConditions = new FieldCondition[conditionFieldGetters.length];
        for (int i = 0; i < conditionFieldGetters.length; i++) {
            fieldConditions[i] =
                    new FieldCondition(
                            conditionFieldGetters[i].getFieldOrNull(lookupKey),
                            resultFieldGetters[i]);
        }
        return new RemainingFilter(fieldConditions);
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

        private FieldCondition(Object expectedValue, FieldGetter resultFieldGetter) {
            this.expectedValue = expectedValue;
            this.resultFieldGetter = resultFieldGetter;
        }

        public boolean fieldMatches(RowData result) {
            Object fieldValue = resultFieldGetter.getFieldOrNull(result);
            return Objects.equals(expectedValue, fieldValue);
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Validate the lookup key indexes and primary keys, and create a {@link LookupNormalizer}. */
    public static LookupNormalizer validateAndCreateLookupNormalizer(
            int[][] lookupKeyIndexes, int[] primaryKeys, RowType schema) {
        if (primaryKeys.length == 0) {
            throw new UnsupportedOperationException(
                    "Fluss lookup function only support lookup table with primary key.");
        }
        // we compare string names rather than int index for better error message and readability,
        // the length of lookup key and primary key shouldn't be large, so the overhead is low.
        String[] columnNames = schema.getFieldNames().toArray(new String[0]);
        String[] primaryKeyNames =
                Arrays.stream(primaryKeys).mapToObj(i -> columnNames[i]).toArray(String[]::new);

        // get the lookup keys
        int[] lookupKeys = new int[lookupKeyIndexes.length];
        String[] lookupKeyNames = new String[lookupKeyIndexes.length];
        for (int i = 0; i < lookupKeyNames.length; i++) {
            int[] innerKeyArr = lookupKeyIndexes[i];
            checkArgument(innerKeyArr.length == 1, "Do not support nested lookup keys");
            // lookupKeyIndexes passed by Flink is key indexed after projection pushdown,
            // we do remaining condition filter on the projected row, so no remapping needed.
            lookupKeys[i] = innerKeyArr[0];
            lookupKeyNames[i] = columnNames[innerKeyArr[0]];
        }

        if (Arrays.equals(lookupKeys, primaryKeys)) {
            return NOOP_NORMALIZER;
        }

        FieldGetter[] normalizedKeyGetters = new FieldGetter[primaryKeys.length];
        for (int i = 0; i < primaryKeyNames.length; i++) {
            LogicalType fieldType = schema.getTypeAt(primaryKeys[i]);
            int lookupKeyIndex = findIndex(lookupKeyNames, primaryKeyNames[i]);
            normalizedKeyGetters[i] = RowData.createFieldGetter(fieldType, lookupKeyIndex);
        }

        Set<Integer> primaryKeySet = Arrays.stream(primaryKeys).boxed().collect(Collectors.toSet());
        List<FieldGetter> conditionFieldGetters = new ArrayList<>();
        List<FieldGetter> resultFieldGetters = new ArrayList<>();
        for (int i = 0; i < lookupKeys.length; i++) {
            if (!primaryKeySet.contains(i)) {
                LogicalType fieldType = schema.getTypeAt(lookupKeys[i]);
                conditionFieldGetters.add(RowData.createFieldGetter(fieldType, i));
                resultFieldGetters.add(RowData.createFieldGetter(fieldType, lookupKeys[i]));
            }
        }

        return new LookupNormalizer(
                normalizedKeyGetters,
                conditionFieldGetters.toArray(new FieldGetter[0]),
                resultFieldGetters.toArray(new FieldGetter[0]));
    }

    private static int findIndex(String[] columnNames, String key) {
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equals(key)) {
                return i;
            }
        }
        throw new TableException(
                "Fluss lookup function only supports lookup table with lookup keys contain all primary keys."
                        + " Can't find primary key '"
                        + key
                        + "' in lookup keys "
                        + Arrays.toString(columnNames));
    }
}
