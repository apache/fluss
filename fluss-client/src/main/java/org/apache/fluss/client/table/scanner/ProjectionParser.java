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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeParser;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.VariantType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parses projection column names that may include Variant sub-field syntax.
 *
 * <p>The syntax for Variant sub-field projection is {@code "column_name:sub_field"}, where {@code
 * column_name} is a Variant-typed column and {@code sub_field} is the name of a top-level sub-field
 * to project.
 *
 * <p>An optional cast suffix {@code "::TYPE"} can be appended to a sub-field expression to coerce
 * the projected sub-field into a typed scalar column at the client side. The expression must take
 * the form {@code "column:subfield::TYPE"}; using {@code "::TYPE"} on a regular column is not
 * supported. The {@code TYPE} part is parsed via {@link DataTypeParser#parse(String)} and is
 * restricted to scalar types (i.e. not ROW / ARRAY / MAP / VARIANT).
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code "id"} - project column "id" (regular column)
 *   <li>{@code "data:name"} - project sub-field "name" from Variant column "data" (untyped, raw
 *       Variant value)
 *   <li>{@code "data:age::BIGINT"} - project sub-field "age" from Variant column "data" and expose
 *       it as a typed BIGINT column at the client side
 * </ul>
 *
 * <p>Only top-level Variant fields are accepted in the first implementation. Nested paths such as
 * {@code data:address.city} and array paths such as {@code data:items[0]} are intentionally
 * rejected until the write path, RPC representation, and Arrow IPC pruning path all become
 * path-aware.
 *
 * <p>When at least one expression in the input list uses sub-field projection (with or without a
 * cast suffix), the result will contain a non-null {@link ParsedProjection#getProjectedFields()},
 * which describes the flattened logical row schema seen by the user (in user-supplied order). The
 * {@link ParsedProjection#getProjectedColumns()} array still contains the physical source column
 * indexes (deduplicated).
 *
 * @since 0.7
 */
@Internal
public final class ProjectionParser {

    /** The separator between column name and sub-field path. */
    private static final String SUB_FIELD_SEPARATOR = ":";

    /** The separator between the sub-field path and the cast type. */
    private static final String CAST_SEPARATOR = "::";

    private ProjectionParser() {}

    /**
     * Parses a list of projection column names (with optional Variant sub-field syntax) into
     * physical column indexes, RPC sub-field hints, and the flattened logical projection schema.
     *
     * @param projectedColumnNames the projection expressions
     * @param rowType the table's row type for column name lookup and type validation
     * @return the parsed result
     */
    public static ParsedProjection parse(List<String> projectedColumnNames, RowType rowType) {
        // Use LinkedHashMap to maintain insertion order and deduplicate physical columns.
        // Value list collects sub-field names (untyped + typed) for RPC hint aggregation;
        // a null value means "whole column requested" (hint should be cleared).
        LinkedHashMap<Integer, List<String>> columnToVariantFields = new LinkedHashMap<>();
        // Track all physical column indexes in order (deduped via the map above).
        List<Integer> columnIndexList = new ArrayList<>();
        // Logical projected fields, preserved in user-supplied order.
        List<ProjectedField> projectedFields = new ArrayList<>();
        // Whether any expression actually uses sub-field projection.
        boolean hasSubFieldExpr = false;

        for (String expr : projectedColumnNames) {
            // 1) Detect optional "::TYPE" cast suffix.
            String exprBody = expr;
            DataType castType = null;
            int castIdx = expr.indexOf(CAST_SEPARATOR);
            if (castIdx >= 0) {
                exprBody = expr.substring(0, castIdx);
                String typeStr = expr.substring(castIdx + CAST_SEPARATOR.length()).trim();
                if (typeStr.isEmpty()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Invalid projection expression '%s': cast type after '::' cannot be empty.",
                                    expr));
                }
                try {
                    castType = DataTypeParser.parse(typeStr);
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Invalid projection expression '%s': failed to parse cast type '%s': %s",
                                    expr, typeStr, e.getMessage()),
                            e);
                }
                if (!isScalarCastType(castType)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Invalid projection expression '%s': cast type '%s' is not a supported scalar type. "
                                            + "Cast targets must not be ROW / ARRAY / MAP / VARIANT.",
                                    expr, castType.asSummaryString()));
                }
            }

            // 2) Detect optional ":subField" sub-field path.
            int colonIdx = exprBody.indexOf(SUB_FIELD_SEPARATOR);
            if (colonIdx < 0) {
                // Regular column projection: "column_name" (cast suffix is not allowed here).
                if (castType != null) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Invalid projection expression '%s': cast suffix '::TYPE' is only supported on Variant sub-field expressions of the form 'column:subfield::TYPE'.",
                                    expr));
                }
                int index = resolveColumnIndex(exprBody, rowType);
                if (!columnToVariantFields.containsKey(index)) {
                    columnIndexList.add(index);
                    columnToVariantFields.put(index, null);
                } else {
                    // Whole-column request supersedes any prior sub-field hint.
                    columnToVariantFields.put(index, null);
                }
                int projectedPos = columnIndexList.indexOf(index);
                projectedFields.add(
                        new ProjectedField(
                                projectedPos, null, null, rowType.getFieldNames().get(index)));
            } else {
                // Variant sub-field projection: "column_name:sub_field[::TYPE]".
                hasSubFieldExpr = true;
                String columnName = exprBody.substring(0, colonIdx);
                String subField = exprBody.substring(colonIdx + 1);

                if (columnName.isEmpty()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Invalid projection expression '%s': column name before ':' cannot be empty.",
                                    expr));
                }
                if (subField.isEmpty()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Invalid projection expression '%s': sub-field name after ':' cannot be empty.",
                                    expr));
                }
                validateTopLevelSubField(expr, subField);

                int index = resolveColumnIndex(columnName, rowType);
                DataType dataType = rowType.getTypeAt(index);
                if (!(dataType instanceof VariantType)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Column '%s' is not a VARIANT type (actual: %s), "
                                            + "sub-field projection syntax 'column:subfield' "
                                            + "is only supported for Variant columns.",
                                    columnName, dataType.asSummaryString()));
                }

                if (!columnToVariantFields.containsKey(index)) {
                    columnIndexList.add(index);
                    columnToVariantFields.put(index, new ArrayList<>());
                }
                List<String> subFields = columnToVariantFields.get(index);
                if (subFields != null) {
                    if (!subFields.contains(subField)) {
                        subFields.add(subField);
                    }
                }
                // else: the column was already requested as a whole; keep hint cleared.

                int projectedPos = columnIndexList.indexOf(index);
                projectedFields.add(
                        new ProjectedField(projectedPos, subField, castType, null /* defer */));
            }
        }

        // Build the physical column array.
        int[] projectedColumns = new int[columnIndexList.size()];
        for (int i = 0; i < columnIndexList.size(); i++) {
            projectedColumns[i] = columnIndexList.get(i);
        }

        // Build RPC variant field projection hint map (only entries with non-null sub-fields).
        // The wire protocol uses the top-level table column index. The client-side flat row uses
        // projected physical positions and is resolved below.
        Map<Integer, List<String>> variantFieldProjection = null;
        for (Map.Entry<Integer, List<String>> entry : columnToVariantFields.entrySet()) {
            if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                if (variantFieldProjection == null) {
                    variantFieldProjection = new HashMap<>();
                }
                variantFieldProjection.put(entry.getKey(), entry.getValue());
            }
        }

        // Resolve display names for the flattened schema (handle collisions).
        List<ProjectedField> resolvedFields;
        if (!hasSubFieldExpr) {
            // Pure regular projection: keep projectedFields null for backward compatibility.
            resolvedFields = null;
        } else {
            resolvedFields = resolveDisplayNames(projectedFields, rowType, columnIndexList);
            resolvedFields = resolvePhysicalProjectedPositions(resolvedFields, columnIndexList);
        }

        return new ParsedProjection(projectedColumns, variantFieldProjection, resolvedFields);
    }

    private static void validateTopLevelSubField(String expr, String subField) {
        // TODO: FIP-36 follow-up: introduce a real Variant path grammar with escaping for field
        // names that contain '.', '[' or ']', then thread that path object through inference,
        // wire-format projection, server-side Arrow child pruning, and client-side residual decode.
        if (subField.indexOf('.') >= 0
                || subField.indexOf('[') >= 0
                || subField.indexOf(']') >= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid projection expression '%s': only top-level Variant fields are supported in this version. "
                                    + "Nested object fields and array elements are not supported yet.",
                            expr));
        }
    }

    private static boolean isScalarCastType(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        return root != DataTypeRoot.ROW
                && root != DataTypeRoot.ARRAY
                && root != DataTypeRoot.MAP
                && root != DataTypeRoot.VARIANT;
    }

    private static List<ProjectedField> resolveDisplayNames(
            List<ProjectedField> fields, RowType rowType, List<Integer> columnIndexList) {
        List<ProjectedField> resolved = new ArrayList<>(fields.size());
        Set<String> usedNames = new HashSet<>();
        for (ProjectedField f : fields) {
            String name = f.displayName;
            if (name == null) {
                int sourceIdx = columnIndexList.get(f.sourceProjectedPosition);
                String columnName = rowType.getFieldNames().get(sourceIdx);
                String base = columnName + "_" + f.subFieldName.replace('.', '_');
                name = base;
                if (usedNames.contains(name) && f.castType != null) {
                    name = base + "_" + f.castType.getTypeRoot().name().toLowerCase();
                }
                int suffix = 2;
                while (usedNames.contains(name)) {
                    name = base + "_" + suffix++;
                }
            } else if (usedNames.contains(name)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Duplicate projected column name '%s' detected in projection list.",
                                name));
            }
            usedNames.add(name);
            resolved.add(
                    new ProjectedField(
                            f.sourceProjectedPosition, f.subFieldName, f.castType, name));
        }
        return Collections.unmodifiableList(resolved);
    }

    private static List<ProjectedField> resolvePhysicalProjectedPositions(
            List<ProjectedField> fields, List<Integer> columnIndexList) {
        List<Integer> physicalColumnIndexes = new ArrayList<>(columnIndexList);
        Collections.sort(physicalColumnIndexes);

        List<ProjectedField> resolved = new ArrayList<>(fields.size());
        for (ProjectedField field : fields) {
            int sourceColumnIndex = columnIndexList.get(field.sourceProjectedPosition);
            int physicalProjectedPosition = physicalColumnIndexes.indexOf(sourceColumnIndex);
            resolved.add(
                    new ProjectedField(
                            physicalProjectedPosition,
                            field.subFieldName,
                            field.castType,
                            field.displayName));
        }
        return Collections.unmodifiableList(resolved);
    }

    private static int resolveColumnIndex(String columnName, RowType rowType) {
        int index = rowType.getFieldIndex(columnName);
        if (index < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field '%s' not found in table schema. Available fields: %s",
                            columnName, rowType.getFieldNames()));
        }
        return index;
    }

    /**
     * Describes one logical column in the flattened projection schema.
     *
     * <p>For a regular column projection, {@link #subFieldName} and {@link #castType} are both
     * {@code null}. For an untyped Variant sub-field projection ({@code "col:field"}), {@link
     * #subFieldName} is non-null and {@link #castType} is null. For a typed Variant sub-field
     * projection ({@code "col:field::TYPE"}), both are non-null.
     */
    public static final class ProjectedField {
        private final int sourceProjectedPosition;
        @Nullable private final String subFieldName;
        @Nullable private final DataType castType;
        private final String displayName;

        ProjectedField(
                int sourceProjectedPosition,
                @Nullable String subFieldName,
                @Nullable DataType castType,
                @Nullable String displayName) {
            this.sourceProjectedPosition = sourceProjectedPosition;
            this.subFieldName = subFieldName;
            this.castType = castType;
            this.displayName = displayName;
        }

        /**
         * Position in the physical projected row returned by the server. This is the position after
         * top-level columns are sorted for projection pushdown, not necessarily the user-supplied
         * expression order.
         */
        public int getSourceProjectedPosition() {
            return sourceProjectedPosition;
        }

        /** Sub-field path within a Variant column, or {@code null} for a regular column. */
        @Nullable
        public String getSubFieldName() {
            return subFieldName;
        }

        /**
         * Target scalar type when the user wrote {@code "col:field::TYPE"}. {@code null} means the
         * field should be exposed as raw Variant value.
         */
        @Nullable
        public DataType getCastType() {
            return castType;
        }

        /** Display name (column name) for this logical column in the projected row. */
        public String getDisplayName() {
            return displayName;
        }

        /** Whether this field requires a typed cast at the client side. */
        public boolean isCastField() {
            return castType != null;
        }

        /** Whether this field is a Variant sub-field (typed or untyped). */
        public boolean isSubField() {
            return subFieldName != null;
        }
    }

    /** Result of parsing projection expressions. */
    public static final class ParsedProjection {
        private final int[] projectedColumns;
        @Nullable private final Map<Integer, List<String>> variantFieldProjection;
        @Nullable private final List<ProjectedField> projectedFields;

        ParsedProjection(
                int[] projectedColumns,
                @Nullable Map<Integer, List<String>> variantFieldProjection,
                @Nullable List<ProjectedField> projectedFields) {
            this.projectedColumns = projectedColumns;
            this.variantFieldProjection = variantFieldProjection;
            this.projectedFields = projectedFields;
        }

        /** Returns the physical projected column indexes (deduplicated, in projection order). */
        public int[] getProjectedColumns() {
            return projectedColumns;
        }

        /**
         * Returns the variant sub-field projection hints, or null if no sub-field projection was
         * specified. The map key is the top-level table column index, and the value is the list of
         * top-level sub-field names.
         */
        @Nullable
        public Map<Integer, List<String>> getVariantFieldProjection() {
            return variantFieldProjection;
        }

        /**
         * Returns the flattened logical projection schema in user-supplied order, or {@code null}
         * if the projection contains no sub-field expression. When non-null, this list defines the
         * column layout of the rows returned by the scanner.
         */
        @Nullable
        public List<ProjectedField> getProjectedFields() {
            return projectedFields;
        }
    }
}
