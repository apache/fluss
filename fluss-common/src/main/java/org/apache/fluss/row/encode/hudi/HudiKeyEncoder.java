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

package org.apache.fluss.row.encode.hudi;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * An implementation of {@link KeyEncoder} that follows Hudi's bucket-key encoding strategy.
 *
 * <h3>What this encoder must reproduce</h3>
 *
 * The bucket id Hudi computes in production is:
 *
 * <pre>{@code
 * BucketIdentifier.getBucketId(HoodieKey, indexKeyFields, numBuckets)
 *   = (KeyGenUtils.extractRecordKeysByFields(recordKey, fields).hashCode()
 *      & Integer.MAX_VALUE) % numBuckets
 * }</pre>
 *
 * In other words, Hudi parses a {@code "f1:v1,f2:v2,..."} record-key string back into a {@code
 * List<String>} (with {@code "__null__"} → {@code null} and {@code "__empty__"} → {@code ""}) and
 * then takes {@link java.util.List#hashCode()}.
 *
 * <p>This encoder produces a list <em>element-wise equivalent</em> to the one Hudi reconstructs,
 * and hashes it with the same {@code List#hashCode()} contract. The encoded bytes are a 4-byte
 * big-endian representation of that hash, decoded symmetrically by {@code HudiBucketingFunction}.
 *
 * <h3>Why a list of nullable {@code String}s (and not stringified placeholders)</h3>
 *
 * Hudi's parser turns the literal {@code "__null__"}/{@code "__empty__"} substrings back into
 * {@code null}/{@code ""} <em>before</em> hashing. {@code List#hashCode} treats a {@code null}
 * element as contributing {@code 0} and a non-null element as contributing its {@code String}
 * hash. Hashing the placeholder string directly would therefore give a different bucket id than
 * Hudi for any row with a null bucket-key field.
 *
 * <h3>Why we reject values containing {@code ','}</h3>
 *
 * {@code ','} is Hudi's record-part separator and is <strong>not escaped</strong> in Hudi's
 * record-key serialization. Any bucket-key value containing {@code ','} is therefore ambiguous on
 * Hudi's parse path and would produce a different reconstructed list than the one we hash here.
 * We refuse such values up front with {@link IllegalArgumentException} so callers cannot silently
 * desync from Hudi's bucket layout. ({@code ':'} on the other hand is safe — Hudi's parser has a
 * dedicated look-ahead loop that handles values containing {@code ':'}, e.g. timestamps like
 * {@code "2023-10-25T10:01:13.182Z"}.)
 *
 * <h3>Supported key field types</h3>
 *
 * See {@link #SUPPORTED_BUCKET_KEY_TYPE_ROOTS}. Composite or binary types (ARRAY/MAP/ROW/
 * BINARY/VARBINARY) would fall back to {@code Object#toString()} and produce instance-bound,
 * non-reproducible bucket ids; they are rejected at construction time.
 */
public class HudiKeyEncoder implements KeyEncoder {

    /**
     * Placeholder used by Hudi's serialization for a {@code null} bucket-key field. Kept here for
     * cross-validation in tests — the encoder itself never hashes this literal because Hudi's
     * parser turns it back into {@code null} before hashing.
     */
    public static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";

    /** Hudi's record-part separator. Values containing it are rejected. */
    private static final char RECORD_KEY_PARTS_SEPARATOR = ',';

    /**
     * The set of {@link DataTypeRoot}s allowed as Hudi bucket key fields. These are exactly the
     * scalar types for which {@code toString()} (or a deterministic equivalent) round-trips
     * through Hudi's record-key serialization.
     */
    public static final Set<DataTypeRoot> SUPPORTED_BUCKET_KEY_TYPE_ROOTS =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            DataTypeRoot.CHAR,
                            DataTypeRoot.STRING,
                            DataTypeRoot.BOOLEAN,
                            DataTypeRoot.DECIMAL,
                            DataTypeRoot.TINYINT,
                            DataTypeRoot.SMALLINT,
                            DataTypeRoot.INTEGER,
                            DataTypeRoot.BIGINT,
                            DataTypeRoot.FLOAT,
                            DataTypeRoot.DOUBLE,
                            DataTypeRoot.DATE,
                            DataTypeRoot.TIME_WITHOUT_TIME_ZONE,
                            DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                            DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE));

    private final InternalRow.FieldGetter[] fieldGetters;
    private final String[] keyFieldNames;

    public HudiKeyEncoder(RowType rowType, List<String> keys) {
        // for getting key fields out of fluss internal row
        fieldGetters = new InternalRow.FieldGetter[keys.size()];
        keyFieldNames = new String[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            String keyField = keys.get(i);
            int keyIndex = rowType.getFieldIndex(keyField);
            if (keyIndex < 0) {
                throw new IllegalArgumentException(
                        "Bucket key field '" + keyField + "' does not exist in row type.");
            }
            DataType keyDataType = rowType.getTypeAt(keyIndex);
            validateSupportedBucketKeyType(keyField, keyDataType);
            fieldGetters[i] = InternalRow.createFieldGetter(keyDataType, keyIndex);
            keyFieldNames[i] = keyField;
        }
    }

    private static void validateSupportedBucketKeyType(String keyField, DataType dataType) {
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        if (!SUPPORTED_BUCKET_KEY_TYPE_ROOTS.contains(typeRoot)) {
            throw new IllegalArgumentException(
                    "Unsupported Hudi bucket key type for field '"
                            + keyField
                            + "': "
                            + dataType
                            + ". A bucket key field must be a scalar type with a deterministic "
                            + "string form; composite or binary types (ARRAY/MAP/ROW/BINARY/VARBINARY) "
                            + "would otherwise fall back to Object#toString() and produce "
                            + "non-reproducible bucket ids. Supported type roots are: "
                            + SUPPORTED_BUCKET_KEY_TYPE_ROOTS);
        }
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        // Build the same List<String> that Hudi's KeyGenUtils.extractRecordKeysByFields would
        // hand to List#hashCode() in BucketIdentifier#getBucketId.
        //
        // Hudi has two parse paths:
        //   * single-field record key (no ',' AND no ':' in the serialized form): the parser
        //     returns the raw recordKey verbatim — placeholders are NOT round-tripped, so
        //     "__null__"/"__empty__" stay as literal strings.
        //   * composite record key ("f1:v1,f2:v2,..."): the parser splits on ':'/',' and
        //     additionally turns "__null__" → null and "__empty__" → "" before hashing.
        //
        // We mirror the same two-mode behaviour so the resulting List#hashCode matches Hudi's.
        boolean composite = fieldGetters.length > 1;
        List<String> values = new ArrayList<>(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            Object value = fieldGetters[i].getFieldOrNull(row);
            values.add(toHudiHashElement(keyFieldNames[i], value, composite));
        }
        int hashCode = values.hashCode();

        // 4-byte big-endian, decoded symmetrically by HudiBucketingFunction.
        return new byte[] {
            (byte) (hashCode >>> 24),
            (byte) (hashCode >>> 16),
            (byte) (hashCode >>> 8),
            (byte) hashCode
        };
    }

    /**
     * Produces the same {@code String} element Hudi's parser would have placed in the list it
     * hashes. The behaviour is mode-dependent because Hudi itself has two parse modes:
     *
     * <ul>
     *   <li><b>single-field</b> ({@code composite == false}): Hudi returns the raw recordKey
     *       verbatim, so {@code "__null__"} stays as the literal string. We therefore emit the
     *       placeholder string for a null field, and reject {@code ','}-containing values only
     *       for safety (a single-field recordKey containing {@code ','} would actually trip the
     *       composite quick-path check on Hudi's side too).
     *   <li><b>composite</b> ({@code composite == true}): Hudi parses {@code "__null__"} →
     *       {@code null} and {@code "__empty__"} → {@code ""} before hashing. We emit {@code
     *       null}/{@code ""} directly. Values literally equal to a placeholder, or containing
     *       {@code ','}, are rejected because they would either collide with Hudi's reserved
     *       sentinels or break Hudi's reverse parsing.
     * </ul>
     */
    private static String toHudiHashElement(String fieldName, Object value, boolean composite) {
        if (value == null) {
            // Composite mode: Hudi parses "__null__" back to null before hashing.
            // Single-field mode: Hudi keeps the recordKey verbatim, so we keep the placeholder.
            return composite ? null : NULL_RECORDKEY_PLACEHOLDER;
        }
        String stringValue =
                (value instanceof BinaryString) ? value.toString() : String.valueOf(value);
        if (composite && stringValue.isEmpty()) {
            // Composite mode: Hudi parses "__empty__" back to "" before hashing.
            return "";
        }
        if (composite) {
            rejectIfContainsRecordSeparator(fieldName, stringValue);
            // Guard against literal placeholder collisions — if a user literally stores
            // "__null__"/"__empty__" as the value, Hudi would round-trip it to null/"" and
            // we'd silently disagree.
            if (NULL_RECORDKEY_PLACEHOLDER.equals(stringValue)
                    || "__empty__".equals(stringValue)) {
                throw new IllegalArgumentException(
                        "Bucket key field '"
                                + fieldName
                                + "' has value "
                                + stringValue
                                + " which collides with Hudi's reserved record-key placeholder; "
                                + "this value cannot be safely used as a composite Hudi bucket key.");
            }
        }
        return stringValue;
    }

    private static void rejectIfContainsRecordSeparator(String fieldName, String value) {
        if (value.indexOf(RECORD_KEY_PARTS_SEPARATOR) >= 0) {
            throw new IllegalArgumentException(
                    "Bucket key field '"
                            + fieldName
                            + "' has value containing ',', which is Hudi's record-part "
                            + "separator and is not escaped by Hudi's record-key serialization. "
                            + "Such a value would produce a bucket id divergent from Hudi's "
                            + "BucketIdentifier and is rejected to keep the bucket layout in sync.");
        }
    }
}