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

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/** UT for {@link IcebergUtils#isLegacyTable(Schema)}. */
class IcebergUtilsTest {

    private static Types.NestedField bucketField(int id) {
        return required(id, BUCKET_COLUMN_NAME, Types.IntegerType.get());
    }

    private static Types.NestedField offsetField(int id) {
        return required(id, OFFSET_COLUMN_NAME, Types.LongType.get());
    }

    private static Types.NestedField timestampField(int id) {
        return required(id, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone());
    }

    @Test
    void testCleanTableHasNoSystemColumns() {
        Schema clean =
                new Schema(
                        required(1, "id", Types.LongType.get()),
                        optional(2, "name", Types.StringType.get()));
        assertThat(IcebergUtils.isLegacyTable(clean)).isFalse();
    }

    @Test
    void testLegacyTableWithAllThreeTrailingSystemColumns() {
        Schema legacy =
                new Schema(
                        required(1, "id", Types.LongType.get()),
                        optional(2, "name", Types.StringType.get()),
                        bucketField(3),
                        offsetField(4),
                        timestampField(5));
        assertThat(IcebergUtils.isLegacyTable(legacy)).isTrue();
    }

    @Test
    void testUserTableWithOnlyTimestampNameIsClean() {
        // A pre-existing user table that merely reuses the __timestamp name (but is not a full
        // Fluss
        // legacy layout) must be treated as clean, not misdetected as legacy.
        Schema oneMatch =
                new Schema(
                        required(1, "id", Types.LongType.get()),
                        optional(2, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone()));
        assertThat(IcebergUtils.isLegacyTable(oneMatch)).isFalse();
    }

    @Test
    void testUserTableWithTwoSystemColumnNamesIsClean() {
        Schema twoMatch =
                new Schema(
                        required(1, "id", Types.LongType.get()), offsetField(2), timestampField(3));
        assertThat(IcebergUtils.isLegacyTable(twoMatch)).isFalse();
    }
}
