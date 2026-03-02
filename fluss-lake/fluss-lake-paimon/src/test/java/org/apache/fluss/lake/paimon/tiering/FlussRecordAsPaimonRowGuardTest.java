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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that {@link FlussRecordAsPaimonRow} guards against misuse: calling accessor methods before
 * {@link FlussRecordAsPaimonRow#setFlussRecord} has been invoked, and passing {@code null} to
 * {@link FlussRecordAsPaimonRow#setFlussRecord}.
 *
 * <p>These tests lock in the exception type and message so that any accidental regression (e.g.
 * reverting to an opaque NPE) is caught immediately.
 */
class FlussRecordAsPaimonRowGuardTest {

    private static final String UNINITIALIZED_MSG =
            "setFlussRecord() must be called before accessing the row.";

    /** Minimal row type: one business INT column + three system columns. */
    private static final RowType ROW_TYPE =
            RowType.of(
                    new org.apache.paimon.types.IntType(),
                    // system columns: __bucket, __offset, __timestamp
                    new org.apache.paimon.types.IntType(),
                    new org.apache.paimon.types.BigIntType(),
                    new org.apache.paimon.types.LocalZonedTimestampType(3));

    private FlussRecordAsPaimonRow row;

    @BeforeEach
    void setUp() {
        row = new FlussRecordAsPaimonRow(0, ROW_TYPE);
    }

    // -------------------------------------------------------------------------
    // setFlussRecord(null)
    // -------------------------------------------------------------------------

    @Test
    void testSetFlussRecordNullThrowsNPE() {
        assertThatThrownBy(() -> row.setFlussRecord(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("logRecord must not be null");
    }

    // -------------------------------------------------------------------------
    // Uninitialized access — setFlussRecord() never called
    // -------------------------------------------------------------------------

    @Test
    void testGetRowKindBeforeSetThrowsIllegalState() {
        assertThatThrownBy(() -> row.getRowKind())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(UNINITIALIZED_MSG);
    }

    @Test
    void testIsNullAtBeforeSetThrowsIllegalState() {
        assertThatThrownBy(() -> row.isNullAt(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(UNINITIALIZED_MSG);
    }

    @Test
    void testGetLongBeforeSetThrowsIllegalState() {
        // pos 0 is a business column — routes through the logRecord null-check
        assertThatThrownBy(() -> row.getLong(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(UNINITIALIZED_MSG);
    }

    @Test
    void testGetLongOffsetColumnBeforeSetThrowsIllegalState() {
        // pos == offsetFieldIndex (businessFieldCount + 1 == 2) also accesses logRecord
        assertThatThrownBy(() -> row.getLong(2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(UNINITIALIZED_MSG);
    }

    @Test
    void testGetLongTimestampColumnBeforeSetThrowsIllegalState() {
        // pos == timestampFieldIndex (businessFieldCount + 2 == 3) also accesses logRecord
        assertThatThrownBy(() -> row.getLong(3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(UNINITIALIZED_MSG);
    }

    @Test
    void testGetTimestampBeforeSetThrowsIllegalState() {
        assertThatThrownBy(() -> row.getTimestamp(0, 6))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(UNINITIALIZED_MSG);
    }

    @Test
    void testGetTimestampSystemColumnBeforeSetThrowsIllegalState() {
        // pos == timestampFieldIndex (3) is the __timestamp system column
        assertThatThrownBy(() -> row.getTimestamp(3, 3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(UNINITIALIZED_MSG);
    }

    @Test
    void testGetIntBusinessColumnBeforeSetThrowsIllegalState() {
        // pos 0 is a business column (not bucketFieldIndex == 1), so the null-check fires
        assertThatThrownBy(() -> row.getInt(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(UNINITIALIZED_MSG);
    }

    @Test
    void testGetIntBucketColumnBeforeSetReturnsValue() {
        // bucketFieldIndex (businessFieldCount == 1) does NOT depend on logRecord; it returns the
        // bucket value passed to the constructor, so it must never throw even before setFlussRecord
        assertThatThrownBy(() -> row.getInt(0))
                .isInstanceOf(IllegalStateException.class);
        // bucket column itself is safe
        org.assertj.core.api.Assertions.assertThat(row.getInt(1)).isEqualTo(0);
    }
}