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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link MultiTableWriteRecord}. */
class MultiTableWriteRecordTest {

    private static final TablePath TP = TablePath.of("db", "t");

    private static InternalRow newRow() {
        GenericRow r = new GenericRow(1);
        r.setField(0, 42);
        return r;
    }

    @Test
    void testOfWithExplicitSchemaId() {
        InternalRow row = newRow();
        MultiTableWriteRecord rec = MultiTableWriteRecord.of(TP, ChangeType.INSERT, row, 5);
        assertThat(rec.getTablePath()).isEqualTo(TP);
        assertThat(rec.getChangeType()).isEqualTo(ChangeType.INSERT);
        assertThat(rec.getRow()).isSameAs(row);
        assertThat(rec.getSchemaId()).isEqualTo(5);
    }

    @Test
    void testOfWithDifferentChangeType() {
        InternalRow row = newRow();
        MultiTableWriteRecord rec = MultiTableWriteRecord.of(TP, ChangeType.UPDATE_AFTER, row, 7);
        assertThat(rec.getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
        assertThat(rec.getSchemaId()).isEqualTo(7);
    }

    @Test
    void testToStringContainsCoreFields() {
        MultiTableWriteRecord rec = MultiTableWriteRecord.of(TP, ChangeType.DELETE, newRow(), 3);
        assertThat(rec.toString())
                .contains("tablePath=" + TP)
                .contains("changeType=DELETE")
                .contains("schemaId=3");
    }

    @Test
    void testRejectsNullTablePath() {
        assertThatThrownBy(() -> MultiTableWriteRecord.of(null, ChangeType.INSERT, newRow(), 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("tablePath");
    }

    @Test
    void testRejectsNullChangeType() {
        assertThatThrownBy(() -> MultiTableWriteRecord.of(TP, null, newRow(), 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("changeType");
    }

    @Test
    void testRejectsNullRow() {
        assertThatThrownBy(() -> MultiTableWriteRecord.of(TP, ChangeType.INSERT, null, 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("row");
    }
}
