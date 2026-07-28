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
    void testForAppendWithExplicitSchemaId() {
        InternalRow row = newRow();
        MultiTableWriteRecord rec = MultiTableWriteRecord.forAppend(TP, row, 5);
        assertThat(rec.getTablePath()).isEqualTo(TP);
        assertThat(rec.getOperation()).isEqualTo(MultiTableWriteRecord.Operation.APPEND);
        assertThat(rec.getRow()).isSameAs(row);
        assertThat(rec.getSchemaId()).isEqualTo(5);
    }

    @Test
    void testForUpsertWithExplicitSchemaId() {
        InternalRow row = newRow();
        MultiTableWriteRecord rec = MultiTableWriteRecord.forUpsert(TP, row, 7);
        assertThat(rec.getOperation()).isEqualTo(MultiTableWriteRecord.Operation.UPSERT);
        assertThat(rec.getSchemaId()).isEqualTo(7);
    }

    @Test
    void testForDeleteWithExplicitSchemaId() {
        InternalRow row = newRow();
        MultiTableWriteRecord rec = MultiTableWriteRecord.forDelete(TP, row, 9);
        assertThat(rec.getOperation()).isEqualTo(MultiTableWriteRecord.Operation.DELETE);
        assertThat(rec.getSchemaId()).isEqualTo(9);
    }

    @Test
    void testToStringContainsCoreFields() {
        MultiTableWriteRecord rec = MultiTableWriteRecord.forDelete(TP, newRow(), 3);
        assertThat(rec.toString())
                .contains("tablePath=" + TP)
                .contains("operation=DELETE")
                .contains("schemaId=3");
    }

    @Test
    void testRejectsNullTablePath() {
        assertThatThrownBy(() -> MultiTableWriteRecord.forAppend(null, newRow(), 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("tablePath");
    }

    @Test
    void testRejectsNullRow() {
        assertThatThrownBy(() -> MultiTableWriteRecord.forAppend(TP, null, 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("row");
    }
}
