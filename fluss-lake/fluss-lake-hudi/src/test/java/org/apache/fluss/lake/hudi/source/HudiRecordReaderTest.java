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

package org.apache.fluss.lake.hudi.source;

import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HudiRecordReader}. */
class HudiRecordReaderTest {

    @Test
    void testIteratorConvertsHudiRowDataToFlussLogRecord() {
        TestingClosableIterator hudiIterator =
                new TestingClosableIterator(
                        rowData(RowKind.UPDATE_AFTER, 11, "value", 3, 42L, 1234L));
        HudiRecordReader.HudiRecordAsFlussRecordIterator iterator =
                new HudiRecordReader.HudiRecordAsFlussRecordIterator(
                        hudiIterator, fullSchema(), 2, 3);

        assertThat(iterator.hasNext()).isTrue();
        LogRecord logRecord = iterator.next();

        assertThat(logRecord.getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
        assertThat(logRecord.logOffset()).isEqualTo(42L);
        assertThat(logRecord.timestamp()).isEqualTo(1234L);
        assertThat(logRecord.getRow().getFieldCount()).isEqualTo(2);
        assertThat(logRecord.getRow().getInt(0)).isEqualTo(11);
        assertThat(logRecord.getRow().getString(1)).isEqualTo(BinaryString.fromString("value"));

        iterator.close();
        iterator.close();
        assertThat(iterator.hasNext()).isFalse();
        assertThat(hudiIterator.getCloseCount()).isEqualTo(1);
    }

    @Test
    void testIteratorSkipsMetadataAndAllSystemColumnsForProjectedSchema() {
        TestingClosableIterator hudiIterator =
                new TestingClosableIterator(
                        projectedRowData(RowKind.DELETE, "projected", 5, 7L, 999L));
        HudiRecordReader.HudiRecordAsFlussRecordIterator iterator =
                new HudiRecordReader.HudiRecordAsFlussRecordIterator(
                        hudiIterator, projectedSchema(), 2, 3);

        LogRecord logRecord = iterator.next();

        assertThat(logRecord.getChangeType()).isEqualTo(ChangeType.DELETE);
        assertThat(logRecord.logOffset()).isEqualTo(7L);
        assertThat(logRecord.timestamp()).isEqualTo(999L);
        assertThat(logRecord.getRow().getFieldCount()).isEqualTo(1);
        assertThat(logRecord.getRow().getString(0)).isEqualTo(BinaryString.fromString("projected"));
    }

    private static RowData rowData(
            RowKind rowKind, int id, String value, int bucket, long offset, long timestamp) {
        GenericRowData rowData = new GenericRowData(7);
        rowData.setRowKind(rowKind);
        rowData.setField(0, StringData.fromString("commit"));
        rowData.setField(1, StringData.fromString("record-key"));
        rowData.setField(2, id);
        rowData.setField(3, StringData.fromString(value));
        rowData.setField(4, bucket);
        rowData.setField(5, offset);
        rowData.setField(6, TimestampData.fromEpochMillis(timestamp));
        return rowData;
    }

    private static RowData projectedRowData(
            RowKind rowKind, String value, int bucket, long offset, long timestamp) {
        GenericRowData rowData = new GenericRowData(6);
        rowData.setRowKind(rowKind);
        rowData.setField(0, StringData.fromString("commit"));
        rowData.setField(1, StringData.fromString("record-key"));
        rowData.setField(2, StringData.fromString(value));
        rowData.setField(3, bucket);
        rowData.setField(4, offset);
        rowData.setField(5, TimestampData.fromEpochMillis(timestamp));
        return rowData;
    }

    private static Schema fullSchema() {
        return parseSchema(
                "["
                        + "{\"name\":\"_hoodie_commit_time\",\"type\":\"string\"},"
                        + "{\"name\":\"_hoodie_record_key\",\"type\":\"string\"},"
                        + "{\"name\":\"id\",\"type\":\"int\"},"
                        + "{\"name\":\"value\",\"type\":\"string\"},"
                        + "{\"name\":\"__bucket\",\"type\":\"int\"},"
                        + "{\"name\":\"__offset\",\"type\":\"long\"},"
                        + "{\"name\":\"__timestamp\",\"type\":\"long\"}"
                        + "]");
    }

    private static Schema projectedSchema() {
        return parseSchema(
                "["
                        + "{\"name\":\"_hoodie_commit_time\",\"type\":\"string\"},"
                        + "{\"name\":\"_hoodie_record_key\",\"type\":\"string\"},"
                        + "{\"name\":\"value\",\"type\":\"string\"},"
                        + "{\"name\":\"__bucket\",\"type\":\"int\"},"
                        + "{\"name\":\"__offset\",\"type\":\"long\"},"
                        + "{\"name\":\"__timestamp\",\"type\":\"long\"}"
                        + "]");
    }

    private static Schema parseSchema(String fields) {
        return new Schema.Parser()
                .parse(
                        "{"
                                + "\"type\":\"record\","
                                + "\"name\":\"hudi_record\","
                                + "\"fields\":"
                                + fields
                                + "}");
    }

    private static class TestingClosableIterator implements ClosableIterator<RowData> {
        private final Iterator<RowData> iterator;
        private int closeCount;

        private TestingClosableIterator(RowData rowData) {
            this.iterator = Collections.singletonList(rowData).iterator();
        }

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public RowData next() {
            if (!iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        private int getCloseCount() {
            return closeCount;
        }
    }
}
