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

package org.apache.fluss.flink.sink.writer;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SinkOperationMode;
import org.apache.fluss.flink.utils.TestUpsertWriter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;

import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link UpsertSinkWriter}. */
class UpsertSinkWriterTest {

    @Test
    void testRetractSentDirectlyWithoutBuffering() throws Exception {
        UpsertSinkWriter<RowData> writer = createTestingWriter(true);
        TestUpsertWriter mockWriter = getTestUpsertWriter(writer);

        writer.writeRow(OperationType.RETRACT, genericRow(1, 10L));

        assertThat(mockWriter.getRetractCount()).isEqualTo(1);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(0);
    }

    @Test
    void testRetractOnNonRetractSchemaThrows() throws Exception {
        UpsertSinkWriter<RowData> writer = createTestingWriter(false);

        assertThatThrownBy(() -> writer.writeRow(OperationType.RETRACT, genericRow(1, 10L)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support retract");
    }

    @Test
    void testMixedRetractAndUpsertInSequence() throws Exception {
        UpsertSinkWriter<RowData> writer = createTestingWriter(true);
        TestUpsertWriter mockWriter = getTestUpsertWriter(writer);

        writer.writeRow(OperationType.RETRACT, genericRow(1, 10L));
        writer.writeRow(OperationType.UPSERT, genericRow(1, 11L));

        assertThat(mockWriter.getRetractCount()).isEqualTo(1);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(1);
    }

    private static UpsertSinkWriter<RowData> createTestingWriter(boolean schemaSupportsRetract)
            throws Exception {
        UpsertSinkWriter<RowData> writer =
                new UpsertSinkWriter<>(
                        TablePath.of("test_db", "test_table"),
                        new Configuration(),
                        flinkRowTypeForSimplePk(),
                        null,
                        new MockWriterInitContext(new InterceptingOperatorMetricGroup())
                                .getMailboxExecutor(),
                        new RowDataSerializationSchema(SinkOperationMode.upsert(false)),
                        null,
                        schemaSupportsRetract);

        writer.upsertWriter = new TestUpsertWriter();
        return writer;
    }

    private static TestUpsertWriter getTestUpsertWriter(UpsertSinkWriter<RowData> writer) {
        return (TestUpsertWriter) writer.upsertWriter;
    }

    private static RowType flinkRowTypeForSimplePk() {
        return RowType.of(
                new org.apache.flink.table.types.logical.LogicalType[] {
                    new IntType(), new org.apache.flink.table.types.logical.BigIntType()
                },
                new String[] {"id", "value"});
    }

    private static GenericRow genericRow(Object... values) {
        GenericRow row = new GenericRow(values.length);
        for (int i = 0; i < values.length; i++) {
            row.setField(i, values[i]);
        }
        return row;
    }
}
