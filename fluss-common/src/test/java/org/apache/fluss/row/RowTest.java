/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowSerializer;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowSerializer;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.row.serializer.InternalRowSerializer;
import org.apache.fluss.row.serializer.Serializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.math.BigDecimal;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** RowTest. */
public class RowTest {

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    public void testGenericRowWithFormat(KvFormat format) {
        RowTestFactory factory = createFactory(format);

        GenericRow genericRow = GenericRow.of(fromString("Test"), 12.345, 123);
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.STRING(), "f0"),
                        DataTypes.FIELD("f1", DataTypes.DOUBLE(), "f1"),
                        DataTypes.FIELD("f2", DataTypes.INT(), "f2"));

        BinaryRow row = factory.createRow(rowType);
        Serializer<InternalRow> serializer = factory.createSerializer(rowType);

        try (AutoCloseableRowWriter writer = factory.createWriter(rowType)) {
            writer.writeRow(genericRow, (InternalRowSerializer) serializer);
            row.pointTo(writer.getSegment(), 0, writer.getPosition());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        InternalRow result = row.getRow(0, 3);
        assertThat(result.getString(0).toString()).isEqualTo("Test");
        assertThat(result.getDouble(1)).isEqualTo(12.345);
        assertThat(result.getInt(2)).isEqualTo(123);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    public void testNestedRowWithFormat(KvFormat format) throws Exception {
        RowTestFactory factory = createFactory(format);

        GenericRow innerRow =
                format == KvFormat.INDEXED
                        ? GenericRow.of(
                                42,
                                fromString("innerStr"),
                                999L,
                                Decimal.fromUnscaledLong(12345L, 10, 2))
                        : GenericRow.of(
                                50,
                                fromString("innerStr"),
                                Decimal.fromUnscaledLong(12345L, 10, 2));

        GenericRow outerRow = GenericRow.of(100, innerRow, fromString("outerStr"));

        DataType nestedDataType =
                format == KvFormat.INDEXED
                        ? DataTypes.ROW(
                                DataTypes.FIELD("ra", DataTypes.INT()),
                                DataTypes.FIELD("rb", DataTypes.STRING()),
                                DataTypes.FIELD("rc", DataTypes.BIGINT()),
                                DataTypes.FIELD("rd", DataTypes.DECIMAL(10, 2)))
                        : DataTypes.ROW(
                                DataTypes.FIELD("ra", DataTypes.INT()),
                                DataTypes.FIELD("rb", DataTypes.STRING()),
                                DataTypes.FIELD("rc", DataTypes.DECIMAL(10, 2)));

        RowType outerType =
                DataTypes.ROW(
                        DataTypes.FIELD("i", DataTypes.INT()),
                        DataTypes.FIELD("r", nestedDataType),
                        DataTypes.FIELD("s", DataTypes.STRING()));

        BinaryRow row = factory.createRow(outerType);
        Serializer<InternalRow> serializer = factory.createSerializer(outerType);

        try (AutoCloseableRowWriter writer = factory.createWriter(outerType)) {
            writer.writeRow(outerRow, (InternalRowSerializer) serializer);
            row.pointTo(writer.getSegment(), 0, writer.getPosition());
        }

        InternalRow result = row.getRow(0, 3);
        assertThat(result.getInt(0)).isEqualTo(100);
        assertThat(result.getString(2).toString()).isEqualTo("outerStr");

        InternalRow nestedResult = result.getRow(1, format == KvFormat.INDEXED ? 4 : 3);
        assertNestedRowValues(nestedResult, format);
    }

    private RowTestFactory createFactory(KvFormat format) {
        switch (format) {
            case COMPACTED:
                return new CompactedRowTestFactory();
            case INDEXED:
                return new IndexedRowTestFactory();
            default:
                throw new IllegalArgumentException();
        }
    }

    private void assertNestedRowValues(InternalRow row, KvFormat format) {
        assertThat(row.getInt(0)).isBetween(42, 50);
        assertThat(row.getString(1).toString()).isEqualTo("innerStr");

        if (format == KvFormat.INDEXED) {
            assertThat(row.getLong(2)).isEqualTo(999L);
            assertThat(row.getDecimal(3, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("123.45"));
        } else {
            assertThat(row.getDecimal(2, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("123.45"));
        }
    }

    interface RowTestFactory {
        BinaryRow createRow(RowType rowType);

        Serializer<InternalRow> createSerializer(RowType rowType);

        AutoCloseableRowWriter createWriter(RowType rowType) throws IOException;
    }

    interface AutoCloseableRowWriter extends AutoCloseable {
        void writeRow(GenericRow row, InternalRowSerializer serializer) throws IOException;

        MemorySegment getSegment();

        int getPosition();
    }

    static class CompactedRowTestFactory implements RowTestFactory {
        public BinaryRow createRow(RowType rowType) {
            return new CompactedRow(rowType);
        }

        public Serializer<InternalRow> createSerializer(RowType rowType) {
            return CompactedRowSerializer.create(rowType);
        }

        public AutoCloseableRowWriter createWriter(RowType rowType) throws IOException {
            final CompactedRowWriter writer = new CompactedRowWriter(rowType);
            return new AutoCloseableRowWriter() {
                public void writeRow(GenericRow row, InternalRowSerializer serializer)
                        throws IOException {
                    writer.writeRow(row, serializer);
                }

                public MemorySegment getSegment() {
                    return writer.segment();
                }

                public int getPosition() {
                    return writer.position();
                }

                public void close() throws IOException {
                    writer.close();
                }
            };
        }
    }

    static class IndexedRowTestFactory implements RowTestFactory {
        public BinaryRow createRow(RowType rowType) {
            return new IndexedRow(rowType);
        }

        public Serializer<InternalRow> createSerializer(RowType rowType) {
            return IndexedRowSerializer.create(rowType);
        }

        public AutoCloseableRowWriter createWriter(RowType rowType) throws IOException {
            final IndexedRowWriter writer = new IndexedRowWriter(rowType);
            return new AutoCloseableRowWriter() {
                public void writeRow(GenericRow row, InternalRowSerializer serializer) {
                    writer.writeRow(row, serializer);
                }

                public MemorySegment getSegment() {
                    return writer.segment();
                }

                public int getPosition() {
                    return writer.position();
                }

                public void close() throws IOException {
                    writer.close();
                }
            };
        }
    }
}
