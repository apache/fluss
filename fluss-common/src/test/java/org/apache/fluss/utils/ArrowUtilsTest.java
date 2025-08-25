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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.BigIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.BitVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.DateDayVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.Float4Vector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.Float8Vector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.SmallIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeSecVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TinyIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarBinaryVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarCharVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorUnloader;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.NoCompressionCodec.DEFAULT_BODY_COMPRESSION;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link com.alibaba.fluss.utils.ArrowUtils}. */
class ArrowUtilsTest {

    private static final List<DataType> TYPES =
            Arrays.asList(
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.BOOLEAN(),
                    DataTypes.STRING(),
                    DataTypes.BYTES(),
                    DataTypes.DATE(),
                    DataTypes.TIME(),
                    DataTypes.TIMESTAMP());

    @Test
    void testEstimateArrowMetadataSizeInBytes() throws IOException {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f1", DataTypes.INT()),
                        DataTypes.FIELD("f2", DataTypes.STRING()),
                        DataTypes.FIELD("f3", DataTypes.DOUBLE()));
        Schema schema = ArrowUtils.toArrowSchema(rowType);
        int metadataSize = ArrowUtils.estimateArrowMetadataLength(schema, DEFAULT_BODY_COMPRESSION);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                WriteChannel writeChannel = new WriteChannel(Channels.newChannel(out))) {
            root.allocateNew();
            IntVector f1Vector = (IntVector) root.getVector(0);
            VarCharVector f2Vector = (VarCharVector) root.getVector(1);
            Float8Vector f3Vector = (Float8Vector) root.getVector(2);
            for (int i = 0; i < 10; i++) {
                f1Vector.setSafe(i, i);
                f2Vector.setSafe(i, new Text("f2_" + i));
                f3Vector.setSafe(i, i + 0.1);
            }
            root.setRowCount(10);

            long bufferSize = ArrowUtils.estimateArrowBodyLength(root);

            VectorUnloader unloader = new VectorUnloader(root);
            try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
                ArrowBlock block = MessageSerializer.serialize(writeChannel, recordBatch);
                assertThat(metadataSize).isEqualTo(block.getMetadataLength());
                assertThat(bufferSize).isEqualTo(block.getBodyLength());
                assertThat(metadataSize + bufferSize).isEqualTo(out.toByteArray().length);
            }
        }
    }

    @Test
    void testRandomEstimateArrowMetadataSizeInBytes() throws IOException {
        int fieldCount = RandomUtils.nextInt(1, 100);
        List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            DataType type = TYPES.get(RandomUtils.nextInt(0, TYPES.size()));
            fields.add(new DataField("f" + i, type));
        }
        RowType rowType = new RowType(fields);
        Schema schema = ArrowUtils.toArrowSchema(rowType);
        int metadataSize = ArrowUtils.estimateArrowMetadataLength(schema, DEFAULT_BODY_COMPRESSION);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int rowCount = RandomUtils.nextInt(1, 1000);
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                WriteChannel writeChannel = new WriteChannel(Channels.newChannel(out))) {
            root.allocateNew();
            for (int i = 0; i < fieldCount; i++) {
                FieldVector vector = root.getVector(i);
                generateTestingData(vector, fields.get(i).getType(), rowCount);
            }
            root.setRowCount(rowCount);

            long bufferSize = ArrowUtils.estimateArrowBodyLength(root);

            String errorMessage = String.format("rowCount=%d, schema=%s", rowCount, rowType);
            VectorUnloader unloader = new VectorUnloader(root);
            try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
                ArrowBlock block = MessageSerializer.serialize(writeChannel, recordBatch);
                assertThat(metadataSize).as(errorMessage).isEqualTo(block.getMetadataLength());
                assertThat(bufferSize).as(errorMessage).isEqualTo(block.getBodyLength());
                assertThat(metadataSize + bufferSize)
                        .as(errorMessage)
                        .isEqualTo(out.toByteArray().length);
            }
        }
    }

    private static void generateTestingData(FieldVector vector, DataType type, int rowCount) {
        final boolean nullable = type.isNullable();
        switch (type.getTypeRoot()) {
            case TINYINT:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((TinyIntVector) vector).setSafe(i, RandomUtils.nextBytes(1)[0]);
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            case SMALLINT:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((SmallIntVector) vector).setSafe(i, (short) RandomUtils.nextInt());
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            case INTEGER:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((IntVector) vector).setSafe(i, RandomUtils.nextInt());
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            case BIGINT:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((BigIntVector) vector).setSafe(i, RandomUtils.nextLong());
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            case FLOAT:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((Float4Vector) vector).setSafe(i, RandomUtils.nextFloat());
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            case DOUBLE:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((Float8Vector) vector).setSafe(i, RandomUtils.nextDouble());
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            case BOOLEAN:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((BitVector) vector).setSafe(i, RandomUtils.nextInt(0, 2));
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            case STRING:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((VarCharVector) vector)
                                .setSafe(
                                        i,
                                        new Text(
                                                RandomStringUtils.randomAlphabetic(
                                                        RandomUtils.nextInt(0, 100))));
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            case BYTES:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((VarBinaryVector) vector)
                                .setSafe(
                                        i,
                                        RandomStringUtils.randomAlphabetic(
                                                        RandomUtils.nextInt(0, 100))
                                                .getBytes());
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            case DATE:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((DateDayVector) vector).setSafe(i, RandomUtils.nextInt(0, 10000));
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            case TIME_WITHOUT_TIME_ZONE:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((TimeSecVector) vector).setSafe(i, RandomUtils.nextInt(0, 10000));
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            case TIMESTAMP_WITHOUT_TIME_ZONE:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && RandomUtils.nextInt(0, 10) > 0) {
                        ((TimeStampVector) vector).setSafe(i, System.currentTimeMillis());
                    } else {
                        vector.setNull(i);
                    }
                }
                break;

            default:
                throw new UnsupportedOperationException();
        }
    }
}
