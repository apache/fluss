/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.source.deserializer;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.UserCodeClassLoader;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * Test class for the {@link RowDataDeserializationSchema} that validates the conversion from Fluss
 * {@link com.alibaba.fluss.record.LogRecord} to Flink's {@link RowData} format.
 */
public class RowDataDeserializationSchemaTest {

    private RowType rowType;
    private RowDataDeserializationSchema deserializer;

    @BeforeEach
    public void setUp() throws Exception {
        List<DataField> fields =
                Arrays.asList(
                        new DataField("orderId", DataTypes.BIGINT()),
                        new DataField("itemId", DataTypes.BIGINT()),
                        new DataField("amount", DataTypes.INT()),
                        new DataField("address", DataTypes.STRING()));

        rowType = new RowType(fields);
        deserializer = getRowDataDeserializationSchema(rowType);
    }

    @Test
    public void testDeserialize() throws Exception {
        // Create test data
        GenericRow row = new GenericRow(4);
        row.setField(0, 100L);
        row.setField(1, 10L);
        row.setField(2, 45);
        row.setField(3, BinaryString.fromString("Test addr"));

        ScanRecord scanRecord = new ScanRecord(row);

        RowDataDeserializationSchema deserializer = getRowDataDeserializationSchema(rowType);
        RowData result = deserializer.deserialize(scanRecord);

        assertThat(result.getArity()).isEqualTo(4);
        assertThat(result.getLong(0)).isEqualTo(100L);
        assertThat(result.getLong(1)).isEqualTo(10L);
        assertThat(result.getInt(2)).isEqualTo(45);
        assertThat(result.getString(3).toString()).isEqualTo("Test addr");
    }

    private @NotNull RowDataDeserializationSchema getRowDataDeserializationSchema(RowType rowType)
            throws Exception {
        RowDataDeserializationSchema deserializationSchema = new RowDataDeserializationSchema();
        deserializationSchema.open(
                new FlussDeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return null;
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return null;
                    }

                    @Override
                    public RowType getRowSchema() {
                        return rowType;
                    }
                });
        return deserializationSchema;
    }

    @Test
    public void testDeserializeWithNullRecord() {
        assertThatThrownBy(() -> deserializer.deserialize(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testGetProducedType() {
        TypeInformation<RowData> typeInfo = deserializer.getProducedType(rowType);

        assertThat(typeInfo).isNotNull();
        assertThat(typeInfo).isInstanceOf(InternalTypeInfo.class);
        assertThat(((InternalTypeInfo) typeInfo).toRowType().getFieldCount())
                .isEqualTo(rowType.getFieldCount());
        assertThat(typeInfo.getTypeClass()).isEqualTo(RowData.class);
        assertThat(typeInfo.createSerializer(new SerializerConfigImpl()))
                .isInstanceOf(RowDataSerializer.class);
    }

    @Test
    public void testSerializable() throws Exception {
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(deserializer);
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        RowDataDeserializationSchema deserializedSchema =
                (RowDataDeserializationSchema) ois.readObject();
        ois.close();

        // Verify
        assertThat(deserializedSchema).isNotNull();
        assertThat(deserializedSchema.getProducedType(rowType)).isNotNull();
        assertThat(deserializedSchema.getProducedType(rowType))
                .isEqualTo(deserializer.getProducedType(rowType));
        assertThat(
                        deserializedSchema
                                .getProducedType(rowType)
                                .createSerializer(new SerializerConfigImpl()))
                .isInstanceOf(RowDataSerializer.class);
    }

    @Test
    public void testDifferentRowTypes() throws Exception {
        // Test with different row types
        List<DataField> simpleFields =
                Collections.singletonList(new DataField("id", DataTypes.BIGINT()));

        RowDataDeserializationSchema simpleSchema =
                getRowDataDeserializationSchema(new RowType(simpleFields));

        assertThat(simpleSchema).isNotNull();
        assertThat(simpleSchema.getProducedType(rowType).getTypeClass()).isEqualTo(RowData.class);
    }
}
