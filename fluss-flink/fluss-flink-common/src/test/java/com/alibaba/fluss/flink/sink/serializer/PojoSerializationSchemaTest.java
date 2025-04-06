/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.sink.serializer;

import com.alibaba.fluss.flink.common.Order;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.StringType;

import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * Test class for the {@link FlussSerializationSchema} implementations. It validates the conversion
 * from Pojo to Fluss RowType.
 */
public class PojoSerializationSchemaTest {

    private RowType rowType;
    private RowType extendedRowType;
    private PojoSerializationSchema<Order> standardSchema;

    @Mock private FlussSerializationSchema.InitializationContext context;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Create standard schema matching the Order class
        rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField(
                                        "address", new StringType(true), "Shipping address")));

        // Create extended schema with an extra field
        extendedRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField("address", new StringType(true), "Shipping address"),
                                new DataField(
                                        "nonExistentField",
                                        new StringType(true),
                                        "A field that doesn't exist")));

        // Configure mock context to return the standard row type
        when(context.getRowSchema()).thenReturn(rowType);

        // Create and initialize standard schema
        standardSchema = createAndInitializeSchema(Order.class, rowType);
    }

    // Helper method to create and initialize a serialization schema
    private <T> PojoSerializationSchema<T> createAndInitializeSchema(
            Class<T> pojoClass, RowType rowType) throws Exception {
        PojoSerializationSchema<T> schema = new PojoSerializationSchema<>(pojoClass, rowType);

        // Configure context
        when(context.getRowSchema()).thenReturn(rowType);

        // Initialize schema
        schema.open(context);

        return schema;
    }

    // Basic serialization test for Pojo
    @Test
    public void testOrderSerialization() throws Exception {
        Order order = new Order(1001L, 5001L, 10, "123 Mumbai");

        RowData result = standardSchema.serialize(order);

        assertThat(result.getArity()).isEqualTo(4);
        assertThat(result.getLong(0)).isEqualTo(1001L);
        assertThat(result.getLong(1)).isEqualTo(5001L);
        assertThat(result.getInt(2)).isEqualTo(10);
        assertThat(result.getString(3).toString()).isEqualTo("123 Mumbai");
    }

    // Test method specifically for checking how the serializer handles null values.
    @Test
    public void testNullHandling() throws Exception {
        RowData nullResult = standardSchema.serialize(null);
        assertThat(nullResult).isNull();

        Order order = new Order(1002L, 5002L, 5, null);

        RowData result = standardSchema.serialize(order);
        assertThat(result.getLong(0)).isEqualTo(1002L);
        assertThat(result.getLong(1)).isEqualTo(5002L);
        assertThat(result.getInt(2)).isEqualTo(5);
        assertThat(result.isNullAt(3)).isTrue();
    }

    // This checks that when a field exists in the schema but not in the POJO, it's treated as null.
    @Test
    public void testMissingField() throws Exception {
        PojoSerializationSchema<Order> extendedSchema =
                createAndInitializeSchema(Order.class, extendedRowType);

        Order order = new Order(1003L, 5003L, 15, "1124 Rohtak");

        RowData result = extendedSchema.serialize(order);

        assertThat(result.getArity()).isEqualTo(5);
        assertThat(result.getLong(0)).isEqualTo(1003L);
        assertThat(result.getLong(1)).isEqualTo(5003L);
        assertThat(result.getInt(2)).isEqualTo(15);
        assertThat(result.getString(3).toString()).isEqualTo("1124 Rohtak");
        assertThat(result.isNullAt(4)).isTrue(); // The non-existent field should be null
    }

    @Test
    public void testUninitializedSchema() {
        // Create a schema but don't initialize it
        PojoSerializationSchema<Order> uninitializedSchema =
                new PojoSerializationSchema<>(Order.class);

        // Try to serialize with uninitialized schema
        Order order = new Order(1004L, 5004L, 20, "555 Shenzhen");

        assertThatThrownBy(() -> uninitializedSchema.serialize(order))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Converter not initialized");
    }

    @Test
    public void testSerializable() throws Exception {
        // Serialize the schema
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(standardSchema);
        oos.close();

        // Deserialize the schema
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        PojoSerializationSchema<Order> deserializedSchema =
                (PojoSerializationSchema<Order>) ois.readObject();
        ois.close();

        deserializedSchema.open(context);

        Order order = new Order(1005L, 5005L, 25, "666 Beijing");
        RowData result = deserializedSchema.serialize(order);

        assertThat(result).isNotNull();
        assertThat(result.getArity()).isEqualTo(4);
        assertThat(result.getLong(0)).isEqualTo(1005L);
    }
}
