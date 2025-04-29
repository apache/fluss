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

import com.alibaba.fluss.flink.source.testutils.Order;
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

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * Test class for the {@link OrderSerializationSchema} that validates the conversion from {@link
 * Order} objects to Flink's {@link RowData} format.
 */
public class OrderSerializationSchemaTest {

    private RowType standardRowType;
    private RowType extendedRowType;
    private OrderSerializationSchema serializer;

    @Mock private FlussSerializationSchema.InitializationContext context;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Create standard schema matching the Order class
        standardRowType =
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

        // Set up the context to return the standard schema
        when(context.getRowSchema()).thenReturn(standardRowType);

        // Create and initialize the serializer
        serializer = new OrderSerializationSchema();
        serializer.open(context);
    }

    // Basic serialization test for Order
    @Test
    public void testOrderSerialization() throws Exception {

        Order order = new Order(1001L, 5001L, 10, "A-12 Mumbai");

        RowData result = serializer.serialize(order);

        assertThat(result.getArity()).isEqualTo(4);
        assertThat(result.getLong(0)).isEqualTo(1001L);
        assertThat(result.getLong(1)).isEqualTo(5001L);
        assertThat(result.getInt(2)).isEqualTo(10);
        assertThat(result.getString(3).toString()).isEqualTo("A-12 Mumbai");
    }

    // Test null input
    @Test
    public void testNullHandling() throws Exception {

        RowData nullResult = serializer.serialize(null);
        assertThat(nullResult).isNull();

        Order order = new Order(1002L, 5002L, 5, null);

        RowData result = serializer.serialize(order);
        assertThat(result.getLong(0)).isEqualTo(1002L);
        assertThat(result.getLong(1)).isEqualTo(5002L);
        assertThat(result.getInt(2)).isEqualTo(5);
        assertThat(result.isNullAt(3)).isTrue();
    }

    // Test for extended schema
    @Test
    public void testExtendedSchema() throws Exception {

        when(context.getRowSchema()).thenReturn(extendedRowType);

        OrderSerializationSchema extendedSerializer = new OrderSerializationSchema();
        extendedSerializer.open(context);

        Order order = new Order(1003L, 5003L, 15, "1124 Rohtak");

        RowData result = extendedSerializer.serialize(order);

        assertThat(result.getArity()).isEqualTo(5);
        assertThat(result.getLong(0)).isEqualTo(1003L);
        assertThat(result.getLong(1)).isEqualTo(5003L);
        assertThat(result.getInt(2)).isEqualTo(15);
        assertThat(result.getString(3).toString()).isEqualTo("1124 Rohtak");
        assertThat(result.isNullAt(4)).isTrue(); // The non-existent field should be null
    }

    // Test for incompatible schema with too few fields
    @Test
    public void testIncompatibleSchema() {

        RowType incompatibleRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(
                                        "singleField", new BigIntType(false), "Single Field")));

        when(context.getRowSchema()).thenReturn(incompatibleRowType);

        OrderSerializationSchema invalidSerializer = new OrderSerializationSchema();

        assertThatThrownBy(() -> invalidSerializer.open(context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Schema must have at least 4 fields");
    }
}
