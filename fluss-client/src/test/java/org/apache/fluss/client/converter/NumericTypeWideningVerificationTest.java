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

package org.apache.fluss.client.converter;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verification tests for numeric type widening in POJO to Row conversion.
 *
 * <p>These tests verify that POJOs with smaller numeric types (e.g., Integer, Short) can be
 * successfully converted to schemas with larger numeric types (e.g., BIGINT), following Java's safe
 * widening conversion rules.
 */
public class NumericTypeWideningVerificationTest {

    /** Test POJO with int orderId field. */
    public static class OrderPojo {
        public Integer orderId;

        public OrderPojo() {}

        public OrderPojo(Integer orderId) {
            this.orderId = orderId;
        }
    }

    @Test
    public void testIntToBigIntWideningManual() {
        // Create schema with BIGINT orderId field
        RowType table = RowType.builder().field("orderId", DataTypes.BIGINT()).build();
        RowType projection = table;

        // Create converter - this should succeed with numeric widening
        PojoToRowConverter<OrderPojo> converter =
                PojoToRowConverter.of(OrderPojo.class, table, projection);

        // Create test POJO with int value
        OrderPojo pojo = new OrderPojo(123456);

        // Convert to row
        GenericRow row = converter.toRow(pojo);

        // Verify converted row has correct long value
        assertThat(row.getLong(0)).isEqualTo(123456L);
    }

    /** Test POJO with short quantity field. */
    public static class ProductPojo {
        public Short quantity;

        public ProductPojo() {}

        public ProductPojo(Short quantity) {
            this.quantity = quantity;
        }
    }

    @Test
    public void testShortToBigIntWideningManual() {
        // Create schema with BIGINT quantity field
        RowType table = RowType.builder().field("quantity", DataTypes.BIGINT()).build();
        RowType projection = table;

        // Create converter - this should succeed with numeric widening
        PojoToRowConverter<ProductPojo> converter =
                PojoToRowConverter.of(ProductPojo.class, table, projection);

        // Create test POJO with short value
        ProductPojo pojo = new ProductPojo((short) 999);

        // Convert to row
        GenericRow row = converter.toRow(pojo);

        // Verify converted row has correct long value
        assertThat(row.getLong(0)).isEqualTo(999L);
    }

    @Test
    public void testNullHandlingWithWidening() {
        // Create schema with BIGINT orderId field
        RowType table = RowType.builder().field("orderId", DataTypes.BIGINT()).build();
        RowType projection = table;

        // Create converter
        PojoToRowConverter<OrderPojo> converter =
                PojoToRowConverter.of(OrderPojo.class, table, projection);

        // Create test POJO with null value
        OrderPojo pojo = new OrderPojo(null);

        // Convert to row
        GenericRow row = converter.toRow(pojo);

        // Verify null is preserved
        assertThat(row.isNullAt(0)).isTrue();
    }
}
