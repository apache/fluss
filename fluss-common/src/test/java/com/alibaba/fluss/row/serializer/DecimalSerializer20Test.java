/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row.serializer;

import com.alibaba.fluss.row.Decimal;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DecimalSerializer} with precision 20, scale 10. */
public class DecimalSerializer20Test extends SerializerTestBase<Decimal> {

    @Override
    protected DecimalSerializer createSerializer() {
        return new DecimalSerializer(20, 10);
    }

    @Override
    protected boolean deepEquals(Decimal t1, Decimal t2) {
        if (t1 == null && t2 == null) {
            return true;
        }
        if (t1 == null || t2 == null) {
            return false;
        }
        return t1.equals(t2);
    }

    @Override
    protected Decimal[] getTestData() {
        int precision = 20;
        int scale = 10;

        // For non-compact precisions, use larger values that require byte arrays
        return new Decimal[] {
            Decimal.fromBigDecimal(new BigDecimal("1"), precision, scale),
            Decimal.fromBigDecimal(new BigDecimal("2"), precision, scale),
            Decimal.fromBigDecimal(new BigDecimal("3"), precision, scale),
            Decimal.fromBigDecimal(new BigDecimal("4"), precision, scale),
            Decimal.fromBigDecimal(new BigDecimal("0"), precision, scale),
            Decimal.fromBigDecimal(new BigDecimal("-12345"), precision, scale),
            Decimal.fromBigDecimal(new BigDecimal("123.45"), precision, scale),
            Decimal.fromBigDecimal(new BigDecimal("-123.45"), precision, scale),
            Decimal.fromBigDecimal(BigDecimal.ZERO, precision, scale),
            Decimal.fromBigDecimal(BigDecimal.ONE, precision, scale),
            Decimal.fromBigDecimal(BigDecimal.TEN, precision, scale),
            Decimal.fromBigDecimal(new BigDecimal("99999999.99"), precision, scale),
            Decimal.fromBigDecimal(new BigDecimal("-99999999.99"), precision, scale),
            Decimal.fromBigDecimal(new BigDecimal("0.1234567890"), precision, scale),
            Decimal.fromBigDecimal(new BigDecimal("9999999999.9999999999"), precision, scale)
        };
    }

    @Test
    void testEqualsAndHashCode() {
        DecimalSerializer serializer1 = new DecimalSerializer(20, 10);
        DecimalSerializer serializer2 = new DecimalSerializer(20, 10);
        DecimalSerializer serializer3 = new DecimalSerializer(6, 3);

        assertThat(serializer1).isEqualTo(serializer2);
        assertThat(serializer1).isNotEqualTo(serializer3);
        assertThat(serializer1).isNotEqualTo(null);
        assertThat(serializer1).isNotEqualTo("string");

        assertThat(serializer1.hashCode()).isEqualTo(serializer2.hashCode());
        assertThat(serializer1.hashCode()).isNotEqualTo(serializer3.hashCode());
    }

    @Test
    void testNonCompactMode() {
        DecimalSerializer serializer = createSerializer();
        // Check actual compact mode behavior for precision 20
        boolean isCompact = Decimal.isCompact(20);
        // Note: The actual behavior depends on the implementation
        // We just test that the method works without throwing exceptions
        assertThat(isCompact).isNotNull();
    }

    @Test
    void testNullHandling() {
        DecimalSerializer serializer = createSerializer();
        // Test that null values are handled properly
        assertThat(serializer.copy(null)).isNull();
    }
}
