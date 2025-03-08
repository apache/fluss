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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static com.alibaba.fluss.utils.TypeUtils.castFromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test of {@link com.alibaba.fluss.utils.TypeUtils}. */
public class TypeUtilsTest {
    @Test
    void testCastFromString() {
        assertThat(castFromString("abc", DataTypes.CHAR(3)))
                .isEqualTo(BinaryString.fromString("abc"));

        assertThat(castFromString("abc", DataTypes.STRING()))
                .isEqualTo(BinaryString.fromString("abc"));

        assertThat((boolean) castFromString("true", DataTypes.BOOLEAN())).isTrue();
        assertThat((boolean) castFromString("false", DataTypes.BOOLEAN())).isFalse();

        assertThat(castFromString("abc", DataTypes.BINARY(3)))
                .isEqualTo(new byte[] {'a', 'b', 'c'});
        assertThat(castFromString("abc", DataTypes.BYTES())).isEqualTo(new byte[] {'a', 'b', 'c'});

        assertThat(castFromString("13145678.90123", DataTypes.DECIMAL(15, 5)))
                .isEqualTo(Decimal.fromBigDecimal(new BigDecimal("13145678.90123"), 15, 5));

        assertThat(castFromString("2", DataTypes.TINYINT())).isEqualTo((byte) 2);
        assertThat(castFromString("2", DataTypes.SMALLINT())).isEqualTo((short) 2);
        assertThat(castFromString("2", DataTypes.INT())).isEqualTo(2);
        assertThat(castFromString("2", DataTypes.BIGINT())).isEqualTo(2L);
        assertThat(castFromString("2.2", DataTypes.FLOAT())).isEqualTo(2.2f);
        assertThat(castFromString("2.2", DataTypes.DOUBLE())).isEqualTo(2.2d);

        assertThat(DateTimeUtils.toLocalDate((int) castFromString("2023-10-25", DataTypes.DATE())))
                .isEqualTo(LocalDate.of(2023, 10, 25));
        assertThat(DateTimeUtils.toLocalTime((int) castFromString("09:30:00.0", DataTypes.TIME())))
                .isEqualTo(LocalTime.of(9, 30, 0, 0));
        assertThat(castFromString("2023-10-25", DataTypes.TIMESTAMP(6)))
                .isEqualTo(TimestampNtz.fromLocalDateTime(LocalDateTime.of(2023, 10, 25, 0, 0)));

        assertThatThrownBy(() -> castFromString("1", DataTypes.ARRAY(DataTypes.INT())))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported type");
    }
}
