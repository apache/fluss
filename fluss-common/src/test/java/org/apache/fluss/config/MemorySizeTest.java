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

package org.apache.fluss.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link org.apache.fluss.config.MemorySize} class. */
public class MemorySizeTest {

    static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of(
                        new MemorySize(MemorySize.MemoryUnit.KILO_BYTES.getMultiplier() + 1),
                        "1025 bytes"),
                Arguments.of(new MemorySize(100), "100 bytes"),
                Arguments.of(new MemorySize(1024), "1 kb"),
                Arguments.of(
                        new MemorySize(MemorySize.MemoryUnit.GIGA_BYTES.getMultiplier() + 1),
                        String.format(
                                "%d %s",
                                MemorySize.MemoryUnit.GIGA_BYTES.getMultiplier() + 1, "bytes")),
                Arguments.of(new MemorySize(0), "0 bytes"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testFormatting(MemorySize size, String expectedString) {
        assertThat(size.toString()).isEqualTo(expectedString);
    }

    @Test
    void testUnitConversion() {
        final MemorySize zero = MemorySize.ZERO;
        assertThat(zero.getBytes()).isEqualTo(0);
        assertThat(zero.getKibiBytes()).isEqualTo(0);
        assertThat(zero.getMebiBytes()).isEqualTo(0);
        assertThat(zero.getGibiBytes()).isEqualTo(0);
        assertThat(zero.getTebiBytes()).isEqualTo(0);

        final MemorySize bytes = new MemorySize(955);
        assertThat(bytes.getBytes()).isEqualTo(955);
        assertThat(bytes.getKibiBytes()).isEqualTo(0);
        assertThat(bytes.getMebiBytes()).isEqualTo(0);
        assertThat(bytes.getGibiBytes()).isEqualTo(0);
        assertThat(bytes.getTebiBytes()).isEqualTo(0);

        final MemorySize kilos = new MemorySize(18500);
        assertThat(kilos.getBytes()).isEqualTo(18500);
        assertThat(kilos.getKibiBytes()).isEqualTo(18);
        assertThat(kilos.getMebiBytes()).isEqualTo(0);
        assertThat(kilos.getGibiBytes()).isEqualTo(0);
        assertThat(kilos.getTebiBytes()).isEqualTo(0);

        final MemorySize megas = new MemorySize(15 * 1024 * 1024);
        assertThat(megas.getBytes()).isEqualTo(15_728_640);
        assertThat(megas.getKibiBytes()).isEqualTo(15_360);
        assertThat(megas.getMebiBytes()).isEqualTo(15);
        assertThat(megas.getGibiBytes()).isEqualTo(0);
        assertThat(megas.getTebiBytes()).isEqualTo(0);

        final MemorySize teras = new MemorySize(2L * 1024 * 1024 * 1024 * 1024 + 10);
        assertThat(teras.getBytes()).isEqualTo(2199023255562L);
        assertThat(teras.getKibiBytes()).isEqualTo(2147483648L);
        assertThat(teras.getMebiBytes()).isEqualTo(2097152);
        assertThat(teras.getGibiBytes()).isEqualTo(2048);
        assertThat(teras.getTebiBytes()).isEqualTo(2);
    }

    @Test
    void testInvalid() {
        assertThatThrownBy(() -> new MemorySize(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testParseBytes() {
        assertThat(MemorySize.parseBytes("1234b")).isEqualTo(1234);
        assertThat(MemorySize.parseBytes("1234 b")).isEqualTo(1234);
        assertThat(MemorySize.parseBytes("1234bytes")).isEqualTo(1234);
        assertThat(MemorySize.parseBytes("1234 bytes")).isEqualTo(1234);
    }

    @Test
    void testParseKibiBytes() {
        assertThat(MemorySize.parse("667766k").getKibiBytes()).isEqualTo(667766);
        assertThat(MemorySize.parse("667766 k").getKibiBytes()).isEqualTo(667766);
        assertThat(MemorySize.parse("667766kb").getKibiBytes()).isEqualTo(667766);
        assertThat(MemorySize.parse("667766 kb").getKibiBytes()).isEqualTo(667766);
        assertThat(MemorySize.parse("667766kibibytes").getKibiBytes()).isEqualTo(667766);
        assertThat(MemorySize.parse("667766 kibibytes").getKibiBytes()).isEqualTo(667766);
    }

    @Test
    void testParseMebiBytes() {
        assertThat(MemorySize.parse("7657623m").getMebiBytes()).isEqualTo(7657623);
        assertThat(MemorySize.parse("7657623 m").getMebiBytes()).isEqualTo(7657623);
        assertThat(MemorySize.parse("7657623mb").getMebiBytes()).isEqualTo(7657623);
        assertThat(MemorySize.parse("7657623 mb").getMebiBytes()).isEqualTo(7657623);
        assertThat(MemorySize.parse("7657623mebibytes").getMebiBytes()).isEqualTo(7657623);
        assertThat(MemorySize.parse("7657623 mebibytes").getMebiBytes()).isEqualTo(7657623);
    }

    @Test
    void testParseGibiBytes() {
        assertThat(MemorySize.parse("987654g").getGibiBytes()).isEqualTo(987654);
        assertThat(MemorySize.parse("987654 g").getGibiBytes()).isEqualTo(987654);
        assertThat(MemorySize.parse("987654g").getGibiBytes()).isEqualTo(987654);
        assertThat(MemorySize.parse("987654 gb").getGibiBytes()).isEqualTo(987654);
        assertThat(MemorySize.parse("987654gibibytes").getGibiBytes()).isEqualTo(987654);
        assertThat(MemorySize.parse("987654 gibibytes").getGibiBytes()).isEqualTo(987654);
    }

    @Test
    void testParseTebiBytes() {
        assertThat(MemorySize.parse("1234567t").getTebiBytes()).isEqualTo(1234567);
        assertThat(MemorySize.parse("1234567 t").getTebiBytes()).isEqualTo(1234567);
        assertThat(MemorySize.parse("1234567tb").getTebiBytes()).isEqualTo(1234567);
        assertThat(MemorySize.parse("1234567 tb").getTebiBytes()).isEqualTo(1234567);
        assertThat(MemorySize.parse("1234567tebibytes").getTebiBytes()).isEqualTo(1234567);
        assertThat(MemorySize.parse("1234567 tebibytes").getTebiBytes()).isEqualTo(1234567);
    }

    @Test
    void testUpperCase() {
        assertThat(MemorySize.parse("1 B").getBytes()).isEqualTo(1L);
        assertThat(MemorySize.parse("1 K").getKibiBytes()).isEqualTo(1L);
        assertThat(MemorySize.parse("1 M").getMebiBytes()).isEqualTo(1L);
        assertThat(MemorySize.parse("1 G").getGibiBytes()).isEqualTo(1L);
        assertThat(MemorySize.parse("1 T").getTebiBytes()).isEqualTo(1L);
    }

    @Test
    void testTrimBeforeParse() {
        assertThat(MemorySize.parseBytes("      155      bytes   ")).isEqualTo(155L);
    }

    @Test
    void testParseInvalid() {
        // no unit
        assertThatThrownBy(() -> MemorySize.parseBytes("1234"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The memory value '1234' does not specify a memory unit.");
        assertThatThrownBy(() -> MemorySize.parseBytes("      155      "))
                .isInstanceOf(IllegalArgumentException.class);

        // null
        assertThatThrownBy(() -> MemorySize.parseBytes(null))
                .isInstanceOf(NullPointerException.class);

        // empty
        assertThatThrownBy(() -> MemorySize.parseBytes(""))
                .isInstanceOf(IllegalArgumentException.class);

        // blank
        assertThatThrownBy(() -> MemorySize.parseBytes("     "))
                .isInstanceOf(IllegalArgumentException.class);

        // no number
        assertThatThrownBy(() -> MemorySize.parseBytes("foobar or fubar or foo bazz"))
                .isInstanceOf(IllegalArgumentException.class);

        // wrong unit
        assertThatThrownBy(() -> MemorySize.parseBytes("16 gjah"))
                .isInstanceOf(IllegalArgumentException.class);

        // multiple numbers
        assertThatThrownBy(() -> MemorySize.parseBytes("16 16 17 18 bytes"))
                .isInstanceOf(IllegalArgumentException.class);

        // negative number
        assertThatThrownBy(() -> MemorySize.parseBytes("-100 bytes"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testParseNumberOverflow() {
        assertThatThrownBy(() -> MemorySize.parseBytes("100000000000000000000000000000000 bytes"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testParseNumberTimeUnitOverflow() {
        assertThatThrownBy(() -> MemorySize.parseBytes("100000000000000 tb"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDivideByLong() {
        final MemorySize memory = new MemorySize(100L);
        assertThat(memory.divide(23)).isEqualTo(new MemorySize(4L));
    }

    @Test
    void testDivideByNegativeLong() {
        final MemorySize memory = new MemorySize(100L);
        assertThatThrownBy(() -> memory.divide(-23L)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testToHumanReadableString() {
        assertThat(new MemorySize(0L).toHumanReadableString()).isEqualTo("0 bytes");
        assertThat(new MemorySize(1L).toHumanReadableString()).isEqualTo("1 bytes");
        assertThat(new MemorySize(1024L).toHumanReadableString()).isEqualTo(("1024 bytes"));
        assertThat(new MemorySize(1025L).toHumanReadableString()).isEqualTo("1.001kb (1025 bytes)");
        assertThat(new MemorySize(1536L).toHumanReadableString()).isEqualTo("1.500kb (1536 bytes)");
        assertThat(new MemorySize(1_000_000L).toHumanReadableString())
                .isEqualTo("976.563kb (1000000 bytes)");
        assertThat(new MemorySize(1_000_000_000L).toHumanReadableString())
                .isEqualTo("953.674mb (1000000000 bytes)");
        assertThat(new MemorySize(1_000_000_000_000L).toHumanReadableString())
                .isEqualTo("931.323gb (1000000000000 bytes)");
        assertThat(new MemorySize(1_000_000_000_000_000L).toHumanReadableString())
                .isEqualTo("909.495tb (1000000000000000 bytes)");
    }
}
