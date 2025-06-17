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

package com.alibaba.fluss.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StructuredOptionsSplitter}. */
public class StructuredOptionsSplitterTest {

    static Stream<Arguments> parameters() {
        return Stream.of(
                // Use single quotes for quoting
                Arguments.of(TestSpec.split("'A;B';C", ';').expect("A;B", "C")),
                Arguments.of(TestSpec.split("'A;B';'C'", ';').expect("A;B", "C")),
                Arguments.of(TestSpec.split("A;B;C", ';').expect("A", "B", "C")),
                Arguments.of(TestSpec.split("'AB''D;B';C", ';').expect("AB'D;B", "C")),
                Arguments.of(TestSpec.split("A'BD;B';C", ';').expect("A'BD", "B'", "C")),
                Arguments.of(
                        TestSpec.split("'AB'D;B;C", ';')
                                .expectException(
                                        "Could not split string. Illegal quoting at position: 3")),
                Arguments.of(
                        TestSpec.split("'A", ';')
                                .expectException(
                                        "Could not split string. Quoting was not closed properly.")),
                Arguments.of(
                        TestSpec.split("C;'", ';')
                                .expectException(
                                        "Could not split string. Quoting was not closed properly.")),

                // Use double quotes for quoting
                Arguments.of(TestSpec.split("\"A;B\";C", ';').expect("A;B", "C")),
                Arguments.of(TestSpec.split("\"A;B\";\"C\"", ';').expect("A;B", "C")),
                Arguments.of(TestSpec.split("A;B;C", ';').expect("A", "B", "C")),
                Arguments.of(TestSpec.split("\"AB\"\"D;B\";C", ';').expect("AB\"D;B", "C")),
                Arguments.of(TestSpec.split("A\"BD;B\";C", ';').expect("A\"BD", "B\"", "C")),
                Arguments.of(
                        TestSpec.split("\"AB\"D;B;C", ';')
                                .expectException(
                                        "Could not split string. Illegal quoting at position: 3")),
                Arguments.of(
                        TestSpec.split("\"A", ';')
                                .expectException(
                                        "Could not split string. Quoting was not closed properly.")),
                Arguments.of(
                        TestSpec.split("C;\"", ';')
                                .expectException(
                                        "Could not split string. Quoting was not closed properly.")),

                // Mix different quoting
                Arguments.of(TestSpec.split("'AB\"D';B;C", ';').expect("AB\"D", "B", "C")),
                Arguments.of(TestSpec.split("'AB\"D;B';C", ';').expect("AB\"D;B", "C")),
                Arguments.of(TestSpec.split("'AB\"''D;B';C", ';').expect("AB\"'D;B", "C")),
                Arguments.of(TestSpec.split("\"AB'D\";B;C", ';').expect("AB'D", "B", "C")),
                Arguments.of(TestSpec.split("\"AB'D;B\";C", ';').expect("AB'D;B", "C")),
                Arguments.of(TestSpec.split("\"AB'\"\"D;B\";C", ';').expect("AB'\"D;B", "C")),

                // Use different delimiter
                Arguments.of(TestSpec.split("'A,B',C", ',').expect("A,B", "C")),
                Arguments.of(TestSpec.split("A,B,C", ',').expect("A", "B", "C")),

                // Whitespaces handling
                Arguments.of(TestSpec.split("   'A;B'    ;   C   ", ';').expect("A;B", "C")),
                Arguments.of(TestSpec.split("   A;B    ;   C   ", ';').expect("A", "B", "C")),
                Arguments.of(TestSpec.split("'A;B'    ;C A", ';').expect("A;B", "C A")),
                Arguments.of(
                        TestSpec.split("' A    ;B'    ;'   C'", ';').expect(" A    ;B", "   C")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testParse(TestSpec testSpec) {
        if (testSpec.getExpectedException().isPresent()) {
            assertThat(testSpec.getExpectedException().get()).isNotEmpty();
            return;
        }

        List<String> splits =
                StructuredOptionsSplitter.splitEscaped(
                        testSpec.getString(), testSpec.getDelimiter());

        assertThat(splits).isEqualTo(testSpec.getExpectedSplits());
    }

    private static class TestSpec {
        private final String string;
        private final char delimiter;
        @Nullable private String expectedException = null;
        private List<String> expectedSplits = null;

        private TestSpec(String string, char delimiter) {
            this.string = string;
            this.delimiter = delimiter;
        }

        public static TestSpec split(String string, char delimiter) {
            return new TestSpec(string, delimiter);
        }

        public TestSpec expect(String... splits) {
            this.expectedSplits = Arrays.asList(splits);
            return this;
        }

        public TestSpec expectException(String message) {
            this.expectedException = message;
            return this;
        }

        public String getString() {
            return string;
        }

        public char getDelimiter() {
            return delimiter;
        }

        public Optional<String> getExpectedException() {
            return Optional.ofNullable(expectedException);
        }

        public List<String> getExpectedSplits() {
            return expectedSplits;
        }

        @Override
        public String toString() {
            return String.format(
                    "str = [ %s ], del = '%s', expected = %s",
                    string,
                    delimiter,
                    getExpectedException()
                            .map(e -> String.format("Exception(%s)", e))
                            .orElseGet(
                                    () ->
                                            expectedSplits.stream()
                                                    .collect(
                                                            Collectors.joining("], [", "[", "]"))));
        }
    }
}
