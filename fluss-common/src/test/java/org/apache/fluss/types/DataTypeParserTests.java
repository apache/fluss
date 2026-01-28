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

package org.apache.fluss.types;

import org.apache.fluss.metadata.ValidationException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataTypeParser}. */
public class DataTypeParserTests {
    private static Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forString("CHAR").expectType(new CharType()),
                TestSpec.forString("CHAR NOT NULL").expectType(new CharType().copy(false)),
                TestSpec.forString("CHAR   NOT \t\nNULL").expectType(new CharType().copy(false)),
                TestSpec.forString("char not null").expectType(new CharType().copy(false)),
                TestSpec.forString("CHAR NULL").expectType(new CharType()),
                TestSpec.forString("CHAR(33)").expectType(new CharType(33)),
                TestSpec.forString("STRING").expectType(new StringType()),
                TestSpec.forString("BOOLEAN").expectType(new BooleanType()),
                TestSpec.forString("BINARY").expectType(new BinaryType()),
                TestSpec.forString("BINARY(33)").expectType(new BinaryType(33)),
                TestSpec.forString("BYTES").expectType(new BytesType()),
                TestSpec.forString("DECIMAL").expectType(new DecimalType()),
                TestSpec.forString("DEC").expectType(new DecimalType()),
                TestSpec.forString("NUMERIC").expectType(new DecimalType()),
                TestSpec.forString("DECIMAL(10)").expectType(new DecimalType(10)),
                TestSpec.forString("DEC(10)").expectType(new DecimalType(10)),
                TestSpec.forString("NUMERIC(10)").expectType(new DecimalType(10)),
                TestSpec.forString("DECIMAL(10, 3)").expectType(new DecimalType(10, 3)),
                TestSpec.forString("DEC(10, 3)").expectType(new DecimalType(10, 3)),
                TestSpec.forString("NUMERIC(10, 3)").expectType(new DecimalType(10, 3)),
                TestSpec.forString("TINYINT").expectType(new TinyIntType()),
                TestSpec.forString("SMALLINT").expectType(new SmallIntType()),
                TestSpec.forString("INTEGER").expectType(new IntType()),
                TestSpec.forString("INT").expectType(new IntType()),
                TestSpec.forString("BIGINT").expectType(new BigIntType()),
                TestSpec.forString("FLOAT").expectType(new FloatType()),
                TestSpec.forString("DOUBLE").expectType(new DoubleType()),
                TestSpec.forString("DOUBLE PRECISION").expectType(new DoubleType()),
                TestSpec.forString("DATE").expectType(new DateType()),
                TestSpec.forString("TIME").expectType(new TimeType()),
                TestSpec.forString("TIME(3)").expectType(new TimeType(3)),
                TestSpec.forString("TIME WITHOUT TIME ZONE").expectType(new TimeType()),
                TestSpec.forString("TIME(3) WITHOUT TIME ZONE").expectType(new TimeType(3)),
                TestSpec.forString("TIMESTAMP").expectType(new TimestampType()),
                TestSpec.forString("TIMESTAMP(3)").expectType(new TimestampType(3)),
                TestSpec.forString("TIMESTAMP WITHOUT TIME ZONE").expectType(new TimestampType()),
                TestSpec.forString("TIMESTAMP(3) WITHOUT TIME ZONE")
                        .expectType(new TimestampType(3)),
                TestSpec.forString("TIMESTAMP WITH LOCAL TIME ZONE")
                        .expectType(new LocalZonedTimestampType()),
                TestSpec.forString("TIMESTAMP_LTZ").expectType(new LocalZonedTimestampType()),
                TestSpec.forString("TIMESTAMP(3) WITH LOCAL TIME ZONE")
                        .expectType(new LocalZonedTimestampType(3)),
                TestSpec.forString("TIMESTAMP_LTZ(3)").expectType(new LocalZonedTimestampType(3)),
                TestSpec.forString("ARRAY<TIMESTAMP(3) WITH LOCAL TIME ZONE>")
                        .expectType(new ArrayType(new LocalZonedTimestampType(3))),
                TestSpec.forString("ARRAY<INT NOT NULL>")
                        .expectType(new ArrayType(new IntType(false))),
                TestSpec.forString("INT ARRAY").expectType(new ArrayType(new IntType())),
                TestSpec.forString("INT NOT NULL ARRAY")
                        .expectType(new ArrayType(new IntType(false))),
                TestSpec.forString("INT ARRAY NOT NULL")
                        .expectType(new ArrayType(false, new IntType())),
                TestSpec.forString("MAP<BIGINT, BOOLEAN>")
                        .expectType(new MapType(new BigIntType(), new BooleanType())),
                TestSpec.forString("ROW<f0 INT NOT NULL, f1 BOOLEAN>")
                        .expectType(
                                new RowType(
                                        Arrays.asList(
                                                new DataField("f0", new IntType(false)),
                                                new DataField("f1", new BooleanType())))),
                TestSpec.forString("ROW(f0 INT NOT NULL, f1 BOOLEAN)")
                        .expectType(
                                new RowType(
                                        Arrays.asList(
                                                new DataField("f0", new IntType(false)),
                                                new DataField("f1", new BooleanType())))),
                TestSpec.forString("ROW<`f0` INT>")
                        .expectType(
                                new RowType(
                                        Collections.singletonList(
                                                new DataField("f0", new IntType())))),
                TestSpec.forString("ROW(`a` INT)")
                        .expectType(
                                new RowType(
                                        Collections.singletonList(
                                                new DataField("a", new IntType())))),
                TestSpec.forString("ROW<>").expectType(new RowType(Collections.emptyList())),
                TestSpec.forString("ROW()").expectType(new RowType(Collections.emptyList())),
                TestSpec.forString(
                                "ROW<a INT NOT NULL 'This is a comment.', b BOOLEAN 'This as well.'>")
                        .expectType(
                                new RowType(
                                        Arrays.asList(
                                                new DataField(
                                                        "a",
                                                        new IntType(false),
                                                        "This is a comment."),
                                                new DataField(
                                                        "b", new BooleanType(), "This as well.")))),
                TestSpec.forString("ROW<f0 STRING, f1 ROW<n0 INT, n1 STRING>>")
                        .expectType(
                                new RowType(
                                        Arrays.asList(
                                                new DataField("f0", new StringType()),
                                                new DataField(
                                                        "f1",
                                                        new RowType(
                                                                Arrays.asList(
                                                                        new DataField(
                                                                                "n0",
                                                                                new IntType()),
                                                                        new DataField(
                                                                                "n1",
                                                                                new StringType()))))))),
                // error message testing
                TestSpec.forString("TIMESTAMP WITH TIME ZONE")
                        .expectErrorMessage("Timestamp with time zone not supported"),
                TestSpec.forString("ROW<`f0").expectErrorMessage("Unexpected end"),
                TestSpec.forString("ROW<`f0`").expectErrorMessage("Unexpected end"),
                TestSpec.forString("CHAR(test)")
                        .expectErrorMessage("<LITERAL_INT> expected but was <IDENTIFIER>"),
                TestSpec.forString("CHAR(33333333333)").expectErrorMessage("Invalid integer value"),
                TestSpec.forString("VARCHAR(20)")
                        .expectErrorMessage(
                                "Unsupported keyword VARCHAR found in type information."),
                TestSpec.forString("ROW<field INT, field2>")
                        .expectErrorMessage("<KEYWORD> expected"));
    }

    @ParameterizedTest(name = "{index}: [From: {0}, To: {1}]")
    @MethodSource("testData")
    void testParsing(TestSpec testSpec) {
        if (testSpec.expectedType != null) {
            assertThat(DataTypeParser.parse(testSpec.typeString)).isEqualTo(testSpec.expectedType);
        }
    }

    @ParameterizedTest(name = "{index}: [From: {0}, To: {1}]")
    @MethodSource("testData")
    void testSerializableParsing(TestSpec testSpec) {
        if (testSpec.expectedType != null) {
            assertThat(DataTypeParser.parse(testSpec.expectedType.asSerializableString()))
                    .isEqualTo(testSpec.expectedType);
        }
    }

    @ParameterizedTest(name = "{index}: [From: {0}, To: {1}]")
    @MethodSource("testData")
    void testErrorMessage(TestSpec testSpec) {
        if (testSpec.expectedErrorMessage != null) {
            assertThatThrownBy(() -> DataTypeParser.parse(testSpec.typeString))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(testSpec.expectedErrorMessage);
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class TestSpec {

        private final String typeString;

        private @Nullable DataType expectedType;

        private @Nullable String expectedErrorMessage;

        private TestSpec(String typeString) {
            this.typeString = typeString;
        }

        static TestSpec forString(String typeString) {
            return new TestSpec(typeString);
        }

        TestSpec expectType(DataType expectedType) {
            this.expectedType = expectedType;
            return this;
        }

        TestSpec expectErrorMessage(String expectedErrorMessage) {
            this.expectedErrorMessage = expectedErrorMessage;
            return this;
        }
    }
}
