/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataTypeRoot;

import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

/** Lossless conversion of single, non-null Trino predicate values to Fluss native values. */
final class FlussPredicateValues {
    private FlussPredicateValues() {}

    /**
     * Converts only verified Trino representations. Unsupported types do not participate in
     * pruning.
     */
    static Optional<Object> toFlussValue(DataTypeRoot type, Domain domain) {
        if (!domain.isSingleValue()) {
            return Optional.empty();
        }
        Type trinoType = domain.getType();
        Object value = domain.getSingleValue();

        switch (type) {
            case BOOLEAN:
                return trinoType.equals(BOOLEAN) && value instanceof Boolean
                        ? Optional.of(value)
                        : Optional.empty();
            case TINYINT:
                return narrowIntegral(trinoType, TINYINT, value, Byte.MIN_VALUE, Byte.MAX_VALUE)
                        .map(number -> (Object) number.byteValue());
            case SMALLINT:
                return narrowIntegral(trinoType, SMALLINT, value, Short.MIN_VALUE, Short.MAX_VALUE)
                        .map(number -> (Object) number.shortValue());
            case INTEGER:
                return narrowIntegral(
                                trinoType, INTEGER, value, Integer.MIN_VALUE, Integer.MAX_VALUE)
                        .map(number -> (Object) number.intValue());
            case DATE:
                return narrowIntegral(trinoType, DATE, value, Integer.MIN_VALUE, Integer.MAX_VALUE)
                        .map(number -> (Object) number.intValue());
            case BIGINT:
                return trinoType.equals(BIGINT) && value instanceof Long
                        ? Optional.of(value)
                        : Optional.empty();
            case STRING:
                if (!trinoType.equals(VARCHAR) || !(value instanceof Slice)) {
                    return Optional.empty();
                }
                try {
                    String string =
                            StandardCharsets.UTF_8
                                    .newDecoder()
                                    .onMalformedInput(CodingErrorAction.REPORT)
                                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                                    .decode(ByteBuffer.wrap(((Slice) value).getBytes()))
                                    .toString();

                    return Optional.of(BinaryString.fromString(string));
                } catch (CharacterCodingException e) {
                    return Optional.empty();
                }
            case BYTES:
                return trinoType.equals(VARBINARY) && value instanceof Slice
                        ? Optional.of(((Slice) value).getBytes())
                        : Optional.empty();
            default:
                /*
                 * CHAR, BINARY, DECIMAL, FLOAT and temporal
                 * types need additional encoding-semantic
                 * tests before pruning can be enabled.
                 */
                return Optional.empty();
        }
    }

    private static Optional<Long> narrowIntegral(
            Type actual, Type expected, Object value, long min, long max) {
        if (!actual.equals(expected) || !(value instanceof Long)) {
            return Optional.empty();
        }
        long number = (Long) value;
        return number >= min && number <= max ? Optional.of(number) : Optional.empty();
    }
}
