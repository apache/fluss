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

package org.apache.fluss.lake.lance.source;

import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.predicate.GreaterOrEqual;
import org.apache.fluss.predicate.GreaterThan;
import org.apache.fluss.predicate.In;
import org.apache.fluss.predicate.IsNotNull;
import org.apache.fluss.predicate.IsNull;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.LessOrEqual;
import org.apache.fluss.predicate.LessThan;
import org.apache.fluss.predicate.NotEqual;
import org.apache.fluss.predicate.NotIn;
import org.apache.fluss.predicate.Or;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Converts Fluss predicate into Lance SQL filter expression. */
final class LancePredicatePushDown {

    private LancePredicatePushDown() {}

    static Optional<String> toSql(Predicate predicate) {
        if (predicate instanceof LeafPredicate) {
            return toLeafSql((LeafPredicate) predicate);
        }

        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) predicate;
            String op;
            if (compound.function() instanceof And) {
                op = "AND";
            } else if (compound.function() instanceof Or) {
                op = "OR";
            } else {
                return Optional.empty();
            }

            List<String> childSql = new ArrayList<>();
            for (Predicate child : compound.children()) {
                Optional<String> sql = toSql(child);
                if (!sql.isPresent()) {
                    return Optional.empty();
                }
                childSql.add("(" + sql.get() + ")");
            }
            if (childSql.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(String.join(" " + op + " ", childSql));
        }

        return Optional.empty();
    }

    private static Optional<String> toLeafSql(LeafPredicate predicate) {
        String column = quoteIdentifier(predicate.fieldName());

        if (predicate.function() instanceof IsNull) {
            return Optional.of(column + " IS NULL");
        }
        if (predicate.function() instanceof IsNotNull) {
            return Optional.of(column + " IS NOT NULL");
        }

        if (predicate.function() instanceof In || predicate.function() instanceof NotIn) {
            if (predicate.literals() == null || predicate.literals().isEmpty()) {
                return Optional.empty();
            }

            List<String> literals = new ArrayList<>();
            for (Object literal : predicate.literals()) {
                Optional<String> sql = literalToSql(literal);
                if (!sql.isPresent()) {
                    return Optional.empty();
                }
                literals.add(sql.get());
            }

            String op = predicate.function() instanceof In ? " IN " : " NOT IN ";
            return Optional.of(column + op + "(" + String.join(", ", literals) + ")");
        }

        if (predicate.literals() == null || predicate.literals().size() != 1) {
            return Optional.empty();
        }
        Optional<String> literal = literalToSql(predicate.literals().get(0));
        if (!literal.isPresent()) {
            return Optional.empty();
        }

        if (predicate.function() instanceof Equal) {
            return Optional.of(column + " = " + literal.get());
        }
        if (predicate.function() instanceof NotEqual) {
            return Optional.of(column + " <> " + literal.get());
        }
        if (predicate.function() instanceof GreaterThan) {
            return Optional.of(column + " > " + literal.get());
        }
        if (predicate.function() instanceof GreaterOrEqual) {
            return Optional.of(column + " >= " + literal.get());
        }
        if (predicate.function() instanceof LessThan) {
            return Optional.of(column + " < " + literal.get());
        }
        if (predicate.function() instanceof LessOrEqual) {
            return Optional.of(column + " <= " + literal.get());
        }

        return Optional.empty();
    }

    private static String quoteIdentifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static String quoteString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static Optional<String> literalToSql(Object literal) {
        if (literal == null) {
            return Optional.of("NULL");
        }

        if (literal instanceof String) {
            return Optional.of(quoteString((String) literal));
        }
        if (literal instanceof BinaryString) {
            return Optional.of(quoteString(((BinaryString) literal).toString()));
        }
        if (literal instanceof Boolean) {
            return Optional.of((Boolean) literal ? "TRUE" : "FALSE");
        }
        if (literal instanceof Byte
                || literal instanceof Short
                || literal instanceof Integer
                || literal instanceof Long
                || literal instanceof Float
                || literal instanceof Double) {
            return Optional.of(String.valueOf(literal));
        }
        if (literal instanceof BigDecimal) {
            return Optional.of(((BigDecimal) literal).toPlainString());
        }
        if (literal instanceof Decimal) {
            return Optional.of(((Decimal) literal).toBigDecimal().toPlainString());
        }
        if (literal instanceof LocalDate) {
            return Optional.of(quoteString(literal.toString()));
        }
        if (literal instanceof LocalDateTime) {
            return Optional.of(quoteString(literal.toString()));
        }
        if (literal instanceof OffsetDateTime) {
            return Optional.of(quoteString(literal.toString()));
        }
        if (literal instanceof Instant) {
            return Optional.of(quoteString(((Instant) literal).atOffset(ZoneOffset.UTC).toString()));
        }
        if (literal instanceof TimestampNtz) {
            return Optional.of(quoteString(((TimestampNtz) literal).toLocalDateTime().toString()));
        }
        if (literal instanceof TimestampLtz) {
            return Optional.of(quoteString(((TimestampLtz) literal).toInstant().toString()));
        }

        return Optional.empty();
    }
}
