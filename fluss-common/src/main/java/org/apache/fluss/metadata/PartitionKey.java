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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** One ordered table partition key, either a physical column or a virtual expression. */
@PublicEvolving
public final class PartitionKey implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Partition key kind. */
    public enum Kind {
        COLUMN,
        EXPRESSION
    }

    private final Kind kind;
    private final String columnName;
    private final PartitionExpression expression;

    private PartitionKey(Kind kind, String columnName, PartitionExpression expression) {
        this.kind = checkNotNull(kind, "partition key kind must not be null.");
        this.columnName = columnName;
        this.expression = expression;
    }

    /** Creates a physical column partition key. */
    public static PartitionKey column(String columnName) {
        return new PartitionKey(
                Kind.COLUMN, checkNotNull(columnName, "column name must not be null."), null);
    }

    /** Creates a virtual expression partition key. */
    public static PartitionKey expression(PartitionExpression expression) {
        return new PartitionKey(
                Kind.EXPRESSION,
                null,
                checkNotNull(expression, "partition expression must not be null."));
    }

    /** Returns the partition key kind. */
    public Kind getKind() {
        return kind;
    }

    /** Returns the physical column name or resolved virtual partition spec key. */
    public Optional<String> getPartitionSpecKey() {
        if (kind == Kind.COLUMN) {
            return Optional.of(columnName);
        }
        return expression.getVirtualPartitionSpecKey();
    }

    /** Returns the physical column name for a column partition key. */
    public Optional<String> getColumnName() {
        return Optional.ofNullable(columnName);
    }

    /** Returns the expression for a virtual expression partition key. */
    public Optional<PartitionExpression> getExpression() {
        return Optional.ofNullable(expression);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionKey that = (PartitionKey) o;
        return kind == that.kind
                && Objects.equals(columnName, that.columnName)
                && Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, columnName, expression);
    }

    @Override
    public String toString() {
        return "PartitionKey{"
                + "kind="
                + kind
                + ", columnName='"
                + columnName
                + '\''
                + ", expression="
                + expression
                + '}';
    }
}
