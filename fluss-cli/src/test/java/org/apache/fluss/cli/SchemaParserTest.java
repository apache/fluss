/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.cli;

import org.apache.fluss.metadata.Schema;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SchemaParser}. */
class SchemaParserTest {

    @Test
    void testSimpleColumns() {
        Schema schema = SchemaParser.parseSchema("id INT, name STRING", Collections.emptyList());

        List<Schema.Column> cols = schema.getColumns();
        assertThat(cols).hasSize(2);
        assertThat(cols.get(0).getName()).isEqualTo("id");
        assertThat(cols.get(0).getDataType().isNullable()).isTrue();
        assertThat(cols.get(1).getName()).isEqualTo("name");
        assertThat(cols.get(1).getDataType().isNullable()).isTrue();
    }

    @Test
    void testNotNullColumn() {
        Schema schema =
                SchemaParser.parseSchema("id INT NOT NULL, name STRING", Collections.emptyList());

        List<Schema.Column> cols = schema.getColumns();
        assertThat(cols.get(0).getName()).isEqualTo("id");
        assertThat(cols.get(0).getDataType().isNullable()).isFalse();
        assertThat(cols.get(1).getDataType().isNullable()).isTrue();
    }

    @Test
    void testDecimalWithCommaInParens() {
        Schema schema = SchemaParser.parseSchema("amount DECIMAL(10,2)", Collections.emptyList());

        List<Schema.Column> cols = schema.getColumns();
        assertThat(cols).hasSize(1);
        assertThat(cols.get(0).getName()).isEqualTo("amount");
        assertThat(cols.get(0).getDataType().toString()).contains("DECIMAL");
    }

    @Test
    void testMultipleNotNull() {
        Schema schema =
                SchemaParser.parseSchema(
                        "a INT NOT NULL, b STRING NOT NULL", Collections.emptyList());

        List<Schema.Column> cols = schema.getColumns();
        assertThat(cols.get(0).getDataType().isNullable()).isFalse();
        assertThat(cols.get(1).getDataType().isNullable()).isFalse();
    }

    @Test
    void testSingleColumn() {
        Schema schema = SchemaParser.parseSchema("id BIGINT", Collections.emptyList());

        assertThat(schema.getColumns()).hasSize(1);
        assertThat(schema.getColumns().get(0).getName()).isEqualTo("id");
    }

    @Test
    void testExtraWhitespace() {
        Schema schema =
                SchemaParser.parseSchema("  id   INT  ,  name   STRING  ", Collections.emptyList());

        List<Schema.Column> cols = schema.getColumns();
        assertThat(cols).hasSize(2);
        assertThat(cols.get(0).getName()).isEqualTo("id");
        assertThat(cols.get(1).getName()).isEqualTo("name");
    }

    @Test
    void testWithPrimaryKey() {
        Schema schema = SchemaParser.parseSchema("id INT, name STRING", Arrays.asList("id"));

        assertThat(schema.getPrimaryKey()).isPresent();
        assertThat(schema.getPrimaryKey().get().getColumnNames()).containsExactly("id");
    }

    @Test
    void testInvalidColumnDefinition() {
        assertThatThrownBy(() -> SchemaParser.parseSchema("nospace", Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid column definition");
    }

    @Test
    void testUnknownTypeThrows() {
        assertThatThrownBy(() -> SchemaParser.parseSchema("x FOOBAR", Collections.emptyList()))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testEmptySchemaThrows() {
        assertThatThrownBy(() -> SchemaParser.parseSchema("", Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testAllSupportedTypes() {
        String schema =
                "a BOOLEAN, b TINYINT, c SMALLINT, d INT, e BIGINT, "
                        + "f FLOAT, g DOUBLE, h STRING, i DATE, j TIMESTAMP";
        Schema result = SchemaParser.parseSchema(schema, Collections.emptyList());

        assertThat(result.getColumns()).hasSize(10);
    }
}
