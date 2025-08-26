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

import org.apache.fluss.exception.InvalidDatabaseException;
import org.apache.fluss.exception.InvalidTableException;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link org.apache.fluss.metadata.TablePath}. */
class TablePathTest {

    @Test
    void testValidate() {
        // assert valid name
        TablePath path = TablePath.of("db_2-abc3", "table-1_abc_2");
        path.validate();
        assertThat(path.isValid()).isTrue();
        assertThat(path.toString()).isEqualTo("db_2-abc3.table-1_abc_2");

        // check max length
        String longName = StringUtils.repeat("a", 200);
        assertThat(TablePath.of(longName, longName).isValid()).isTrue();

        // assert invalid names
        assertInvalidName("*abc", "'*abc' contains one or more characters other than");
        assertInvalidName("table.abc", "'table.abc' contains one or more characters other than");
        assertInvalidName(null, "null string is not allowed");
        assertInvalidName("", "the empty string is not allowed");
        assertInvalidName(" ", "' ' contains one or more characters other than");
        assertInvalidName(".", "'.' is not allowed");
        assertInvalidName("..", "'..' is not allowed");
        assertInvalidName("..", "'..' is not allowed");
        String invalidLongName = StringUtils.repeat("a", 201);
        assertInvalidName(
                invalidLongName,
                "the length of '"
                        + invalidLongName
                        + "' is longer than the max allowed length 200");
    }

    private static void assertInvalidName(String name, String expectedMessage) {
        TablePath invalidTable = TablePath.of("db", name);
        assertThat(invalidTable.isValid()).isFalse();
        assertThatThrownBy(invalidTable::validate)
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Table name " + name + " is invalid: " + expectedMessage);

        TablePath invalidDb = TablePath.of(name, "table");
        assertThat(invalidDb.isValid()).isFalse();
        assertThatThrownBy(invalidDb::validate)
                .isInstanceOf(InvalidDatabaseException.class)
                .hasMessageContaining("Database name " + name + " is invalid: " + expectedMessage);
    }
}
