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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link EnableFlussOnLakeTableProcedure}. */
class EnableFlussOnLakeTableProcedureTest {

    @Test
    void testParseArguments() {
        assertThat(EnableFlussOnLakeTableProcedure.parseTablePath(" db.table ", "default_db"))
                .isEqualTo(TablePath.of("db", "table"));
        assertThat(EnableFlussOnLakeTableProcedure.parseTablePath(" table ", "default_db"))
                .isEqualTo(TablePath.of("default_db", "table"));

        Map<String, String> options =
                EnableFlussOnLakeTableProcedure.parseOptions(
                        " bucket.num = 4 , table.log.format = compacted ");
        assertThat(options)
                .containsExactly(entry("bucket.num", "4"), entry("table.log.format", "compacted"));
        assertThat(EnableFlussOnLakeTableProcedure.parseOptions(" ")).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {".table", "database.", "database.table.extra", " "})
    void testRejectInvalidTablePath(String table) {
        assertThatThrownBy(
                        () -> EnableFlussOnLakeTableProcedure.parseTablePath(table, "default_db"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"key", "=value", "key=", "key=value,key=other", ",", "key=value,"})
    void testRejectInvalidOptions(String options) {
        assertThatThrownBy(() -> EnableFlussOnLakeTableProcedure.parseOptions(options))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
