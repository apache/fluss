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

import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link org.apache.fluss.metadata.Schema}. */
class TableSchemaTest {

    @Test
    void testAutoIncrementColumnSchema() {
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .primaryKey("f0")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Multiple primary keys are not supported.");

        assertThat(
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.BIGINT())
                                .column("f3", DataTypes.STRING())
                                .enableAutoIncrement("f1")
                                .primaryKey("f0")
                                .build()
                                .getAutoIncrementColumnNames())
                .isEqualTo(Collections.singletonList("f1"));
        assertThat(
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.BIGINT())
                                .column("f3", DataTypes.STRING())
                                .primaryKey("f0")
                                .build()
                                .getAutoIncrementColumnNames())
                .isEmpty();

        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f0")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Auto increment column can not be used as the primary key.");

        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f1")
                                        .enableAutoIncrement("f1")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Multiple auto increment columns are not supported yet.");
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f3")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The data type of auto increment column must be INT or BIGINT.");
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f4")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Auto increment column f4 does not exist in table columns [f0, f1, f3].");
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f1")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Auto increment column can only be used in primary-key table.");
    }
}
