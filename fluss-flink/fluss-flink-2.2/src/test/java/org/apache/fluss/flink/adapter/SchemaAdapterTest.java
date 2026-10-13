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

package org.apache.fluss.flink.adapter;

import org.apache.fluss.testutils.common.MultiVersionTest;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SchemaAdapter}. */
@MultiVersionTest
public class SchemaAdapterTest {

    @Test
    public void testDeduplicateIndexesWithSameColumns() {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();

        Schema schemaWithIndexes =
                SchemaAdapter.withIndex(
                        schema,
                        Arrays.asList(
                                Arrays.asList("a", "b"),
                                Collections.singletonList("a"),
                                Collections.singletonList("a"),
                                Arrays.asList("b", "a")));

        List<List<String>> indexColumns =
                schemaWithIndexes.getIndexes().stream()
                        .map(Schema.UnresolvedIndex::getColumnNames)
                        .collect(Collectors.toList());
        assertThat(indexColumns)
                .containsExactly(
                        Arrays.asList("a", "b"),
                        Collections.singletonList("a"),
                        Arrays.asList("b", "a"));
    }
}
