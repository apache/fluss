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

package org.apache.fluss.flink.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.DefaultIndex;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;

import java.util.Arrays;
import java.util.Collections;

/** Test for {@link FlinkCatalog}. */
public class Flink22CatalogTest extends FlinkCatalogTest {

    protected ResolvedSchema createSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("first", DataTypes.STRING().notNull()),
                        Column.physical("second", DataTypes.INT()),
                        Column.physical("third", DataTypes.STRING().notNull())),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("PK_first_third", Arrays.asList("first", "third")),
                Collections.singletonList(
                        DefaultIndex.newIndex(
                                "INDEX_first_third", Arrays.asList("first", "third"))));
    }
}
