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

import org.apache.flink.table.catalog.TableChange;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_DEFINITION_QUERY;

/**
 * A Flink 2.x override of {@link TableChangeAdapter} that converts the {@link TableChange}s
 * introduced since Flink 2.0.
 *
 * <p>To support a new version-specific table change, add a branch to {@link #convert}.
 *
 * <p>TODO: remove this class when no longer support all the Flink 1.x series.
 */
public class TableChangeAdapter {

    private TableChangeAdapter() {}

    /**
     * Converts a version-specific Flink {@link TableChange} into a list of Fluss {@link
     * org.apache.fluss.metadata.TableChange}.
     *
     * @return the converted Fluss table changes, or {@link Optional#empty()} if the given change is
     *     not a version-specific table change handled by this adapter (the caller should then treat
     *     it as unsupported).
     */
    public static Optional<List<org.apache.fluss.metadata.TableChange>> convert(
            TableChange tableChange) {
        if (tableChange instanceof TableChange.ModifyDefinitionQuery) {
            String definitionQuery =
                    ((TableChange.ModifyDefinitionQuery) tableChange).getDefinitionQuery();
            return Optional.of(
                    Collections.singletonList(
                            org.apache.fluss.metadata.TableChange.set(
                                    MATERIALIZED_TABLE_DEFINITION_QUERY.key(), definitionQuery)));
        }
        return Optional.empty();
    }
}
