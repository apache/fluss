/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** The function of {@link CatalogTable} that creates an instance of {@link CatalogTable}. */
public interface CatalogTableFunction {

    /**
     * Creates a basic implementation of this interface.
     *
     * <p>The signature is similar to a SQL {@code CREATE TABLE} statement.
     *
     * @param schema unresolved schema
     * @param comment optional comment
     * @param partitionKeys list of partition keys or an empty list if not partitioned
     * @param options options to configure the connector
     * @deprecated Use the builder {@link CatalogTable#newBuilder()} instead.
     */
    CatalogTable apply(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options);
}
