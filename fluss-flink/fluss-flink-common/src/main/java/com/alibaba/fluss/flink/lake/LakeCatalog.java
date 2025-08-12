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

package com.alibaba.fluss.flink.lake;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkFileIOLoader;
import org.apache.paimon.options.Options;

import java.util.Map;

/** A lake catalog to delegate the operations on lake table. */
public class LakeCatalog {

    // currently, only support paimon
    // todo make it pluggable
    private final FlinkCatalog paimonFlinkCatalog;

    public LakeCatalog(
            String catalogName, Map<String, String> catalogProperties, ClassLoader classLoader) {
        CatalogContext catalogContext =
                CatalogContext.create(
                        Options.fromMap(catalogProperties), null, new FlinkFileIOLoader());
        paimonFlinkCatalog =
                FlinkCatalogFactory.createCatalog(catalogName, catalogContext, classLoader);
    }

    public CatalogBaseTable getTable(ObjectPath objectPath)
            throws TableNotExistException, CatalogException {
        return paimonFlinkCatalog.getTable(objectPath);
    }
}
