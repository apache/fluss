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

package org.apache.fluss.flink.lake;

import org.apache.fluss.metadata.DataLakeFormat;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/** A lake catalog to delegate the operations on lake table. */
public class LakeCatalog {

    private final Catalog delegateCatalog;
    private final DataLakeFormat lakeFormat;

    public LakeCatalog(
            String catalogName,
            Map<String, String> catalogProperties,
            ClassLoader classLoader,
            DataLakeFormat lakeFormat) {
        this.lakeFormat = lakeFormat;

        if (lakeFormat == DataLakeFormat.PAIMON) {
            this.delegateCatalog =
                    createPaimonFlinkCatalog(catalogName, catalogProperties, classLoader);
        } else if (lakeFormat == DataLakeFormat.ICEBERG) {
            this.delegateCatalog =
                    createIcebergFlinkCatalog(catalogName, catalogProperties, classLoader);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported lake format: "
                            + lakeFormat
                            + ". Only PAIMON and ICEBERG are supported.");
        }
    }

    private Catalog createPaimonFlinkCatalog(
            String catalogName, Map<String, String> catalogProperties, ClassLoader classLoader) {
        try {
            // Use Paimon's Flink catalog factory via reflection to avoid hard dependency
            Class<?> catalogContextClass =
                    classLoader.loadClass("org.apache.paimon.catalog.CatalogContext");
            Class<?> optionsClass = classLoader.loadClass("org.apache.paimon.options.Options");
            Class<?> fileIOLoaderClass =
                    classLoader.loadClass("org.apache.paimon.flink.FlinkFileIOLoader");
            Class<?> flinkCatalogFactoryClass =
                    classLoader.loadClass("org.apache.paimon.flink.FlinkCatalogFactory");

            // Create Options.fromMap(catalogProperties)
            Method fromMapMethod = optionsClass.getMethod("fromMap", Map.class);
            Object options = fromMapMethod.invoke(null, catalogProperties);

            // Create new FlinkFileIOLoader()
            Object fileIOLoader = fileIOLoaderClass.getDeclaredConstructor().newInstance();

            // Create CatalogContext.create(options, null, fileIOLoader)
            // CatalogContext.create signature: create(Options, Configuration, FileIOLoader)
            Class<?> hadoopConfigClass =
                    classLoader.loadClass("org.apache.hadoop.conf.Configuration");
            Method createMethod =
                    catalogContextClass.getMethod(
                            "create", optionsClass, hadoopConfigClass, fileIOLoaderClass);
            Object catalogContext = createMethod.invoke(null, options, null, fileIOLoader);

            // Call FlinkCatalogFactory.createCatalog(catalogName, catalogContext, classLoader)
            Method createCatalogMethod =
                    flinkCatalogFactoryClass.getMethod(
                            "createCatalog", String.class, catalogContextClass, ClassLoader.class);
            return (Catalog)
                    createCatalogMethod.invoke(null, catalogName, catalogContext, classLoader);
        } catch (Exception e) {
            throw new CatalogException(
                    "Failed to create Paimon Flink catalog. Make sure paimon-flink is on the classpath.",
                    e);
        }
    }

    public CatalogBaseTable getTable(ObjectPath objectPath)
            throws TableNotExistException, CatalogException {
        return delegateCatalog.getTable(objectPath);
    }

    private Catalog createIcebergFlinkCatalog(
            String catalogName, Map<String, String> catalogProperties, ClassLoader classLoader) {
        try {
            // Use Iceberg's Flink catalog factory via reflection to avoid hard dependency
            Class<?> icebergCatalogFactoryClass =
                    classLoader.loadClass("org.apache.iceberg.flink.FlinkCatalogFactory");
            Object factoryInstance =
                    icebergCatalogFactoryClass.getDeclaredConstructor().newInstance();
            Method createCatalogMethod =
                    icebergCatalogFactoryClass.getMethod("createCatalog", String.class, Map.class);

            // Prepare Iceberg catalog properties
            // Iceberg expects 'catalog-type' instead of 'type' for the catalog implementation
            Map<String, String> icebergProps = new java.util.HashMap<>(catalogProperties);
            icebergProps.putIfAbsent("type", "iceberg");

            // Map 'type' to 'catalog-type' if present (e.g., hadoop, hive, rest)
            if (catalogProperties.containsKey("type")) {
                icebergProps.put("catalog-type", catalogProperties.get("type"));
            }

            return (Catalog) createCatalogMethod.invoke(factoryInstance, catalogName, icebergProps);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            throw new CatalogException(
                    "Failed to create Iceberg Flink catalog: "
                            + (cause != null ? cause.getMessage() : e.getMessage()),
                    cause != null ? cause : e);
        } catch (Exception e) {
            throw new CatalogException(
                    "Failed to create Iceberg Flink catalog. Make sure iceberg-flink-runtime is on the classpath. Error: "
                            + e.getMessage(),
                    e);
        }
    }
}
