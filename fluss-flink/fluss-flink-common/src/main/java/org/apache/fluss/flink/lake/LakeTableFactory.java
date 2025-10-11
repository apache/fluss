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

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

/** A factory to create {@link DynamicTableSource} for lake table. */
public class LakeTableFactory {

    private final Object delegateFactory;
    private final DataLakeFormat lakeFormat;

    public LakeTableFactory(DataLakeFormat lakeFormat, ClassLoader classLoader) {
        this.lakeFormat = lakeFormat;

        if (lakeFormat == DataLakeFormat.PAIMON) {
            this.delegateFactory = createPaimonFlinkTableFactory(classLoader);
        } else if (lakeFormat == DataLakeFormat.ICEBERG) {
            this.delegateFactory = createIcebergFlinkTableFactory(classLoader);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported lake format: "
                            + lakeFormat
                            + ". Only PAIMON and ICEBERG are supported.");
        }
    }

    private Object createPaimonFlinkTableFactory(ClassLoader classLoader) {
        try {
            Class<?> paimonFactoryClass =
                    classLoader.loadClass("org.apache.paimon.flink.FlinkTableFactory");
            return paimonFactoryClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create Paimon FlinkTableFactory. Make sure paimon-flink is on the classpath.",
                    e);
        }
    }

    private Object createIcebergFlinkTableFactory(ClassLoader classLoader) {
        try {
            Class<?> icebergFactoryClass =
                    classLoader.loadClass("org.apache.iceberg.flink.FlinkDynamicTableFactory");
            return icebergFactoryClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create Iceberg FlinkDynamicTableFactory. Make sure iceberg-flink is on the classpath.",
                    e);
        }
    }

    public DynamicTableSource createDynamicTableSource(
            DynamicTableFactory.Context context, String tableName) {
        ObjectIdentifier originIdentifier = context.getObjectIdentifier();
        ObjectIdentifier lakeIdentifier =
                ObjectIdentifier.of(
                        originIdentifier.getCatalogName(),
                        originIdentifier.getDatabaseName(),
                        tableName);
        DynamicTableFactory.Context newContext =
                new FactoryUtil.DefaultDynamicTableContext(
                        lakeIdentifier,
                        context.getCatalogTable(),
                        context.getEnrichmentOptions(),
                        context.getConfiguration(),
                        context.getClassLoader(),
                        context.isTemporary());

        try {
            // Use reflection to call createDynamicTableSource on the delegate factory
            java.lang.reflect.Method createMethod =
                    delegateFactory
                            .getClass()
                            .getMethod(
                                    "createDynamicTableSource", DynamicTableFactory.Context.class);
            return (DynamicTableSource) createMethod.invoke(delegateFactory, newContext);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create DynamicTableSource for lake format: " + lakeFormat, e);
        }
    }
}
