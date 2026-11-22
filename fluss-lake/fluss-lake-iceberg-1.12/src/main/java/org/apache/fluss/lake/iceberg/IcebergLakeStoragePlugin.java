/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;

import org.apache.iceberg.formats.FormatModelRegistry;

/**
 * Iceberg 1.12 implementation of {@link LakeStoragePlugin}. It replaces the class of the same name
 * in fluss-lake-iceberg-common.
 */
public class IcebergLakeStoragePlugin implements LakeStoragePlugin {

    private static final String IDENTIFIER = "iceberg";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public LakeStorage createLakeStorage(Configuration configuration) {
        Iceberg112RuntimeCheck.checkJavaVersion();
        initFormatModelRegistry();
        return new IcebergLakeStorage(configuration);
    }

    /**
     * {@link FormatModelRegistry} registers the Parquet, ORC and Avro format models with the thread
     * context classloader when it is initialized. Initializes it with the plugin classloader, which
     * holds the format models, instead of the classloader of the caller.
     */
    private static void initFormatModelRegistry() {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        ClassLoader pluginClassLoader = IcebergLakeStoragePlugin.class.getClassLoader();
        thread.setContextClassLoader(pluginClassLoader);
        try {
            Class.forName(FormatModelRegistry.class.getName(), true, pluginClassLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to initialize the Iceberg format models.", e);
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
