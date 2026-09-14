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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.plugin.PluginManager;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Encapsulates everything needed for the instantiation and configuration of a {@link
 * LakeStoragePlugin}.
 */
public class LakeStoragePluginSetUp {

    /**
     * Finds the {@link LakeStoragePlugin} for the given datalake format, from the plugins directory
     * or from the main classpath.
     *
     * <p>Providing it from both cannot work: {@code org.apache.fluss.} is parent-first, so the copy
     * on the main classpath shadows the plugin and is loaded without the dependencies bundled next
     * to it. That is rejected here rather than failing later with a {@link NoClassDefFoundError}.
     */
    public static LakeStoragePlugin fromDataLakeFormat(
            final String dataLakeFormat, @Nullable final PluginManager pluginManager) {
        LakeStoragePlugin fromPluginsDir =
                findByIdentifier(loadFromPluginManager(pluginManager), dataLakeFormat);
        LakeStoragePlugin fromClasspath = findByIdentifier(loadFromClasspath(), dataLakeFormat);

        if (fromPluginsDir != null && fromClasspath != null) {
            throw new FlussRuntimeException(
                    String.format(
                            "Found two LakeStoragePlugin for datalake format '%s': one in the plugins "
                                    + "directory and one on the main classpath (usually <FLUSS_HOME>/lib). "
                                    + "The copy on the main classpath shadows the one in the plugins "
                                    + "directory, but is loaded without the dependencies bundled next to "
                                    + "the plugin, such as Hadoop. This later fails with errors like "
                                    + "'NoClassDefFoundError: org/apache/hadoop/hdfs/HdfsConfiguration'. "
                                    + "Remove the fluss-lake-%s jar from <FLUSS_HOME>/lib and keep it only "
                                    + "in <FLUSS_HOME>/plugins/%s/.",
                            dataLakeFormat, dataLakeFormat, dataLakeFormat));
        }

        LakeStoragePlugin lakeStoragePlugin =
                fromPluginsDir != null ? fromPluginsDir : fromClasspath;
        if (lakeStoragePlugin == null) {
            throw new UnsupportedOperationException(
                    "No LakeStoragePlugin can be found for datalake format: " + dataLakeFormat);
        }
        return PluginLakeStorageWrapper.of(lakeStoragePlugin);
    }

    @Nullable
    private static LakeStoragePlugin findByIdentifier(
            Iterator<LakeStoragePlugin> lakeStoragePlugins, String dataLakeFormat) {
        while (lakeStoragePlugins.hasNext()) {
            LakeStoragePlugin lakeStoragePlugin = lakeStoragePlugins.next();
            if (Objects.equals(lakeStoragePlugin.identifier(), dataLakeFormat)) {
                return lakeStoragePlugin;
            }
        }
        return null;
    }

    private static Iterator<LakeStoragePlugin> loadFromPluginManager(
            @Nullable PluginManager pluginManager) {
        if (pluginManager == null) {
            return Collections.emptyIterator();
        }
        return pluginManager.load(LakeStoragePlugin.class);
    }

    private static Iterator<LakeStoragePlugin> loadFromClasspath() {
        return ServiceLoader.load(LakeStoragePlugin.class, LakeStoragePlugin.class.getClassLoader())
                .iterator();
    }
}
