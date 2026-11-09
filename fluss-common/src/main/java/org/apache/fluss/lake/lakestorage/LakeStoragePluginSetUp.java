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

import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.security.CodeSource;
import java.util.Iterator;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Encapsulates everything needed for the instantiation and configuration of a {@link
 * LakeStoragePlugin}.
 */
public class LakeStoragePluginSetUp {

    public static LakeStoragePlugin fromDataLakeFormat(
            final String dataLakeFormat, @Nullable final PluginManager pluginManager) {
        // now, load lake storage plugin
        Iterator<LakeStoragePlugin> lakeStoragePluginIterator =
                getAllLakeStoragePlugins(pluginManager);

        LakeStoragePlugin matchingPlugin = null;
        while (lakeStoragePluginIterator.hasNext()) {
            LakeStoragePlugin lakeStoragePlugin = lakeStoragePluginIterator.next();
            if (Objects.equals(lakeStoragePlugin.identifier(), dataLakeFormat)) {
                if (matchingPlugin != null) {
                    throw new IllegalConfigurationException(
                            "Multiple LakeStoragePlugins for datalake format '"
                                    + dataLakeFormat
                                    + "' found: "
                                    + describe(matchingPlugin)
                                    + ", "
                                    + describe(lakeStoragePlugin)
                                    + ". Remove one of them from the plugins directory"
                                    + " or the classpath.");
                }
                matchingPlugin = lakeStoragePlugin;
            }
        }

        if (matchingPlugin != null) {
            return PluginLakeStorageWrapper.of(matchingPlugin);
        }

        // if come here, means we haven't found LakeStoragePlugin match the configured
        // datalake, throw exception
        throw new UnsupportedOperationException(
                "No LakeStoragePlugin can be found for datalake format: " + dataLakeFormat);
    }

    /**
     * The same class can be found twice, e.g. when its jar is both in the plugins directory and on
     * the classpath, so the location is needed to tell the duplicates apart.
     */
    private static String describe(LakeStoragePlugin plugin) {
        Class<?> pluginClass = plugin.getClass();
        CodeSource codeSource = pluginClass.getProtectionDomain().getCodeSource();
        String location =
                codeSource == null || codeSource.getLocation() == null
                        ? "unknown location"
                        : codeSource.getLocation().toString();
        return pluginClass.getName() + " (" + location + ")";
    }

    private static Iterator<LakeStoragePlugin> getAllLakeStoragePlugins(
            @Nullable PluginManager pluginManager) {
        final Iterator<LakeStoragePlugin> pluginIteratorSPI =
                ServiceLoader.load(
                                LakeStoragePlugin.class, LakeStoragePlugin.class.getClassLoader())
                        .iterator();
        if (pluginManager == null) {
            return pluginIteratorSPI;
        } else {
            return Iterators.concat(pluginManager.load(LakeStoragePlugin.class), pluginIteratorSPI);
        }
    }
}
