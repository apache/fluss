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

package org.apache.fluss.e2e.utils;

import org.apache.fluss.plugin.PluginClassLoader;
import org.apache.fluss.server.utils.ComponentClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Inspector utility for analyzing class loader hierarchies and detecting isolation issues.
 *
 * <p>This utility helps verify that:
 *
 * <ul>
 *   <li>Plugin classes are loaded by {@link PluginClassLoader}
 *   <li>Component classes are loaded by {@link ComponentClassLoader}
 *   <li>Core classes are loaded by the appropriate parent class loader
 *   <li>Class loader isolation is properly maintained
 * </ul>
 */
public class ClassLoaderInspector {

    private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderInspector.class);

    /**
     * Verifies that a class is loaded by the expected class loader type.
     *
     * @param clazz the class to check
     * @param expectedLoaderType the expected class loader type
     * @return true if the class is loaded by the expected type
     */
    public static boolean isLoadedBy(Class<?> clazz, Class<? extends ClassLoader> expectedLoaderType) {
        ClassLoader loader = clazz.getClassLoader();
        if (loader == null) {
            // Bootstrap class loader
            return expectedLoaderType == null;
        }
        return expectedLoaderType.isInstance(loader);
    }

    /**
     * Verifies that a class is loaded by a plugin class loader.
     *
     * @param clazz the class to check
     * @return true if loaded by PluginClassLoader
     */
    public static boolean isLoadedByPluginClassLoader(Class<?> clazz) {
        return isLoadedBy(clazz, PluginClassLoader.class);
    }

    /**
     * Verifies that a class is loaded by a component class loader.
     *
     * @param clazz the class to check
     * @return true if loaded by ComponentClassLoader
     */
    public static boolean isLoadedByComponentClassLoader(Class<?> clazz) {
        return isLoadedBy(clazz, ComponentClassLoader.class);
    }

    /**
     * Gets the complete class loader hierarchy for a class.
     *
     * @param clazz the class to inspect
     * @return list of class loaders from the class's loader to the root
     */
    public static List<ClassLoader> getClassLoaderHierarchy(Class<?> clazz) {
        List<ClassLoader> hierarchy = new ArrayList<>();
        ClassLoader loader = clazz.getClassLoader();
        while (loader != null) {
            hierarchy.add(loader);
            loader = loader.getParent();
        }
        return hierarchy;
    }

    /**
     * Gets all URLs accessible by a class loader.
     *
     * @param classLoader the class loader to inspect
     * @return set of URLs, or empty set if not a URLClassLoader
     */
    public static Set<URL> getClassLoaderURLs(ClassLoader classLoader) {
        Set<URL> urls = new HashSet<>();
        if (classLoader instanceof URLClassLoader) {
            urls.addAll(Arrays.asList(((URLClassLoader) classLoader).getURLs()));
        }
        return urls;
    }

    /**
     * Detects potential class loader conflicts by checking if a class name is loadable from
     * multiple class loaders in the hierarchy.
     *
     * @param className the fully qualified class name to check
     * @param rootLoader the root class loader to start from
     * @return map of class loaders that can load the class
     */
    public static Map<ClassLoader, Class<?>> detectConflicts(
            String className, ClassLoader rootLoader) {
        Map<ClassLoader, Class<?>> loaders = new HashMap<>();
        ClassLoader current = rootLoader;

        while (current != null) {
            try {
                Class<?> clazz = Class.forName(className, false, current);
                loaders.put(current, clazz);
            } catch (ClassNotFoundException e) {
                // Class not loadable from this loader
            }
            current = current.getParent();
        }

        return loaders;
    }

    /**
     * Verifies class loader isolation by ensuring that plugin/component specific classes are not
     * accessible from parent class loaders.
     *
     * @param isolatedLoader the isolated class loader (plugin or component)
     * @param isolatedClasses classes that should only be loadable from the isolated loader
     * @return list of violation messages, empty if no violations
     */
    public static List<String> verifyIsolation(
            ClassLoader isolatedLoader, List<String> isolatedClasses) {
        List<String> violations = new ArrayList<>();

        for (String className : isolatedClasses) {
            Map<ClassLoader, Class<?>> loaders = detectConflicts(className, isolatedLoader);

            if (loaders.size() > 1) {
                violations.add(
                        String.format(
                                "Class %s is loadable from multiple class loaders: %s",
                                className, loaders.keySet()));
            }

            // Verify the class is loaded by the isolated loader
            Class<?> clazz = loaders.get(isolatedLoader);
            if (clazz == null) {
                violations.add(
                        String.format(
                                "Class %s is not loadable from the isolated loader %s",
                                className, isolatedLoader));
            }
        }

        return violations;
    }

    /**
     * Gets detailed information about a class loader.
     *
     * @param loader the class loader to inspect
     * @return info object containing class loader details
     */
    public static ClassLoaderInfo getClassLoaderInfo(ClassLoader loader) {
        ClassLoaderInfo info = new ClassLoaderInfo();
        info.type = loader.getClass().getName();
        info.urls = getClassLoaderURLs(loader);

        // Try to get parent
        info.parent = loader.getParent();

        // For plugin class loader, try to get plugin descriptor
        if (loader instanceof PluginClassLoader) {
            info.isPlugin = true;
            info.pluginId = extractPluginId((PluginClassLoader) loader);
        }

        // For component class loader, try to get component type
        if (loader instanceof ComponentClassLoader) {
            info.isComponent = true;
        }

        return info;
    }

    @Nullable
    private static String extractPluginId(PluginClassLoader loader) {
        try {
            // Use reflection to access plugin descriptor
            Field descriptorField = PluginClassLoader.class.getDeclaredField("pluginDescriptor");
            descriptorField.setAccessible(true);
            Object descriptor = descriptorField.get(loader);
            if (descriptor != null) {
                Field idField = descriptor.getClass().getDeclaredField("pluginId");
                idField.setAccessible(true);
                return (String) idField.get(descriptor);
            }
        } catch (Exception e) {
            LOG.debug("Failed to extract plugin ID", e);
        }
        return null;
    }

    /** Information about a class loader. */
    public static class ClassLoaderInfo {
        public String type;
        public Set<URL> urls = new HashSet<>();
        public ClassLoader parent;
        public boolean isPlugin = false;
        public boolean isComponent = false;
        public String pluginId;

        @Override
        public String toString() {
            return String.format(
                    "ClassLoaderInfo{type=%s, isPlugin=%s, isComponent=%s, pluginId=%s, urlCount=%d, hasParent=%s}",
                    type, isPlugin, isComponent, pluginId, urls.size(), parent != null);
        }
    }
}
