/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.testutils.classloader;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Utility class for creating and managing custom class loaders in tests, particularly for testing
 * class loading scenarios in Fluss plugins and connectors.
 */
public class ClassLoaderTestUtils {

    /** A custom exception to simulate class loading failures. */
    public static class ClassLoadingException extends Exception {
        public ClassLoadingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Creates a class loader with isolated classpath for testing plugin isolation.
     *
     * @param classpathElements The classpath elements to include in the isolated class loader
     * @return A new URLClassLoader with the specified classpath
     */
    public static URLClassLoader createIsolatedClassLoader(String... classpathElements) {
        List<URL> urls = new ArrayList<>();
        for (String element : classpathElements) {
            try {
                urls.add(Paths.get(element).toUri().toURL());
            } catch (MalformedURLException e) {
                throw new RuntimeException("Invalid classpath element: " + element, e);
            }
        }
        return new URLClassLoader(urls.toArray(new URL[0]), null);
    }

    /**
     * Loads a plugin with complete classpath isolation.
     *
     * @param pluginClass The fully qualified name of the plugin class to load
     * @param pluginJarPath The path to the plugin JAR file
     * @return The loaded plugin class
     * @throws ClassNotFoundException If the plugin class cannot be found
     * @throws IOException If there's an error reading the JAR file
     */
    public static Class<?> loadPluginWithIsolation(String pluginClass, String pluginJarPath)
            throws ClassNotFoundException, IOException {
        URLClassLoader isolatedClassLoader = createIsolatedClassLoader(pluginJarPath);
        return isolatedClassLoader.loadClass(pluginClass);
    }

    /**
     * Simulates class loading conflicts between different versions of the same library.
     *
     * @param className The class name to load
     * @param version1JarPath Path to the first version of the JAR
     * @param version2JarPath Path to the second version of the JAR
     * @return An array containing the classes loaded from each version
     * @throws ClassNotFoundException If the class cannot be found in either JAR
     * @throws IOException If there's an error reading the JAR files
     */
    public static Class<?>[] simulateVersionConflict(
            String className, String version1JarPath, String version2JarPath)
            throws ClassNotFoundException, IOException {
        ClassLoader version1ClassLoader = createIsolatedClassLoader(version1JarPath);
        ClassLoader version2ClassLoader = createIsolatedClassLoader(version2JarPath);

        Class<?> classVersion1 = version1ClassLoader.loadClass(className);
        Class<?> classVersion2 = version2ClassLoader.loadClass(className);

        return new Class<?>[] {classVersion1, classVersion2};
    }

    /**
     * Verifies that class loaders are properly isolated by checking that classes loaded from
     * different class loaders are not the same.
     *
     * @param class1 A class loaded from the first class loader
     * @param class2 A class loaded from the second class loader
     * @return true if the classes are properly isolated (different), false otherwise
     */
    public static boolean verifyClassLoaderIsolation(Class<?> class1, Class<?> class2) {
        return class1 != class2 && !class1.equals(class2);
    }

    /**
     * Gets the classpath elements for a specific module.
     *
     * @param moduleName The name of the module
     * @return An array of classpath elements for the module
     */
    public static String[] getModuleClasspath(String moduleName) {
        // In a real implementation, this would dynamically determine the classpath
        // For now, we'll return a placeholder
        return new String[] {moduleName + "/target/classes"};
    }

    /**
     * Creates a temporary directory for testing class loading scenarios.
     *
     * @return The path to the temporary directory
     * @throws IOException If the directory cannot be created
     */
    public static Path createTempTestDir() throws IOException {
        return Files.createTempDirectory("fluss-classloader-test");
    }

    /**
     * Cleans up temporary test directories.
     *
     * @param tempDir The temporary directory to delete
     * @throws IOException If the directory cannot be deleted
     */
    public static void cleanupTempTestDir(Path tempDir) throws IOException {
        if (Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(
                            path -> {
                                try {
                                    Files.delete(path);
                                } catch (IOException e) {
                                    throw new RuntimeException("Failed to delete: " + path, e);
                                }
                            });
        }
    }

    /**
     * Gets all JAR files in a directory.
     *
     * @param directory The directory to search
     * @return A list of paths to JAR files in the directory
     */
    public static List<Path> getJarFilesInDirectory(String directory) {
        try {
            return Files.walk(Paths.get(directory))
                    .filter(path -> path.toString().endsWith(".jar"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list JAR files in directory: " + directory, e);
        }
    }

    /**
     * Creates a class loader that excludes certain packages from being loaded, delegating them to
     * the parent class loader instead.
     *
     * @param excludedPackages Packages to exclude from this class loader
     * @param classpathElements The classpath elements to include
     * @return A new URLClassLoader with package exclusions
     */
    public static URLClassLoader createClassLoaderWithExclusions(
            String[] excludedPackages, String... classpathElements) {
        URL[] urls =
                Arrays.stream(classpathElements)
                        .map(
                                element -> {
                                    try {
                                        return Paths.get(element).toUri().toURL();
                                    } catch (MalformedURLException e) {
                                        throw new RuntimeException(
                                                "Invalid classpath element: " + element, e);
                                    }
                                })
                        .toArray(URL[]::new);

        return new URLClassLoader(urls, ClassLoader.getSystemClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                    throws ClassNotFoundException {
                // Check if the class belongs to an excluded package
                for (String excludedPackage : excludedPackages) {
                    if (name.startsWith(excludedPackage)) {
                        // Delegate to parent class loader
                        return getParent().loadClass(name);
                    }
                }
                // Load normally
                return super.loadClass(name, resolve);
            }
        };
    }

    /**
     * Simulates a class loading failure scenario.
     *
     * @param className The class name that should fail to load
     * @param classLoader The class loader to use
     * @return A supplier that when called will attempt to load the class and throw
     *     ClassLoadingException on failure
     */
    public static Supplier<Class<?>> simulateClassLoadingFailure(
            String className, ClassLoader classLoader) {
        return () -> {
            try {
                return classLoader.loadClass(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to load class: " + className, e);
            }
        };
    }

    /**
     * Tests concurrent class loading scenarios.
     *
     * @param className The class name to load concurrently
     * @param classLoaders Multiple class loaders to use for concurrent loading
     * @return An array of loaded classes
     * @throws InterruptedException If the thread is interrupted
     */
    public static Class<?>[] testConcurrentClassLoading(
            String className, ClassLoader... classLoaders) throws InterruptedException {
        Thread[] threads = new Thread[classLoaders.length];
        Class<?>[] loadedClasses = new Class<?>[classLoaders.length];
        Exception[] exceptions = new Exception[classLoaders.length];

        // Create and start threads for concurrent loading
        for (int i = 0; i < classLoaders.length; i++) {
            final int index = i;
            final ClassLoader classLoader = classLoaders[i];
            threads[i] =
                    new Thread(
                            () -> {
                                try {
                                    loadedClasses[index] = classLoader.loadClass(className);
                                } catch (Exception e) {
                                    exceptions[index] = e;
                                }
                            });
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Check for exceptions
        for (Exception exception : exceptions) {
            if (exception != null) {
                throw new RuntimeException("Concurrent class loading failed", exception);
            }
        }

        return loadedClasses;
    }

    /**
     * Verifies that class loaders properly clean up resources when closed.
     *
     * @param classLoader The class loader to test cleanup for
     * @return true if cleanup was successful, false otherwise
     */
    public static boolean verifyClassLoaderCleanup(URLClassLoader classLoader) {
        try {
            classLoader.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
