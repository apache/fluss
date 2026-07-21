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

package org.apache.fluss.codegen;

import org.apache.fluss.utils.MapUtils;

import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Utilities to compile a generated code to a Class. */
public final class CompileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CompileUtils.class);

    /**
     * Cache of compiled classes. Janino generates a new Class Loader and a new Class file every
     * compile (guaranteeing that the class name will not be repeated). This leads to multiple tasks
     * of the same process that generate a large number of duplicate class, resulting in a large
     * number of Meta zone GC (class unloading), resulting in performance bottlenecks. So we add a
     * cache to avoid this problem.
     */
    private static final Map<ClassKey, Class<?>> COMPILED_CLASS_CACHE =
            MapUtils.newConcurrentHashMap();

    private CompileUtils() {}

    /**
     * Compiles a generated code to a Class.
     *
     * @param classLoader the ClassLoader used to load the class
     * @param name the class name
     * @param code the generated code
     * @param <T> the class type
     * @return the compiled class
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> compile(ClassLoader classLoader, String name, String code) {
        checkNotNull(classLoader, "classLoader must not be null");
        checkNotNull(name, "name must not be null");
        checkNotNull(code, "code must not be null");

        // The class name is part of the "code" and makes the string unique,
        // to prevent class leaks we don't cache the class loader directly
        // but only its hash code
        ClassKey classKey = new ClassKey(classLoader.hashCode(), code);
        return (Class<T>)
                COMPILED_CLASS_CACHE.computeIfAbsent(
                        classKey, key -> doCompile(classLoader, name, code));
    }

    private static Class<?> doCompile(ClassLoader classLoader, String name, String code) {
        LOG.debug("Compiling: {} \n\n Code:\n{}", name, code);
        SimpleCompiler compiler = new SimpleCompiler();
        compiler.setParentClassLoader(classLoader);
        try {
            compiler.cook(code);
        } catch (Throwable t) {
            LOG.error("Failed to compile code:\n{}", addLineNumber(code));
            throw new CodeGenException(
                    "Code generation cannot be compiled. This is a bug. Please file an issue.", t);
        }
        try {
            return compiler.getClassLoader().loadClass(name);
        } catch (ClassNotFoundException e) {
            throw new CodeGenException("Cannot load class " + name, e);
        }
    }

    /**
     * To output more information when an error occurs. Generally, when cook fails, it shows which
     * line is wrong. This line number starts at 1.
     */
    private static String addLineNumber(String code) {
        String[] lines = code.split("\n");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            builder.append("/* ").append(i + 1).append(" */").append(lines[i]).append("\n");
        }
        return builder.toString();
    }

    /** Clear the compiled class cache. Mainly for testing purposes. */
    public static void clearCache() {
        COMPILED_CLASS_CACHE.clear();
    }

    /** Class to use as key for the compiled class cache. */
    private static final class ClassKey {
        private final int classLoaderId;
        private final String code;

        private ClassKey(int classLoaderId, String code) {
            this.classLoaderId = classLoaderId;
            this.code = code;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClassKey classKey = (ClassKey) o;
            return classLoaderId == classKey.classLoaderId && code.equals(classKey.code);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classLoaderId, code);
        }
    }
}
