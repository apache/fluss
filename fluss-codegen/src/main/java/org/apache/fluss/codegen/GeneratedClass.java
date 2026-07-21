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

import java.io.Serializable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A wrapper for generated class, defines a {@link #newInstance(ClassLoader)} method to get an
 * instance by reference objects easily.
 *
 * @param <T> the type of the generated class
 */
public final class GeneratedClass<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String className;
    private final String code;
    private final Object[] references;

    private transient Class<T> compiledClass;

    public GeneratedClass(String className, String code) {
        this(className, code, new Object[0]);
    }

    public GeneratedClass(String className, String code, Object[] references) {
        this.className = checkNotNull(className, "className must not be null");
        this.code = checkNotNull(code, "code must not be null");
        this.references = checkNotNull(references, "references must not be null");
    }

    /**
     * Create a new instance of this generated class.
     *
     * @param classLoader the class loader to use for loading the compiled class
     * @return a new instance of the generated class
     */
    public T newInstance(ClassLoader classLoader) {
        try {
            return compile(classLoader)
                    .getConstructor(Object[].class)
                    // Because Constructor.newInstance(Object... initargs), we need to load
                    // references into a new Object[], otherwise it cannot be compiled.
                    .newInstance(new Object[] {references});
        } catch (Throwable e) {
            throw new RuntimeException(
                    "Could not instantiate generated class '" + className + "'", e);
        }
    }

    /**
     * Compiles the generated code, the compiled class will be cached in the {@link GeneratedClass}.
     *
     * @param classLoader the class loader to use for compilation
     * @return the compiled class
     */
    @SuppressWarnings("unchecked")
    public Class<T> compile(ClassLoader classLoader) {
        if (compiledClass == null) {
            compiledClass = (Class<T>) CompileUtils.compile(classLoader, className, code);
        }
        return compiledClass;
    }

    public String getClassName() {
        return className;
    }

    public String getCode() {
        return code;
    }

    public Object[] getReferences() {
        return references;
    }
}
