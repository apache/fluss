/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.annotation.Internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.HashMap;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Utility class to create instances from class objects and checking failure reasons. */
@Internal
public class InstantiationUtils {

    /** A custom ObjectInputStream that can load classes using a specific ClassLoader. */
    public static class ClassLoaderObjectInputStream extends ObjectInputStream {

        protected final ClassLoader classLoader;

        public ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader)
                throws IOException {
            super(in);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            if (classLoader != null) {
                String name = desc.getName();
                try {
                    return Class.forName(name, false, classLoader);
                } catch (ClassNotFoundException ex) {
                    // check if class is a primitive class
                    Class<?> cl = primitiveClasses.get(name);
                    if (cl != null) {
                        // return primitive class
                        return cl;
                    } else {
                        // throw ClassNotFoundException
                        throw ex;
                    }
                }
            }

            return super.resolveClass(desc);
        }

        @Override
        protected Class<?> resolveProxyClass(String[] interfaces)
                throws IOException, ClassNotFoundException {
            if (classLoader != null) {
                ClassLoader nonPublicLoader = null;
                boolean hasNonPublicInterface = false;

                // define proxy in class loader of non-public interface(s), if any
                Class<?>[] classObjs = new Class<?>[interfaces.length];
                for (int i = 0; i < interfaces.length; i++) {
                    Class<?> cl = Class.forName(interfaces[i], false, classLoader);
                    if ((cl.getModifiers() & Modifier.PUBLIC) == 0) {
                        if (hasNonPublicInterface) {
                            if (nonPublicLoader != cl.getClassLoader()) {
                                throw new IllegalAccessError(
                                        "conflicting non-public interface class loaders");
                            }
                        } else {
                            nonPublicLoader = cl.getClassLoader();
                            hasNonPublicInterface = true;
                        }
                    }
                    classObjs[i] = cl;
                }
                try {
                    return Proxy.getProxyClass(
                            hasNonPublicInterface ? nonPublicLoader : classLoader, classObjs);
                } catch (IllegalArgumentException e) {
                    throw new ClassNotFoundException(null, e);
                }
            }

            return super.resolveProxyClass(interfaces);
        }

        // ------------------------------------------------

        private static final HashMap<String, Class<?>> primitiveClasses =
                CollectionUtils.newHashMapWithExpectedSize(9);

        static {
            primitiveClasses.put("boolean", boolean.class);
            primitiveClasses.put("byte", byte.class);
            primitiveClasses.put("char", char.class);
            primitiveClasses.put("short", short.class);
            primitiveClasses.put("int", int.class);
            primitiveClasses.put("long", long.class);
            primitiveClasses.put("float", float.class);
            primitiveClasses.put("double", double.class);
            primitiveClasses.put("void", void.class);
        }
    }

    public static byte[] serializeObject(Object o) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();
            return baos.toByteArray();
        }
    }

    public static void serializeObject(OutputStream out, Object o) throws IOException {
        ObjectOutputStream oos =
                out instanceof ObjectOutputStream
                        ? (ObjectOutputStream) out
                        : new ObjectOutputStream(out);
        oos.writeObject(o);
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserializeObject(byte[] bytes, ClassLoader cl)
            throws IOException, ClassNotFoundException {
        return deserializeObject(new ByteArrayInputStream(bytes), cl);
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserializeObject(InputStream in, ClassLoader cl)
            throws IOException, ClassNotFoundException {

        final ClassLoader old = Thread.currentThread().getContextClassLoader();
        // not using resource try to avoid AutoClosable's close() on the given stream
        try {
            ObjectInputStream oois = new InstantiationUtils.ClassLoaderObjectInputStream(in, cl);
            Thread.currentThread().setContextClassLoader(cl);
            return (T) oois.readObject();
        } finally {
            Thread.currentThread().setContextClassLoader(old);
        }
    }

    /**
     * Clones the given serializable object using Java serialization.
     *
     * @param obj Object to clone
     * @param <T> Type of the object to clone
     * @return The cloned object
     * @throws IOException Thrown if the serialization or deserialization process fails.
     * @throws ClassNotFoundException Thrown if any of the classes referenced by the object cannot
     *     be resolved during deserialization.
     */
    public static <T extends Serializable> T clone(T obj)
            throws IOException, ClassNotFoundException {
        if (obj == null) {
            return null;
        } else {
            return clone(obj, obj.getClass().getClassLoader());
        }
    }

    /**
     * Clones the given serializable object using Java serialization, using the given classloader to
     * resolve the cloned classes.
     *
     * @param obj Object to clone
     * @param classLoader The classloader to resolve the classes during deserialization.
     * @param <T> Type of the object to clone
     * @return Cloned object
     * @throws IOException Thrown if the serialization or deserialization process fails.
     * @throws ClassNotFoundException Thrown if any of the classes referenced by the object cannot
     *     be resolved during deserialization.
     */
    public static <T extends Serializable> T clone(T obj, ClassLoader classLoader)
            throws IOException, ClassNotFoundException {
        if (obj == null) {
            return null;
        } else {
            final byte[] serializedObject = serializeObject(obj);
            return deserializeObject(serializedObject, classLoader);
        }
    }

    /**
     * Creates a new instance of the given class.
     *
     * @param <T> The generic type of the class.
     * @param clazz The class to instantiate.
     * @return An instance of the given class.
     * @throws RuntimeException Thrown, if the class could not be instantiated. The exception
     *     contains a detailed message about the reason why the instantiation failed.
     */
    public static <T> T instantiate(Class<T> clazz) {
        if (clazz == null) {
            throw new NullPointerException();
        }

        // try to instantiate the class
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException iex) {
            // check for the common problem causes
            checkForInstantiation(clazz);

            // here we are, if non of the common causes was the problem. then the error was
            // most likely an exception in the constructor or field initialization
            throw new RuntimeException(
                    "Could not instantiate type '"
                            + clazz.getName()
                            + "' due to an unspecified exception: "
                            + iex.getMessage(),
                    iex);
        } catch (Throwable t) {
            String message = t.getMessage();
            throw new RuntimeException(
                    "Could not instantiate type '"
                            + clazz.getName()
                            + "' Most likely the constructor (or a member variable initialization) threw an exception"
                            + (message == null ? "." : ": " + message),
                    t);
        }
    }

    /**
     * Performs a standard check whether the class can be instantiated by {@code
     * Class#newInstance()}.
     *
     * @param clazz The class to check.
     * @throws RuntimeException Thrown, if the class cannot be instantiated by {@code
     *     Class#newInstance()}.
     */
    public static void checkForInstantiation(Class<?> clazz) {
        final String errorMessage = checkForInstantiationError(clazz);

        if (errorMessage != null) {
            throw new RuntimeException(
                    "The class '" + clazz.getName() + "' is not instantiable: " + errorMessage);
        }
    }

    public static String checkForInstantiationError(Class<?> clazz) {
        if (!isPublic(clazz)) {
            return "The class is not public.";
        } else if (clazz.isArray()) {
            return "The class is an array. An array cannot be simply instantiated, as with a parameterless constructor.";
        } else if (!isProperClass(clazz)) {
            return "The class is not a proper class. It is either abstract, an interface, or a primitive type.";
        } else if (isNonStaticInnerClass(clazz)) {
            return "The class is an inner class, but not statically accessible.";
        } else if (!hasPublicNullaryConstructor(clazz)) {
            return "The class has no (implicit) public nullary constructor, i.e. a constructor without arguments.";
        } else {
            return null;
        }
    }

    /**
     * Checks, whether the given class is public.
     *
     * @param clazz The class to check.
     * @return True, if the class is public, false if not.
     */
    public static boolean isPublic(Class<?> clazz) {
        return Modifier.isPublic(clazz.getModifiers());
    }

    /**
     * Checks, whether the class is a proper class, i.e. not abstract or an interface, and not a
     * primitive type.
     *
     * @param clazz The class to check.
     * @return True, if the class is a proper class, false otherwise.
     */
    public static boolean isProperClass(Class<?> clazz) {
        int mods = clazz.getModifiers();
        return !(Modifier.isAbstract(mods)
                || Modifier.isInterface(mods)
                || Modifier.isNative(mods));
    }

    /**
     * Checks, whether the class is an inner class that is not statically accessible. That is
     * especially true for anonymous inner classes.
     *
     * @param clazz The class to check.
     * @return True, if the class is a non-statically accessible inner class.
     */
    public static boolean isNonStaticInnerClass(Class<?> clazz) {
        return clazz.getEnclosingClass() != null
                && (clazz.getDeclaringClass() == null || !Modifier.isStatic(clazz.getModifiers()));
    }

    /**
     * Checks, whether the given class has a public nullary constructor.
     *
     * @param clazz The class to check.
     * @return True, if the class has a public nullary constructor, false if not.
     */
    public static boolean hasPublicNullaryConstructor(Class<?> clazz) {
        Constructor<?>[] constructors = clazz.getConstructors();
        for (Constructor<?> constructor : constructors) {
            if (constructor.getParameterCount() == 0
                    && Modifier.isPublic(constructor.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    // --------------------------------------------------------------------------------------------

    /** Private constructor to prevent instantiation. */
    private InstantiationUtils() {
        throw new RuntimeException();
    }
}
