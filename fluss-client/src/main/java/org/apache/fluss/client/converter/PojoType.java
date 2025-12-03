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

package org.apache.fluss.client.converter;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Internal representation of a POJO type, used to validate POJO requirements and to provide unified
 * accessors for reading/writing properties.
 */
final class PojoType<T> {
    private final Class<T> pojoClass;
    private final Constructor<T> defaultConstructor;
    private final Map<String, Property> properties; // property name -> property

    private PojoType(Class<T> pojoClass, Constructor<T> ctor, Map<String, Property> props) {
        this.pojoClass = pojoClass;
        this.defaultConstructor = ctor;
        this.properties = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(props));
    }

    Class<T> getPojoClass() {
        return pojoClass;
    }

    Constructor<T> getDefaultConstructor() {
        return defaultConstructor;
    }

    Map<String, Property> getProperties() {
        return properties;
    }

    @Nullable
    Property getProperty(String name) {
        return properties.get(name);
    }

    static <T> PojoType<T> of(Class<T> pojoClass) {
        validatePublicClass(pojoClass);
        Constructor<T> ctor = requirePublicDefaultConstructor(pojoClass);

        Map<String, Field> allFields = discoverAllInstanceFields(pojoClass);
        Map<String, Method> getters = discoverGetters(pojoClass, allFields);
        Map<String, Method> setters = discoverSetters(pojoClass, allFields);

        Map<String, Property> props = new LinkedHashMap<>();
        for (Map.Entry<String, Field> e : allFields.entrySet()) {
            String name = e.getKey();
            Field field = e.getValue();
            if (field.getType().isPrimitive()) {
                throw new IllegalArgumentException(
                        String.format(
                                "POJO class %s has primitive field '%s' of type %s. Primitive types are not allowed; all fields must be nullable (use wrapper types).",
                                pojoClass.getName(), name, field.getType().getName()));
            }
            boolean publicField = Modifier.isPublic(field.getModifiers());
            Method getter = getters.get(name);
            Method setter = setters.get(name);
            if (!publicField) {
                // When not a public field, require both getter and setter
                if (getter == null || setter == null) {
                    final String capitalizedName = capitalize(name);
                    throw new IllegalArgumentException(
                            String.format(
                                    "POJO class %s field '%s' must be public or have both getter and setter (get%s/set%s).",
                                    pojoClass.getName(), name, capitalizedName, capitalizedName));
                }
            }
            props.put(
                    name,
                    new Property(
                            name, field.getType(), publicField ? field : null, getter, setter));
        }

        return new PojoType<>(pojoClass, ctor, props);
    }

    private static <T> void validatePublicClass(Class<T> pojoClass) {
        if (!Modifier.isPublic(pojoClass.getModifiers())) {
            throw new IllegalArgumentException(
                    String.format("POJO class %s must be public.", pojoClass.getName()));
        }
    }

    private static <T> Constructor<T> requirePublicDefaultConstructor(Class<T> pojoClass) {
        Constructor<T>[] ctors = (Constructor<T>[]) pojoClass.getConstructors();
        for (Constructor<T> c : ctors) {
            if (!Modifier.isPublic(c.getModifiers())) {
                continue;
            }
            if (c.getParameterCount() == 0) {

                return c;
            }
            if (c.getParameterCount() == 1
                    && pojoClass
                            .getName()
                            .equals(
                                    c.getParameterTypes()[0].getName()
                                            + "$"
                                            + pojoClass.getSimpleName())) {
                return c;
            }
        }

        throw new IllegalArgumentException(
                String.format(
                        "POJO class %s must have a public default constructor.",
                        pojoClass.getName()));
    }

    private static Map<String, Field> discoverAllInstanceFields(Class<?> clazz) {
        Map<String, Field> fields = new LinkedHashMap<>();
        Class<?> c = clazz;
        while (c != null && c != Object.class) {
            for (Field f : c.getDeclaredFields()) {
                int mod = f.getModifiers();
                if (Modifier.isStatic(mod) || Modifier.isTransient(mod)) {
                    continue;
                }
                // Skip references to enclosing class
                if (f.getName().startsWith("this$")) {
                    final Class type = f.getType();
                    if ((type.getName() + "$" + clazz.getSimpleName()).equals(clazz.getName())) {
                        continue;
                    }
                }
                f.setAccessible(true);
                fields.putIfAbsent(f.getName(), f);
            }
            c = c.getSuperclass();
        }
        return fields;
    }

    private static Map<String, Method> discoverGetters(
            Class<?> clazz, Map<String, Field> fieldMap) {
        final Map<String, Method> getters = new HashMap<>();
        for (Method m : clazz.getMethods()) { // public methods incl. inherited
            final String prop = getGetterProp(m);
            if (fieldMap.containsKey(prop)) {
                getters.put(prop, m);
            }
        }
        return getters;
    }

    private static String getGetterProp(Method m) {
        if (m.getParameterCount() != 0) {
            return null;
        }
        final Class<?> returnType = m.getReturnType();
        if (void.class.equals(returnType) || Void.class.equals(returnType)) {
            return null;
        }
        final String name = m.getName();
        if (name.startsWith("get")) {
            return decapitalize(name.substring(3));
        }
        if (returnType.equals(boolean.class) || returnType.equals(Boolean.class)) {
            if (name.startsWith("is")) {
                return decapitalize(name.substring(2));
            }
            if (name.startsWith("has")) {
                return decapitalize(name.substring(3));
            }
        }
        return null;
    }

    private static Map<String, Method> discoverSetters(
            Class<?> clazz, Map<String, Field> fieldMap) {
        final Map<String, Method> setters = new HashMap<>();
        for (Method m : clazz.getMethods()) { // public methods incl. inherited
            final String prop = getSetterProp(m);
            if (fieldMap.containsKey(prop)) {
                setters.put(prop, m);
            }
        }
        return setters;
    }

    private static String getSetterProp(Method m) {
        if (m.getParameterCount() != 1) {
            return null;
        }
        final Class<?> returnType = m.getReturnType();
        if (!void.class.equals(returnType) && !Void.class.equals(returnType)) {
            return null;
        }
        final String name = m.getName();
        if (name.startsWith("set")) {
            return decapitalize(name.substring(3));
        }
        return null;
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        return s.substring(0, 1).toUpperCase(Locale.ROOT) + s.substring(1);
    }

    private static String decapitalize(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        return s.substring(0, 1).toLowerCase(Locale.ROOT) + s.substring(1);
    }

    static final class Property {
        final String name;
        final Class<?> type;
        @Nullable final Field publicField;
        @Nullable final Method getter;
        @Nullable final Method setter;

        Property(
                String name,
                Class<?> type,
                @Nullable Field publicField,
                @Nullable Method getter,
                @Nullable Method setter) {
            this.name = Objects.requireNonNull(name, "name");
            this.type = Objects.requireNonNull(type, "type");
            this.publicField = publicField;
            this.getter = getter;
            this.setter = setter;
        }

        Object read(Object instance) throws Exception {
            if (publicField != null) {
                return publicField.get(instance);
            } else if (getter != null) {
                return getter.invoke(instance);
            } else {
                throw new IllegalStateException("No readable accessor for property '" + name + "'");
            }
        }

        void write(Object instance, @Nullable Object value) throws Exception {
            if (publicField != null) {
                publicField.set(instance, value);
            } else if (setter != null) {
                setter.invoke(instance, value);
            } else {
                throw new IllegalStateException("No writable accessor for property '" + name + "'");
            }
        }
    }
}
