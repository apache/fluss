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

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * A fluent builder for generating Java source code with proper indentation.
 *
 * <p>This builder provides a type-safe, readable API for constructing Java code, handling
 * indentation automatically and supporting common code patterns like classes, methods, if-else
 * blocks, and loops.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Type-safe modifiers via {@link Modifier} enum
 *   <li>Type-safe primitive types via {@link PrimitiveType} enum
 *   <li>Convenient type references via {@link #typeOf(Class)}
 *   <li>Automatic indentation management
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * import static org.apache.fluss.codegen.JavaCodeBuilder.Modifier.*;
 * import static org.apache.fluss.codegen.JavaCodeBuilder.PrimitiveType.*;
 *
 * String code = new JavaCodeBuilder()
 *     .beginClass(mods(PUBLIC, FINAL), "MyClass", typeOf(Serializable.class))
 *         .field(mods(PRIVATE), INT, "count")
 *         .newLine()
 *         .beginConstructor(PUBLIC, "MyClass", "Object[] references")
 *             .stmt("this.count = 0")
 *         .endConstructor()
 *         .newLine()
 *         .beginMethod(PUBLIC, BOOLEAN, "equals", "InternalRow left, InternalRow right")
 *             .beginIf("left == null")
 *                 .returnStmt("false")
 *             .endIf()
 *             .returnStmt("left.equals(right)")
 *         .endMethod()
 *     .endClass()
 *     .build();
 * }</pre>
 */
public class JavaCodeBuilder {

    private static final String INDENT = "  ";

    private final StringBuilder code;
    private int indentLevel;

    public JavaCodeBuilder() {
        this.code = new StringBuilder();
        this.indentLevel = 0;
    }

    // ==================== Type-Safe Enums ====================

    /** Java access and non-access modifiers. */
    public enum Modifier {
        PUBLIC("public"),
        PRIVATE("private"),
        PROTECTED("protected"),
        STATIC("static"),
        FINAL("final"),
        ABSTRACT("abstract"),
        TRANSIENT("transient"),
        VOLATILE("volatile"),
        SYNCHRONIZED("synchronized"),
        NATIVE("native");

        private final String keyword;

        Modifier(String keyword) {
            this.keyword = keyword;
        }

        @Override
        public String toString() {
            return keyword;
        }
    }

    /** Java primitive types. */
    public enum PrimitiveType {
        BOOLEAN("boolean"),
        BYTE("byte"),
        CHAR("char"),
        SHORT("short"),
        INT("int"),
        LONG("long"),
        FLOAT("float"),
        DOUBLE("double"),
        VOID("void");

        private final String keyword;

        PrimitiveType(String keyword) {
            this.keyword = keyword;
        }

        @Override
        public String toString() {
            return keyword;
        }
    }

    // ==================== Parameter Class ====================

    /**
     * Represents a method or constructor parameter with type and name.
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * // Using primitive type
     * Param.of(INT, "count")
     *
     * // Using class type
     * Param.of(String.class, "name")
     *
     * // Using type string
     * Param.of("InternalRow", "row")
     * }</pre>
     */
    public static final class Param {
        private final String type;
        private final String name;

        private Param(String type, String name) {
            this.type = type;
            this.name = name;
        }

        /** Creates a parameter with a primitive type. */
        public static Param of(PrimitiveType type, String name) {
            return new Param(type.toString(), name);
        }

        /** Creates a parameter with a class type. */
        public static Param of(Class<?> type, String name) {
            return new Param(type.getCanonicalName(), name);
        }

        /** Creates a parameter with a type string. */
        public static Param of(String type, String name) {
            return new Param(type, name);
        }

        /** Returns the type of this parameter. */
        public String getType() {
            return type;
        }

        /** Returns the name of this parameter. */
        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return type + " " + name;
        }
    }

    // ==================== Static Helper Methods ====================

    /**
     * Combines multiple modifiers into a space-separated string.
     *
     * @param modifiers the modifiers to combine
     * @return the combined modifier string
     */
    public static String mods(Modifier... modifiers) {
        return Arrays.stream(modifiers).map(Modifier::toString).collect(Collectors.joining(" "));
    }

    /**
     * Combines multiple parameters into a comma-separated string.
     *
     * @param params the parameters to combine
     * @return the combined parameter string
     */
    public static String params(Param... params) {
        return Arrays.stream(params).map(Param::toString).collect(Collectors.joining(", "));
    }

    /**
     * Returns the canonical name of a class for use in generated code.
     *
     * @param clazz the class
     * @return the canonical class name
     */
    public static String typeOf(Class<?> clazz) {
        return clazz.getCanonicalName();
    }

    /**
     * Returns the array type string for a given element type.
     *
     * @param elementType the element type
     * @return the array type string (e.g., "int[]")
     */
    public static String arrayOf(PrimitiveType elementType) {
        return elementType.toString() + "[]";
    }

    /**
     * Returns the array type string for a given class.
     *
     * @param clazz the element class
     * @return the array type string (e.g., "String[]")
     */
    public static String arrayOf(Class<?> clazz) {
        return clazz.getCanonicalName() + "[]";
    }

    /**
     * Returns the array type string for a given type name.
     *
     * @param typeName the element type name
     * @return the array type string
     */
    public static String arrayOf(String typeName) {
        return typeName + "[]";
    }

    // ==================== Class Structure ====================

    /**
     * Begins a class declaration with type-safe modifiers.
     *
     * @param modifiers class modifiers
     * @param className the class name
     * @param implementsInterface the interface to implement (can be null)
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginClass(
            Modifier[] modifiers, String className, String implementsInterface) {
        return beginClass(mods(modifiers), className, implementsInterface);
    }

    /**
     * Begins a class declaration.
     *
     * @param modifiers class modifiers (e.g., "public final")
     * @param className the class name
     * @param implementsInterface the interface to implement (can be null)
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginClass(
            String modifiers, String className, String implementsInterface) {
        indent();
        code.append(modifiers).append(" class ").append(className);
        if (implementsInterface != null && !implementsInterface.isEmpty()) {
            code.append(" implements ").append(implementsInterface);
        }
        code.append(" {\n");
        indentLevel++;
        return this;
    }

    /** Ends a class declaration. */
    public JavaCodeBuilder endClass() {
        indentLevel--;
        indent();
        code.append("}\n");
        return this;
    }

    // ==================== Fields ====================

    /**
     * Adds a field declaration with type-safe modifiers and primitive type.
     *
     * @param modifiers field modifiers
     * @param type the primitive type
     * @param name the field name
     * @return this builder for chaining
     */
    public JavaCodeBuilder field(Modifier[] modifiers, PrimitiveType type, String name) {
        return field(mods(modifiers), type.toString(), name);
    }

    /**
     * Adds a field declaration with type-safe modifiers.
     *
     * @param modifiers field modifiers
     * @param type the field type
     * @param name the field name
     * @return this builder for chaining
     */
    public JavaCodeBuilder field(Modifier[] modifiers, String type, String name) {
        return field(mods(modifiers), type, name);
    }

    /**
     * Adds a field declaration with single modifier and primitive type.
     *
     * @param modifier field modifier
     * @param type the primitive type
     * @param name the field name
     * @return this builder for chaining
     */
    public JavaCodeBuilder field(Modifier modifier, PrimitiveType type, String name) {
        return field(modifier.toString(), type.toString(), name);
    }

    /**
     * Adds a field declaration with single modifier.
     *
     * @param modifier field modifier
     * @param type the field type
     * @param name the field name
     * @return this builder for chaining
     */
    public JavaCodeBuilder field(Modifier modifier, String type, String name) {
        return field(modifier.toString(), type, name);
    }

    /**
     * Adds a field declaration.
     *
     * @param modifiers field modifiers (e.g., "private", "private transient")
     * @param type the field type
     * @param name the field name
     * @return this builder for chaining
     */
    public JavaCodeBuilder field(String modifiers, String type, String name) {
        indent();
        code.append(modifiers).append(" ").append(type).append(" ").append(name).append(";\n");
        return this;
    }

    /**
     * Adds a field declaration with initialization.
     *
     * @param modifiers field modifiers
     * @param type the field type
     * @param name the field name
     * @param initialValue the initial value expression
     * @return this builder for chaining
     */
    public JavaCodeBuilder fieldWithInit(
            Modifier[] modifiers, PrimitiveType type, String name, String initialValue) {
        return fieldWithInit(mods(modifiers), type.toString(), name, initialValue);
    }

    /**
     * Adds a field declaration with initialization.
     *
     * @param modifiers field modifiers
     * @param type the field type
     * @param name the field name
     * @param initialValue the initial value expression
     * @return this builder for chaining
     */
    public JavaCodeBuilder fieldWithInit(
            Modifier[] modifiers, String type, String name, String initialValue) {
        return fieldWithInit(mods(modifiers), type, name, initialValue);
    }

    /**
     * Adds a field declaration with initialization.
     *
     * @param modifiers field modifiers
     * @param type the field type
     * @param name the field name
     * @param initialValue the initial value expression
     * @return this builder for chaining
     */
    public JavaCodeBuilder fieldWithInit(
            String modifiers, String type, String name, String initialValue) {
        indent();
        code.append(modifiers).append(" ").append(type).append(" ").append(name);
        code.append(" = ").append(initialValue).append(";\n");
        return this;
    }

    // ==================== Constructor ====================

    /**
     * Begins a constructor declaration with type-safe modifier and parameters.
     *
     * @param modifier constructor modifier
     * @param className the class name
     * @param params the type-safe parameters
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginConstructor(Modifier modifier, String className, Param... params) {
        return beginConstructor(modifier.toString(), className, params(params));
    }

    /**
     * Begins a constructor declaration with type-safe modifier.
     *
     * @param modifier constructor modifier
     * @param className the class name
     * @param params the parameter list
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginConstructor(Modifier modifier, String className, String params) {
        return beginConstructor(modifier.toString(), className, params);
    }

    /**
     * Begins a constructor declaration.
     *
     * @param modifiers constructor modifiers
     * @param className the class name
     * @param params the parameter list (e.g., "Object[] references")
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginConstructor(String modifiers, String className, String params) {
        indent();
        code.append(modifiers).append(" ").append(className).append("(").append(params).append(")");
        code.append(" throws Exception {\n");
        indentLevel++;
        return this;
    }

    /** Ends a constructor. */
    public JavaCodeBuilder endConstructor() {
        indentLevel--;
        indent();
        code.append("}\n");
        return this;
    }

    // ==================== Methods ====================

    /**
     * Begins a method declaration with type-safe modifier, primitive return type, and parameters.
     *
     * @param modifier method modifier
     * @param returnType the primitive return type
     * @param methodName the method name
     * @param params the type-safe parameters
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginMethod(
            Modifier modifier, PrimitiveType returnType, String methodName, Param... params) {
        return beginMethod(modifier.toString(), returnType.toString(), methodName, params(params));
    }

    /**
     * Begins a method declaration with type-safe modifier and parameters.
     *
     * @param modifier method modifier
     * @param returnType the return type
     * @param methodName the method name
     * @param params the type-safe parameters
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginMethod(
            Modifier modifier, String returnType, String methodName, Param... params) {
        return beginMethod(modifier.toString(), returnType, methodName, params(params));
    }

    /**
     * Begins a method declaration with type-safe modifier and primitive return type.
     *
     * @param modifier method modifier
     * @param returnType the primitive return type
     * @param methodName the method name
     * @param params the parameter list
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginMethod(
            Modifier modifier, PrimitiveType returnType, String methodName, String params) {
        return beginMethod(modifier.toString(), returnType.toString(), methodName, params);
    }

    /**
     * Begins a method declaration with type-safe modifier.
     *
     * @param modifier method modifier
     * @param returnType the return type
     * @param methodName the method name
     * @param params the parameter list
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginMethod(
            Modifier modifier, String returnType, String methodName, String params) {
        return beginMethod(modifier.toString(), returnType, methodName, params);
    }

    /**
     * Begins a method declaration.
     *
     * @param modifiers method modifiers (e.g., "public", "private")
     * @param returnType the return type
     * @param methodName the method name
     * @param params the parameter list
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginMethod(
            String modifiers, String returnType, String methodName, String params) {
        indent();
        code.append(modifiers).append(" ").append(returnType).append(" ").append(methodName);
        code.append("(").append(params).append(") {\n");
        indentLevel++;
        return this;
    }

    /** Adds an @Override annotation. */
    public JavaCodeBuilder override() {
        indent();
        code.append("@Override\n");
        return this;
    }

    /** Ends a method. */
    public JavaCodeBuilder endMethod() {
        indentLevel--;
        indent();
        code.append("}\n");
        return this;
    }

    // ==================== Control Flow ====================

    /**
     * Begins an if block.
     *
     * @param condition the condition expression
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginIf(String condition) {
        indent();
        code.append("if (").append(condition).append(") {\n");
        indentLevel++;
        return this;
    }

    /** Ends an if block. */
    public JavaCodeBuilder endIf() {
        indentLevel--;
        indent();
        code.append("}\n");
        return this;
    }

    /**
     * Begins an else-if block.
     *
     * @param condition the condition expression
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginElseIf(String condition) {
        indentLevel--;
        indent();
        code.append("} else if (").append(condition).append(") {\n");
        indentLevel++;
        return this;
    }

    /** Begins an else block. */
    public JavaCodeBuilder beginElse() {
        indentLevel--;
        indent();
        code.append("} else {\n");
        indentLevel++;
        return this;
    }

    /**
     * Begins a for loop.
     *
     * @param init initialization expression
     * @param condition loop condition
     * @param update update expression
     * @return this builder for chaining
     */
    public JavaCodeBuilder beginFor(String init, String condition, String update) {
        indent();
        code.append("for (")
                .append(init)
                .append("; ")
                .append(condition)
                .append("; ")
                .append(update)
                .append(") {\n");
        indentLevel++;
        return this;
    }

    /** Ends a for loop. */
    public JavaCodeBuilder endFor() {
        indentLevel--;
        indent();
        code.append("}\n");
        return this;
    }

    // ==================== Statements ====================

    /**
     * Adds a statement with semicolon.
     *
     * @param statement the statement (without semicolon)
     * @return this builder for chaining
     */
    public JavaCodeBuilder stmt(String statement) {
        indent();
        code.append(statement).append(";\n");
        return this;
    }

    /**
     * Adds a return statement.
     *
     * @param expression the return expression
     * @return this builder for chaining
     */
    public JavaCodeBuilder returnStmt(String expression) {
        indent();
        code.append("return ").append(expression).append(";\n");
        return this;
    }

    /**
     * Adds a variable declaration with primitive type.
     *
     * @param type the primitive type
     * @param name the variable name
     * @param value the initial value expression
     * @return this builder for chaining
     */
    public JavaCodeBuilder declare(PrimitiveType type, String name, String value) {
        return declare(type.toString(), name, value);
    }

    /**
     * Adds a variable declaration.
     *
     * @param type the variable type
     * @param name the variable name
     * @param value the initial value expression
     * @return this builder for chaining
     */
    public JavaCodeBuilder declare(String type, String name, String value) {
        indent();
        code.append(type).append(" ").append(name).append(" = ").append(value).append(";\n");
        return this;
    }

    /**
     * Adds an assignment statement.
     *
     * @param variable the variable name
     * @param value the value expression
     * @return this builder for chaining
     */
    public JavaCodeBuilder assign(String variable, String value) {
        indent();
        code.append(variable).append(" = ").append(value).append(";\n");
        return this;
    }

    /**
     * Adds a continue statement.
     *
     * @return this builder for chaining
     */
    public JavaCodeBuilder continueStmt() {
        indent();
        code.append("continue;\n");
        return this;
    }

    /**
     * Adds a break statement.
     *
     * @return this builder for chaining
     */
    public JavaCodeBuilder breakStmt() {
        indent();
        code.append("break;\n");
        return this;
    }

    // ==================== Raw Code ====================

    /**
     * Appends raw code with current indentation.
     *
     * @param rawCode the raw code to append
     * @return this builder for chaining
     */
    public JavaCodeBuilder raw(String rawCode) {
        if (rawCode != null && !rawCode.isEmpty()) {
            for (String line : rawCode.split("\n")) {
                if (!line.trim().isEmpty()) {
                    indent();
                    code.append(line.trim()).append("\n");
                }
            }
        }
        return this;
    }

    /**
     * Appends raw code without any indentation processing.
     *
     * @param rawCode the raw code to append
     * @return this builder for chaining
     */
    public JavaCodeBuilder rawUnindented(String rawCode) {
        if (rawCode != null && !rawCode.isEmpty()) {
            code.append(rawCode);
            if (!rawCode.endsWith("\n")) {
                code.append("\n");
            }
        }
        return this;
    }

    /** Adds a blank line. */
    public JavaCodeBuilder newLine() {
        code.append("\n");
        return this;
    }

    // ==================== Build ====================

    /**
     * Builds and returns the generated code.
     *
     * @return the generated Java source code
     */
    public String build() {
        return code.toString();
    }

    @Override
    public String toString() {
        return build();
    }

    // ==================== Internal ====================

    /** Pre-computed indent strings for common levels to avoid repeated string concatenation. */
    private static final String[] INDENT_CACHE = new String[16];

    static {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < INDENT_CACHE.length; i++) {
            INDENT_CACHE[i] = sb.toString();
            sb.append(INDENT);
        }
    }

    private void indent() {
        if (indentLevel < INDENT_CACHE.length) {
            code.append(INDENT_CACHE[indentLevel]);
        } else {
            // Fallback for deep nesting (unlikely in practice)
            for (int i = 0; i < indentLevel; i++) {
                code.append(INDENT);
            }
        }
    }
}
