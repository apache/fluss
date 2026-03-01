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

import org.apache.fluss.codegen.JavaCodeBuilder.Modifier;
import org.apache.fluss.codegen.JavaCodeBuilder.Param;
import org.apache.fluss.codegen.JavaCodeBuilder.PrimitiveType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.apache.fluss.codegen.JavaCodeBuilder.Modifier.FINAL;
import static org.apache.fluss.codegen.JavaCodeBuilder.Modifier.PRIVATE;
import static org.apache.fluss.codegen.JavaCodeBuilder.Modifier.PUBLIC;
import static org.apache.fluss.codegen.JavaCodeBuilder.Modifier.STATIC;
import static org.apache.fluss.codegen.JavaCodeBuilder.Modifier.TRANSIENT;
import static org.apache.fluss.codegen.JavaCodeBuilder.Param.of;
import static org.apache.fluss.codegen.JavaCodeBuilder.PrimitiveType.BOOLEAN;
import static org.apache.fluss.codegen.JavaCodeBuilder.PrimitiveType.INT;
import static org.apache.fluss.codegen.JavaCodeBuilder.PrimitiveType.VOID;
import static org.apache.fluss.codegen.JavaCodeBuilder.arrayOf;
import static org.apache.fluss.codegen.JavaCodeBuilder.mods;
import static org.apache.fluss.codegen.JavaCodeBuilder.params;
import static org.apache.fluss.codegen.JavaCodeBuilder.typeOf;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link JavaCodeBuilder}.
 *
 * <p>Test strategy:
 *
 * <ul>
 *   <li>Code generation tests use expected file assertions for precise validation
 *   <li>Helper method tests verify static utilities with exact equality
 *   <li>Edge case tests verify specific boundary behaviors
 * </ul>
 *
 * <p>Expected files: {@code src/test/resources/expected/java-code-builder/}
 *
 * <p>Update expected files: {@code mvn test -Dcodegen.update.expected=true}
 */
public class JavaCodeBuilderTest {

    private static final String EXPECTED_DIR = "java-code-builder";

    private JavaCodeBuilder builder;

    @BeforeEach
    public void setUp() {
        builder = new JavaCodeBuilder();
    }

    // ==================== Code Generation Tests ====================

    /** Tests basic class structure: empty class, interface implementation. */
    @Test
    public void testClassStructure() {
        String code =
                builder.beginClass(new Modifier[] {PUBLIC, FINAL}, "MyClass", "Serializable")
                        .endClass()
                        .build();

        CodeGenTestUtils.assertMatchesExpected(
                code, EXPECTED_DIR + "/classStructure.java.expected");
    }

    /** Tests field declarations: primitive, object, with initialization. */
    @Test
    public void testFields() {
        String code =
                builder.beginClass(PUBLIC, "TestClass", null)
                        .field(PRIVATE, INT, "count")
                        .field(new Modifier[] {PRIVATE, FINAL}, "String", "name")
                        .fieldWithInit(PRIVATE, BOOLEAN, "active", "true")
                        .fieldWithInit(new Modifier[] {PRIVATE, STATIC, FINAL}, INT, "MAX", "100")
                        .endClass()
                        .build();

        CodeGenTestUtils.assertMatchesExpected(code, EXPECTED_DIR + "/fields.java.expected");
    }

    /** Tests constructor with parameters. */
    @Test
    public void testConstructor() {
        String code =
                builder.beginClass(PUBLIC, "TestClass", null)
                        .beginConstructor(
                                PUBLIC, "TestClass", of(INT, "value"), of("String", "name"))
                        .stmt("this.value = value")
                        .stmt("this.name = name")
                        .endConstructor()
                        .endClass()
                        .build();

        CodeGenTestUtils.assertMatchesExpected(code, EXPECTED_DIR + "/constructor.java.expected");
    }

    /** Tests method with @Override annotation. */
    @Test
    public void testMethodWithOverride() {
        String code =
                builder.beginClass(PUBLIC, "TestClass", null)
                        .override()
                        .beginMethod(PUBLIC, "String", "toString")
                        .returnStmt("\"TestClass\"")
                        .endMethod()
                        .endClass()
                        .build();

        CodeGenTestUtils.assertMatchesExpected(
                code, EXPECTED_DIR + "/methodWithOverride.java.expected");
    }

    /** Tests if/else-if/else control flow with proper indentation. */
    @Test
    public void testIfElseIfElse() {
        String code =
                builder.beginClass(PUBLIC, "TestClass", null)
                        .beginMethod(PUBLIC, "String", "classify", of(INT, "x"))
                        .beginIf("x > 0")
                        .returnStmt("\"positive\"")
                        .beginElseIf("x < 0")
                        .returnStmt("\"negative\"")
                        .beginElse()
                        .returnStmt("\"zero\"")
                        .endIf()
                        .endMethod()
                        .endClass()
                        .build();

        CodeGenTestUtils.assertMatchesExpected(code, EXPECTED_DIR + "/ifElseIfElse.java.expected");
    }

    /** Tests nested control flow: for loops with if/else, break/continue. */
    @Test
    public void testNestedControlFlow() {
        String code =
                builder.beginClass(PUBLIC, "TestClass", null)
                        .beginMethod(PUBLIC, VOID, "process", of("int[][]", "matrix"))
                        .beginFor("int i = 0", "i < matrix.length", "i++")
                        .beginFor("int j = 0", "j < matrix[i].length", "j++")
                        .beginIf("matrix[i][j] < 0")
                        .continueStmt()
                        .beginElseIf("matrix[i][j] == 0")
                        .breakStmt()
                        .beginElse()
                        .stmt("System.out.println(matrix[i][j])")
                        .endIf()
                        .endFor()
                        .endFor()
                        .endMethod()
                        .endClass()
                        .build();

        CodeGenTestUtils.assertMatchesExpected(
                code, EXPECTED_DIR + "/nestedControlFlow.java.expected");
    }

    /** Tests raw code insertion with and without indentation. */
    @Test
    public void testRawCode() {
        String code =
                builder.beginClass(PUBLIC, "TestClass", null)
                        .raw("// Class-level comment")
                        .raw("private static final int CONST = 42;")
                        .newLine()
                        .rawUnindented("// Unindented comment at class level")
                        .beginMethod(PUBLIC, VOID, "test")
                        .raw("// Multi-line raw\nint x = 1;\nint y = 2;")
                        .endMethod()
                        .endClass()
                        .build();

        CodeGenTestUtils.assertMatchesExpected(code, EXPECTED_DIR + "/rawCode.java.expected");
    }

    /** Tests complete class with all features: fields, constructor, methods, control flow. */
    @Test
    public void testCompleteClass() {
        String code =
                builder.beginClass(new Modifier[] {PUBLIC, FINAL}, "RecordEqualiser", "Equaliser")
                        .field(PRIVATE, INT, "fieldCount")
                        .field(new Modifier[] {PRIVATE, FINAL}, "Object[]", "references")
                        .newLine()
                        .beginConstructor(PUBLIC, "RecordEqualiser", of("Object[]", "references"))
                        .stmt("this.references = references")
                        .stmt("this.fieldCount = 0")
                        .endConstructor()
                        .newLine()
                        .override()
                        .beginMethod(
                                PUBLIC,
                                BOOLEAN,
                                "equals",
                                of("Object", "left"),
                                of("Object", "right"))
                        .beginIf("left == null && right == null")
                        .returnStmt("true")
                        .endIf()
                        .beginIf("left == null || right == null")
                        .returnStmt("false")
                        .endIf()
                        .declare(BOOLEAN, "result", "true")
                        .beginFor("int i = 0", "i < fieldCount", "i++")
                        .stmt("result = result && compareField(i, left, right)")
                        .endFor()
                        .returnStmt("result")
                        .endMethod()
                        .newLine()
                        .beginMethod(
                                PRIVATE,
                                BOOLEAN,
                                "compareField",
                                of(INT, "idx"),
                                of("Object", "l"),
                                of("Object", "r"))
                        .returnStmt("l.equals(r)")
                        .endMethod()
                        .endClass()
                        .build();

        CodeGenTestUtils.assertMatchesExpected(code, EXPECTED_DIR + "/completeClass.java.expected");
    }

    /** Tests type-safe API with full type references. */
    @Test
    public void testTypeSafeApi() {
        String code =
                builder.beginClass(
                                new Modifier[] {PUBLIC, FINAL},
                                "Calculator",
                                typeOf(Serializable.class))
                        .field(new Modifier[] {PRIVATE}, INT, "result")
                        .newLine()
                        .beginConstructor(PUBLIC, "Calculator", of(arrayOf(Object.class), "refs"))
                        .stmt("this.result = 0")
                        .endConstructor()
                        .newLine()
                        .beginMethod(PUBLIC, INT, "add", of(INT, "a"), of(INT, "b"))
                        .declare(INT, "sum", "a + b")
                        .returnStmt("sum")
                        .endMethod()
                        .endClass()
                        .build();

        CodeGenTestUtils.assertMatchesExpected(code, EXPECTED_DIR + "/typeSafeApi.java.expected");
    }

    // ==================== Helper Method Tests ====================

    @Test
    public void testModsHelper() {
        assertThat(mods(PUBLIC)).isEqualTo("public");
        assertThat(mods(PUBLIC, FINAL)).isEqualTo("public final");
        assertThat(mods(PRIVATE, STATIC, FINAL)).isEqualTo("private static final");
        assertThat(mods(PRIVATE, TRANSIENT)).isEqualTo("private transient");
    }

    @Test
    public void testParamsHelper() {
        assertThat(params()).isEqualTo("");
        assertThat(params(of(INT, "a"))).isEqualTo("int a");
        assertThat(params(of(INT, "a"), of(INT, "b"))).isEqualTo("int a, int b");
        assertThat(params(of(String.class, "name"), of(BOOLEAN, "flag")))
                .isEqualTo("java.lang.String name, boolean flag");
    }

    @Test
    public void testTypeOfHelper() {
        assertThat(typeOf(String.class)).isEqualTo("java.lang.String");
        assertThat(typeOf(Serializable.class)).isEqualTo("java.io.Serializable");
    }

    @Test
    public void testArrayOfHelper() {
        assertThat(arrayOf(INT)).isEqualTo("int[]");
        assertThat(arrayOf(String.class)).isEqualTo("java.lang.String[]");
        assertThat(arrayOf("Object")).isEqualTo("Object[]");
    }

    @Test
    public void testParamClass() {
        Param p1 = of(INT, "count");
        assertThat(p1.getType()).isEqualTo("int");
        assertThat(p1.getName()).isEqualTo("count");
        assertThat(p1.toString()).isEqualTo("int count");

        Param p2 = of(String.class, "name");
        assertThat(p2.toString()).isEqualTo("java.lang.String name");

        Param p3 = of("InternalRow", "row");
        assertThat(p3.toString()).isEqualTo("InternalRow row");
    }

    @Test
    public void testModifierEnum() {
        assertThat(Modifier.PUBLIC.toString()).isEqualTo("public");
        assertThat(Modifier.PRIVATE.toString()).isEqualTo("private");
        assertThat(Modifier.STATIC.toString()).isEqualTo("static");
        assertThat(Modifier.FINAL.toString()).isEqualTo("final");
    }

    @Test
    public void testPrimitiveTypeEnum() {
        assertThat(PrimitiveType.BOOLEAN.toString()).isEqualTo("boolean");
        assertThat(PrimitiveType.INT.toString()).isEqualTo("int");
        assertThat(PrimitiveType.LONG.toString()).isEqualTo("long");
        assertThat(PrimitiveType.VOID.toString()).isEqualTo("void");
    }

    // ==================== Edge Case Tests ====================

    /** Tests that empty/null/whitespace raw code is handled correctly. */
    @Test
    public void testEmptyRawCode() {
        String code =
                builder.beginClass(PUBLIC, "Test", null)
                        .raw("")
                        .raw(null)
                        .raw("   \n   \n   ")
                        .rawUnindented("")
                        .rawUnindented(null)
                        .field(PRIVATE, INT, "x")
                        .endClass()
                        .build();

        CodeGenTestUtils.assertMatchesExpected(code, EXPECTED_DIR + "/emptyRawCode.java.expected");
    }

    /** Tests toString() returns same as build(). */
    @Test
    public void testToString() {
        builder.beginClass(PUBLIC, "Test", null).field(PRIVATE, INT, "x").endClass();
        assertThat(builder.toString()).isEqualTo(builder.build());
    }
}
