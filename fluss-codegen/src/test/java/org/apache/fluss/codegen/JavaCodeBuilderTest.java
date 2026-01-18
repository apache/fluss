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
 * <p>Test coverage includes:
 *
 * <ul>
 *   <li>Class structure generation (class, fields, constructors, methods)
 *   <li>Control flow constructs (if/else, for loops)
 *   <li>Statement generation (declarations, assignments, returns)
 *   <li>Indentation correctness at all nesting levels
 *   <li>Edge cases (empty strings, null handling, special characters)
 *   <li>Fluent API chaining behavior
 * </ul>
 */
public class JavaCodeBuilderTest {

    private JavaCodeBuilder builder;

    @BeforeEach
    public void setUp() {
        builder = new JavaCodeBuilder();
    }

    // ==================== Class Structure Tests ====================

    @Test
    public void testEmptyClass() {
        String code = builder.beginClass("public", "EmptyClass", null).endClass().build();

        assertThat(code).isEqualTo("public class EmptyClass {\n}\n");
    }

    @Test
    public void testClassWithInterface() {
        String code =
                builder.beginClass("public final", "MyClass", "Serializable").endClass().build();

        assertThat(code).isEqualTo("public final class MyClass implements Serializable {\n}\n");
    }

    @Test
    public void testClassWithEmptyInterface() {
        String code = builder.beginClass("public", "MyClass", "").endClass().build();

        assertThat(code).isEqualTo("public class MyClass {\n}\n");
    }

    @Test
    public void testField() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .field("private", "int", "count")
                        .endClass()
                        .build();

        assertThat(code).contains("  private int count;\n");
    }

    @Test
    public void testFieldWithInit() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .fieldWithInit("private final", "String", "name", "\"default\"")
                        .endClass()
                        .build();

        assertThat(code).contains("  private final String name = \"default\";\n");
    }

    // ==================== Constructor Tests ====================

    @Test
    public void testConstructor() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginConstructor("public", "TestClass", "int value")
                        .stmt("this.value = value")
                        .endConstructor()
                        .endClass()
                        .build();

        assertThat(code).contains("  public TestClass(int value) throws Exception {\n");
        assertThat(code).contains("    this.value = value;\n");
        assertThat(code).contains("  }\n");
    }

    @Test
    public void testConstructorWithEmptyParams() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginConstructor("public", "TestClass", "")
                        .endConstructor()
                        .endClass()
                        .build();

        assertThat(code).contains("  public TestClass() throws Exception {\n");
    }

    // ==================== Method Tests ====================

    @Test
    public void testMethod() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "doSomething", "")
                        .stmt("System.out.println(\"hello\")")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("  public void doSomething() {\n");
        assertThat(code).contains("    System.out.println(\"hello\");\n");
    }

    @Test
    public void testMethodWithOverride() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .override()
                        .beginMethod("public", "String", "toString", "")
                        .returnStmt("\"TestClass\"")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("  @Override\n");
        assertThat(code).contains("  public String toString() {\n");
    }

    @Test
    public void testMethodWithParams() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "int", "add", "int a, int b")
                        .returnStmt("a + b")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("  public int add(int a, int b) {\n");
        assertThat(code).contains("    return a + b;\n");
    }

    // ==================== Control Flow Tests ====================

    @Test
    public void testIfStatement() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "test", "int x")
                        .beginIf("x > 0")
                        .stmt("System.out.println(\"positive\")")
                        .endIf()
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("    if (x > 0) {\n");
        assertThat(code).contains("      System.out.println(\"positive\");\n");
        assertThat(code).contains("    }\n");
    }

    @Test
    public void testIfElseStatement() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "test", "int x")
                        .beginIf("x > 0")
                        .stmt("System.out.println(\"positive\")")
                        .beginElse()
                        .stmt("System.out.println(\"non-positive\")")
                        .endIf()
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("    if (x > 0) {\n");
        assertThat(code).contains("    } else {\n");
        assertThat(code).contains("      System.out.println(\"non-positive\");\n");
    }

    @Test
    public void testIfElseIfElseStatement() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "String", "classify", "int x")
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

        assertThat(code).contains("    if (x > 0) {\n");
        assertThat(code).contains("    } else if (x < 0) {\n");
        assertThat(code).contains("    } else {\n");
    }

    @Test
    public void testForLoop() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "int", "sum", "int n")
                        .declare("int", "result", "0")
                        .beginFor("int i = 0", "i < n", "i++")
                        .stmt("result += i")
                        .endFor()
                        .returnStmt("result")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("    for (int i = 0; i < n; i++) {\n");
        assertThat(code).contains("      result += i;\n");
        assertThat(code).contains("    }\n");
    }

    @Test
    public void testNestedControlFlow() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "nested", "int[][] matrix")
                        .beginFor("int i = 0", "i < matrix.length", "i++")
                        .beginFor("int j = 0", "j < matrix[i].length", "j++")
                        .beginIf("matrix[i][j] > 0")
                        .stmt("System.out.println(matrix[i][j])")
                        .endIf()
                        .endFor()
                        .endFor()
                        .endMethod()
                        .endClass()
                        .build();

        // Verify 5 levels of indentation for innermost statement (2 spaces each)
        // Level 1: class, Level 2: method, Level 3: outer for, Level 4: inner for, Level 5: if
        assertThat(code).contains("          System.out.println(matrix[i][j]);\n");
    }

    // ==================== Statement Tests ====================

    @Test
    public void testDeclare() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "test", "")
                        .declare("String", "name", "\"test\"")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("    String name = \"test\";\n");
    }

    @Test
    public void testAssign() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "test", "")
                        .assign("this.value", "42")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("    this.value = 42;\n");
    }

    @Test
    public void testContinueStatement() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "test", "")
                        .beginFor("int i = 0", "i < 10", "i++")
                        .beginIf("i % 2 == 0")
                        .continueStmt()
                        .endIf()
                        .endFor()
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("        continue;\n");
    }

    @Test
    public void testBreakStatement() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "test", "")
                        .beginFor("int i = 0", "i < 10", "i++")
                        .beginIf("i == 5")
                        .breakStmt()
                        .endIf()
                        .endFor()
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("        break;\n");
    }

    // ==================== Raw Code Tests ====================

    @Test
    public void testRaw() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "test", "")
                        .raw("// This is a comment\nint x = 1;")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("    // This is a comment\n");
        assertThat(code).contains("    int x = 1;\n");
    }

    @Test
    public void testRawWithEmptyString() {
        String code = builder.beginClass("public", "TestClass", null).raw("").endClass().build();

        // Empty raw should not add anything
        assertThat(code).isEqualTo("public class TestClass {\n}\n");
    }

    @Test
    public void testRawWithNull() {
        String code = builder.beginClass("public", "TestClass", null).raw(null).endClass().build();

        // Null raw should not add anything
        assertThat(code).isEqualTo("public class TestClass {\n}\n");
    }

    @Test
    public void testRawUnindented() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .rawUnindented("// No indent")
                        .endClass()
                        .build();

        assertThat(code).contains("// No indent\n");
        // Should NOT have leading spaces
        assertThat(code).doesNotContain("  // No indent");
    }

    @Test
    public void testRawUnindentedWithoutNewline() {
        String code = builder.rawUnindented("line without newline").build();

        assertThat(code).isEqualTo("line without newline\n");
    }

    @Test
    public void testRawUnindentedWithEmptyString() {
        String code = builder.rawUnindented("").build();

        assertThat(code).isEmpty();
    }

    @Test
    public void testRawUnindentedWithNull() {
        String code = builder.rawUnindented(null).build();

        assertThat(code).isEmpty();
    }

    // ==================== Newline Tests ====================

    @Test
    public void testNewLine() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .field("private", "int", "a")
                        .newLine()
                        .field("private", "int", "b")
                        .endClass()
                        .build();

        assertThat(code).contains("  private int a;\n\n  private int b;\n");
    }

    // ==================== Indentation Tests ====================

    @Test
    public void testIndentationLevels() {
        String code =
                builder.beginClass("public", "TestClass", null) // level 0 -> 1
                        .beginMethod("public", "void", "test", "") // level 1 -> 2
                        .beginIf("true") // level 2 -> 3
                        .beginFor("int i = 0", "i < 1", "i++") // level 3 -> 4
                        .stmt("x = i") // level 4
                        .endFor() // level 4 -> 3
                        .endIf() // level 3 -> 2
                        .endMethod() // level 2 -> 1
                        .endClass() // level 1 -> 0
                        .build();

        // Verify correct indentation at each level
        assertThat(code).contains("public class TestClass"); // 0 spaces
        assertThat(code).contains("  public void test()"); // 2 spaces
        assertThat(code).contains("    if (true)"); // 4 spaces
        assertThat(code).contains("      for (int i = 0"); // 6 spaces
        assertThat(code).contains("        x = i;"); // 8 spaces
    }

    // ==================== toString Tests ====================

    @Test
    public void testToString() {
        builder.beginClass("public", "TestClass", null).endClass();

        assertThat(builder.toString()).isEqualTo(builder.build());
    }

    // ==================== Fluent API Tests ====================

    @Test
    public void testFluentChaining() {
        // Verify all methods return the builder for chaining
        JavaCodeBuilder result =
                new JavaCodeBuilder()
                        .beginClass("public", "Test", "Interface")
                        .field("private", "int", "x")
                        .fieldWithInit("private", "int", "y", "0")
                        .newLine()
                        .beginConstructor("public", "Test", "")
                        .stmt("x = 1")
                        .endConstructor()
                        .override()
                        .beginMethod("public", "void", "test", "")
                        .declare("int", "a", "1")
                        .assign("a", "2")
                        .beginIf("a > 0")
                        .returnStmt("void")
                        .beginElseIf("a < 0")
                        .continueStmt()
                        .beginElse()
                        .breakStmt()
                        .endIf()
                        .beginFor("int i = 0", "i < 1", "i++")
                        .raw("// comment")
                        .endFor()
                        .endMethod()
                        .endClass();

        assertThat(result).isNotNull();
        assertThat(result.build()).isNotEmpty();
    }

    // ==================== Complex Integration Test ====================

    @Test
    public void testCompleteClassGeneration() {
        String code =
                builder.beginClass("public final", "Calculator", "Computable")
                        .field("private", "int", "result")
                        .newLine()
                        .beginConstructor("public", "Calculator", "Object[] references")
                        .stmt("this.result = 0")
                        .endConstructor()
                        .newLine()
                        .override()
                        .beginMethod("public", "int", "add", "int a, int b")
                        .declare("int", "sum", "a + b")
                        .assign("this.result", "sum")
                        .returnStmt("sum")
                        .endMethod()
                        .newLine()
                        .beginMethod("public", "int", "factorial", "int n")
                        .beginIf("n <= 1")
                        .returnStmt("1")
                        .endIf()
                        .declare("int", "result", "1")
                        .beginFor("int i = 2", "i <= n", "i++")
                        .stmt("result *= i")
                        .endFor()
                        .returnStmt("result")
                        .endMethod()
                        .endClass()
                        .build();

        // Verify structure
        assertThat(code).startsWith("public final class Calculator implements Computable {\n");
        assertThat(code).endsWith("}\n");
        assertThat(code).contains("private int result;");
        assertThat(code).contains("public Calculator(Object[] references) throws Exception {");
        assertThat(code).contains("@Override");
        assertThat(code).contains("public int add(int a, int b) {");
        assertThat(code).contains("public int factorial(int n) {");
    }

    // ==================== Edge Cases ====================

    @Test
    public void testSpecialCharactersInStrings() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "test", "")
                        .stmt("String s = \"hello\\nworld\\t!\"")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("String s = \"hello\\nworld\\t!\";");
    }

    @Test
    public void testUnicodeInCode() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "test", "")
                        .stmt("String s = \"你好世界\"")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("String s = \"你好世界\";");
    }

    @Test
    public void testEmptyMethodBody() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod("public", "void", "empty", "")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("  public void empty() {\n  }\n");
    }

    @Test
    public void testRawWithOnlyWhitespace() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .raw("   \n   \n   ")
                        .endClass()
                        .build();

        // Whitespace-only lines should be skipped
        assertThat(code).isEqualTo("public class TestClass {\n}\n");
    }

    // ==================== Type-Safe API Tests ====================

    @Test
    public void testModifierEnum() {
        assertThat(Modifier.PUBLIC.toString()).isEqualTo("public");
        assertThat(Modifier.PRIVATE.toString()).isEqualTo("private");
        assertThat(Modifier.PROTECTED.toString()).isEqualTo("protected");
        assertThat(Modifier.STATIC.toString()).isEqualTo("static");
        assertThat(Modifier.FINAL.toString()).isEqualTo("final");
        assertThat(Modifier.TRANSIENT.toString()).isEqualTo("transient");
    }

    @Test
    public void testPrimitiveTypeEnum() {
        assertThat(PrimitiveType.BOOLEAN.toString()).isEqualTo("boolean");
        assertThat(PrimitiveType.BYTE.toString()).isEqualTo("byte");
        assertThat(PrimitiveType.CHAR.toString()).isEqualTo("char");
        assertThat(PrimitiveType.SHORT.toString()).isEqualTo("short");
        assertThat(PrimitiveType.INT.toString()).isEqualTo("int");
        assertThat(PrimitiveType.LONG.toString()).isEqualTo("long");
        assertThat(PrimitiveType.FLOAT.toString()).isEqualTo("float");
        assertThat(PrimitiveType.DOUBLE.toString()).isEqualTo("double");
        assertThat(PrimitiveType.VOID.toString()).isEqualTo("void");
    }

    @Test
    public void testModsHelper() {
        assertThat(mods(PUBLIC)).isEqualTo("public");
        assertThat(mods(PUBLIC, FINAL)).isEqualTo("public final");
        assertThat(mods(PRIVATE, TRANSIENT)).isEqualTo("private transient");
        assertThat(mods(PUBLIC, Modifier.STATIC, FINAL)).isEqualTo("public static final");
    }

    @Test
    public void testTypeOfHelper() {
        assertThat(typeOf(String.class)).isEqualTo("java.lang.String");
        assertThat(typeOf(Serializable.class)).isEqualTo("java.io.Serializable");
        assertThat(typeOf(JavaCodeBuilder.class))
                .isEqualTo("org.apache.fluss.codegen.JavaCodeBuilder");
    }

    @Test
    public void testArrayOfHelper() {
        assertThat(arrayOf(INT)).isEqualTo("int[]");
        assertThat(arrayOf(BOOLEAN)).isEqualTo("boolean[]");
        assertThat(arrayOf(String.class)).isEqualTo("java.lang.String[]");
        assertThat(arrayOf("Object")).isEqualTo("Object[]");
    }

    @Test
    public void testTypeSafeClassDeclaration() {
        String code =
                builder.beginClass(
                                new Modifier[] {PUBLIC, FINAL},
                                "MyClass",
                                typeOf(Serializable.class))
                        .endClass()
                        .build();

        assertThat(code)
                .isEqualTo("public final class MyClass implements java.io.Serializable {\n}\n");
    }

    @Test
    public void testTypeSafeFieldWithPrimitiveType() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .field(new Modifier[] {PRIVATE}, INT, "count")
                        .field(new Modifier[] {PRIVATE, FINAL}, BOOLEAN, "flag")
                        .endClass()
                        .build();

        assertThat(code).contains("  private int count;\n");
        assertThat(code).contains("  private final boolean flag;\n");
    }

    @Test
    public void testTypeSafeFieldWithSingleModifier() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .field(PRIVATE, INT, "value")
                        .field(PRIVATE, "String", "name")
                        .endClass()
                        .build();

        assertThat(code).contains("  private int value;\n");
        assertThat(code).contains("  private String name;\n");
    }

    @Test
    public void testTypeSafeFieldWithInit() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .fieldWithInit(new Modifier[] {PRIVATE, FINAL}, INT, "count", "0")
                        .fieldWithInit(new Modifier[] {PRIVATE}, "String", "name", "\"default\"")
                        .endClass()
                        .build();

        assertThat(code).contains("  private final int count = 0;\n");
        assertThat(code).contains("  private String name = \"default\";\n");
    }

    @Test
    public void testTypeSafeConstructor() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginConstructor(PUBLIC, "TestClass", "Object[] refs")
                        .stmt("// init")
                        .endConstructor()
                        .endClass()
                        .build();

        assertThat(code).contains("  public TestClass(Object[] refs) throws Exception {\n");
    }

    @Test
    public void testTypeSafeMethodWithPrimitiveReturn() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod(PUBLIC, BOOLEAN, "isValid", "")
                        .returnStmt("true")
                        .endMethod()
                        .beginMethod(PUBLIC, VOID, "doNothing", "")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("  public boolean isValid() {\n");
        assertThat(code).contains("  public void doNothing() {\n");
    }

    @Test
    public void testTypeSafeMethodWithStringReturn() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod(PUBLIC, "String", "getName", "")
                        .returnStmt("\"test\"")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("  public String getName() {\n");
    }

    @Test
    public void testTypeSafeDeclare() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod(PUBLIC, VOID, "test", "")
                        .declare(INT, "count", "0")
                        .declare(BOOLEAN, "flag", "true")
                        .declare("String", "name", "\"test\"")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("    int count = 0;\n");
        assertThat(code).contains("    boolean flag = true;\n");
        assertThat(code).contains("    String name = \"test\";\n");
    }

    @Test
    public void testCompleteTypeSafeExample() {
        String code =
                builder.beginClass(
                                new Modifier[] {PUBLIC, FINAL},
                                "Calculator",
                                typeOf(Serializable.class))
                        .field(new Modifier[] {PRIVATE}, INT, "result")
                        .newLine()
                        .beginConstructor(PUBLIC, "Calculator", "Object[] refs")
                        .stmt("this.result = 0")
                        .endConstructor()
                        .newLine()
                        .override()
                        .beginMethod(PUBLIC, BOOLEAN, "equals", "Object other")
                        .beginIf("other == null")
                        .returnStmt("false")
                        .endIf()
                        .returnStmt("this == other")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("public final class Calculator implements java.io.Serializable");
        assertThat(code).contains("private int result;");
        assertThat(code).contains("public Calculator(Object[] refs) throws Exception");
        assertThat(code).contains("public boolean equals(Object other)");
    }

    // ==================== Type-Safe Parameter API Tests ====================

    @Test
    public void testParamWithPrimitiveType() {
        Param param = of(INT, "count");
        assertThat(param.getType()).isEqualTo("int");
        assertThat(param.getName()).isEqualTo("count");
        assertThat(param.toString()).isEqualTo("int count");
    }

    @Test
    public void testParamWithClassType() {
        Param param = of(String.class, "name");
        assertThat(param.getType()).isEqualTo("java.lang.String");
        assertThat(param.getName()).isEqualTo("name");
        assertThat(param.toString()).isEqualTo("java.lang.String name");
    }

    @Test
    public void testParamWithStringType() {
        Param param = of("InternalRow", "row");
        assertThat(param.getType()).isEqualTo("InternalRow");
        assertThat(param.getName()).isEqualTo("row");
        assertThat(param.toString()).isEqualTo("InternalRow row");
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
    public void testConstructorWithTypeSafeParams() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginConstructor(PUBLIC, "TestClass", of(arrayOf(Object.class), "refs"))
                        .stmt("// init")
                        .endConstructor()
                        .endClass()
                        .build();

        assertThat(code)
                .contains("  public TestClass(java.lang.Object[] refs) throws Exception {\n");
    }

    @Test
    public void testConstructorWithMultipleTypeSafeParams() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginConstructor(
                                PUBLIC,
                                "TestClass",
                                of(INT, "id"),
                                of(String.class, "name"),
                                of(BOOLEAN, "active"))
                        .stmt("this.id = id")
                        .endConstructor()
                        .endClass()
                        .build();

        assertThat(code)
                .contains(
                        "  public TestClass(int id, java.lang.String name, boolean active) throws Exception {\n");
    }

    @Test
    public void testConstructorWithNoParams() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginConstructor(PUBLIC, "TestClass")
                        .stmt("// default")
                        .endConstructor()
                        .endClass()
                        .build();

        assertThat(code).contains("  public TestClass() throws Exception {\n");
    }

    @Test
    public void testMethodWithTypeSafeParams() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod(
                                PUBLIC,
                                BOOLEAN,
                                "equals",
                                of("InternalRow", "left"),
                                of("InternalRow", "right"))
                        .returnStmt("left.equals(right)")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code)
                .contains("  public boolean equals(InternalRow left, InternalRow right) {\n");
    }

    @Test
    public void testMethodWithTypeSafeParamsAndStringReturn() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod(
                                PUBLIC,
                                "String",
                                "format",
                                of(String.class, "pattern"),
                                of(arrayOf(Object.class), "args"))
                        .returnStmt("String.format(pattern, args)")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code)
                .contains(
                        "  public String format(java.lang.String pattern, java.lang.Object[] args) {\n");
    }

    @Test
    public void testMethodWithNoParams() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod(PUBLIC, VOID, "doNothing")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("  public void doNothing() {\n");
    }

    @Test
    public void testMethodWithSingleParam() {
        String code =
                builder.beginClass("public", "TestClass", null)
                        .beginMethod(PUBLIC, INT, "increment", of(INT, "value"))
                        .returnStmt("value + 1")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code).contains("  public int increment(int value) {\n");
    }

    @Test
    public void testCompleteTypeSafeParamExample() {
        String code =
                builder.beginClass(
                                new Modifier[] {PUBLIC, FINAL},
                                "RecordComparator",
                                typeOf(Serializable.class))
                        .field(new Modifier[] {PRIVATE}, "RecordEqualiser", "equaliser")
                        .newLine()
                        .beginConstructor(
                                PUBLIC, "RecordComparator", of(arrayOf(Object.class), "refs"))
                        .stmt("this.equaliser = null")
                        .endConstructor()
                        .newLine()
                        .override()
                        .beginMethod(
                                PUBLIC,
                                BOOLEAN,
                                "equals",
                                of("InternalRow", "left"),
                                of("InternalRow", "right"))
                        .beginIf("left == null && right == null")
                        .returnStmt("true")
                        .endIf()
                        .beginIf("left == null || right == null")
                        .returnStmt("false")
                        .endIf()
                        .returnStmt("equaliser.equals(left, right)")
                        .endMethod()
                        .endClass()
                        .build();

        assertThat(code)
                .contains("public final class RecordComparator implements java.io.Serializable");
        assertThat(code).contains("private RecordEqualiser equaliser;");
        assertThat(code)
                .contains("public RecordComparator(java.lang.Object[] refs) throws Exception");
        assertThat(code).contains("public boolean equals(InternalRow left, InternalRow right)");
    }
}
