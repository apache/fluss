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

package org.apache.fluss.codegen.generator;

import org.apache.fluss.codegen.CodeGenException;
import org.apache.fluss.codegen.CodeGeneratorContext;
import org.apache.fluss.codegen.GeneratedClass;
import org.apache.fluss.codegen.JavaCodeBuilder;
import org.apache.fluss.codegen.JavaCodeBuilder.Modifier;
import org.apache.fluss.codegen.JavaCodeBuilder.PrimitiveType;
import org.apache.fluss.codegen.types.RecordEqualiser;
import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.TypeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.fluss.codegen.JavaCodeBuilder.Modifier.FINAL;
import static org.apache.fluss.codegen.JavaCodeBuilder.Modifier.PRIVATE;
import static org.apache.fluss.codegen.JavaCodeBuilder.Modifier.PUBLIC;
import static org.apache.fluss.codegen.JavaCodeBuilder.Param.of;
import static org.apache.fluss.codegen.JavaCodeBuilder.PrimitiveType.BOOLEAN;
import static org.apache.fluss.codegen.JavaCodeBuilder.PrimitiveType.INT;
import static org.apache.fluss.codegen.JavaCodeBuilder.arrayOf;
import static org.apache.fluss.codegen.JavaCodeBuilder.typeOf;

/**
 * Code generator for {@link RecordEqualiser} using recursive descent approach.
 *
 * <p>The generator recursively descends into nested types (Row, Array, Map) to generate
 * type-specific comparison code. The core method {@link #genEqualsExpr} dispatches to type-specific
 * generators based on the data type category.
 *
 * <h2>Recursive Descent Structure</h2>
 *
 * <pre>
 * genClass()
 *   ├── genMembers()
 *   ├── genConstructor()
 *   ├── genEqualsMethod()
 *   └── genFieldMethods()
 *         └── genFieldMethod()
 *               ├── genNullCheck()
 *               └── genEqualsExpr()  ← core recursive dispatch
 *                     ├── Primitive  → "left == right"
 *                     ├── Binary     → "Arrays.equals(left, right)"
 *                     ├── Comparable → "left.compareTo(right) == 0"
 *                     ├── Object     → "left.equals(right)"
 *                     └── Composite  → recursive descent:
 *                           ├── genRowEquals()   → nested EqualiserCodeGenerator
 *                           ├── genArrayEquals() → genArrayEqualsMethod()
 *                           │     └── genArrayElemComparison()
 *                           │           └── genNotEqualsExpr() → genEqualsExpr()
 *                           └── genMapEquals()   → genMapEqualsMethod()
 *                                 ├── genMapEntryComparison()
 *                                 │     └── genEqualsExpr() for key
 *                                 └── genMapValueComparison()
 *                                       └── genNotEqualsExpr() → genEqualsExpr()
 * </pre>
 *
 * <h2>Generated Code Example</h2>
 *
 * <p>For a schema with fields: {@code (id INT, name STRING, tags ARRAY<STRING>, metadata
 * MAP<STRING, INT>, address ROW<city STRING, zip INT>)}, the generated code is:
 *
 * <pre>{@code
 * public final class RecordEqualiser$1 implements RecordEqualiser {
 *
 *     // ==================== Member Fields ====================
 *     // Nested equaliser for ROW field, lazily compiled
 *     private GeneratedClass nestedEqualiser$1;
 *     private RecordEqualiser rowEq$1;
 *
 *     // ==================== Constructor ====================
 *     public RecordEqualiser$1(Object[] references) {
 *         nestedEqualiser$1 = (GeneratedClass) references[0];
 *         rowEq$1 = (RecordEqualiser) nestedEqualiser$1.newInstance(
 *             this.getClass().getClassLoader());
 *     }
 *
 *     // ==================== Main Equals Method ====================
 *     @Override
 *     public boolean equals(InternalRow left, InternalRow right) {
 *         // Fast path: BinaryRow direct comparison (only when no projection)
 *         if (left instanceof BinaryRow && right instanceof BinaryRow) {
 *             return left.equals(right);
 *         }
 *         // Field-by-field comparison with short-circuit evaluation
 *         boolean result = true;
 *         result = result && equalsField0(left, right);
 *         result = result && equalsField1(left, right);
 *         result = result && equalsField2(left, right);
 *         result = result && equalsField3(left, right);
 *         result = result && equalsField4(left, right);
 *         return result;
 *     }
 *
 *     // ==================== Field 0: INT (Primitive) ====================
 *     private boolean equalsField0(InternalRow left, InternalRow right) {
 *         boolean leftNull = left.isNullAt(0);
 *         boolean rightNull = right.isNullAt(0);
 *         if (leftNull && rightNull) {
 *             return true;
 *         }
 *         if (leftNull || rightNull) {
 *             return false;
 *         }
 *         int leftVal = left.getInt(0);
 *         int rightVal = right.getInt(0);
 *         return leftVal == rightVal;  // Primitive: direct ==
 *     }
 *
 *     // ==================== Field 1: STRING (Object) ====================
 *     private boolean equalsField1(InternalRow left, InternalRow right) {
 *         boolean leftNull = left.isNullAt(1);
 *         boolean rightNull = right.isNullAt(1);
 *         if (leftNull && rightNull) {
 *             return true;
 *         }
 *         if (leftNull || rightNull) {
 *             return false;
 *         }
 *         BinaryString leftVal = ((BinaryString) left.getString(1));
 *         BinaryString rightVal = ((BinaryString) right.getString(1));
 *         return leftVal.equals(rightVal);  // Object: .equals()
 *     }
 *
 *     // ==================== Field 2: ARRAY<STRING> ====================
 *     private boolean equalsField2(InternalRow left, InternalRow right) {
 *         boolean leftNull = left.isNullAt(2);
 *         boolean rightNull = right.isNullAt(2);
 *         if (leftNull && rightNull) {
 *             return true;
 *         }
 *         if (leftNull || rightNull) {
 *             return false;
 *         }
 *         InternalArray leftVal = left.getArray(2);
 *         InternalArray rightVal = right.getArray(2);
 *         return arrEq$1(leftVal, rightVal);  // Delegate to array method
 *     }
 *
 *     // Array comparison helper with BinaryArray fast path
 *     private boolean arrEq$1(InternalArray left, InternalArray right) {
 *         // Fast path: BinaryArray direct comparison
 *         if (left instanceof BinaryArray && right instanceof BinaryArray) {
 *             return left.equals(right);
 *         }
 *         // Size check
 *         if (left.size() != right.size()) {
 *             return false;
 *         }
 *         // Element-by-element comparison
 *         for (int i = 0; i < left.size(); i++) {
 *             if (left.isNullAt(i) && right.isNullAt(i)) {
 *                 continue;
 *             }
 *             if (left.isNullAt(i) || right.isNullAt(i)) {
 *                 return false;
 *             }
 *             BinaryString l = left.getString(i);
 *             BinaryString r = right.getString(i);
 *             if (!l.equals(r)) {  // Recursive: element type comparison
 *                 return false;
 *             }
 *         }
 *         return true;
 *     }
 *
 *     // ==================== Field 3: MAP<STRING, INT> ====================
 *     private boolean equalsField3(InternalRow left, InternalRow right) {
 *         boolean leftNull = left.isNullAt(3);
 *         boolean rightNull = right.isNullAt(3);
 *         if (leftNull && rightNull) {
 *             return true;
 *         }
 *         if (leftNull || rightNull) {
 *             return false;
 *         }
 *         InternalMap leftVal = left.getMap(3);
 *         InternalMap rightVal = right.getMap(3);
 *         return mapEq$1(leftVal, rightVal);  // Delegate to map method
 *     }
 *
 *     // Map comparison helper with O(n²) key lookup
 *     private boolean mapEq$1(InternalMap left, InternalMap right) {
 *         // Size check
 *         if (left.size() != right.size()) {
 *             return false;
 *         }
 *         // Extract key/value arrays
 *         InternalArray lk = left.keyArray();
 *         InternalArray lv = left.valueArray();
 *         InternalArray rk = right.keyArray();
 *         InternalArray rv = right.valueArray();
 *         // O(n²) comparison: for each left entry, find matching right entry
 *         for (int i = 0; i < left.size(); i++) {
 *             BinaryString lKey = lk.getString(i);
 *             boolean found = false;
 *             for (int j = 0; j < right.size(); j++) {
 *                 BinaryString rKey = rk.getString(j);
 *                 if (lKey.equals(rKey)) {  // Recursive: key type comparison
 *                     // Key matched, compare values
 *                     if (lv.isNullAt(i) && rv.isNullAt(j)) {
 *                         found = true;
 *                         break;
 *                     }
 *                     if (lv.isNullAt(i) || rv.isNullAt(j)) {
 *                         return false;
 *                     }
 *                     int lVal = lv.getInt(i);
 *                     int rVal = rv.getInt(j);
 *                     if (lVal != rVal) {  // Recursive: value type comparison
 *                         return false;
 *                     }
 *                     found = true;
 *                     break;
 *                 }
 *             }
 *             if (!found) {
 *                 return false;
 *             }
 *         }
 *         return true;
 *     }
 *
 *     // ==================== Field 4: ROW<city STRING, zip INT> ====================
 *     private boolean equalsField4(InternalRow left, InternalRow right) {
 *         boolean leftNull = left.isNullAt(4);
 *         boolean rightNull = right.isNullAt(4);
 *         if (leftNull && rightNull) {
 *             return true;
 *         }
 *         if (leftNull || rightNull) {
 *             return false;
 *         }
 *         InternalRow leftVal = left.getRow(4, 2);
 *         InternalRow rightVal = right.getRow(4, 2);
 *         return rowEq$1.equals(leftVal, rightVal);  // Delegate to nested equaliser
 *     }
 * }
 * }</pre>
 *
 * <h2>Optimization Strategies</h2>
 *
 * <ul>
 *   <li><b>BinaryRow fast path:</b> When both rows are BinaryRow instances (and no projection),
 *       direct byte-level comparison via {@code equals()} avoids field-by-field overhead.
 *   <li><b>BinaryArray fast path:</b> Similar optimization for array comparisons.
 *   <li><b>Short-circuit evaluation:</b> Field comparisons are chained with {@code &&} to exit
 *       early on first mismatch.
 *   <li><b>Null handling:</b> Null checks are performed before value access to avoid NPE and
 *       correctly handle null equality semantics.
 *   <li><b>Type-specific comparison:</b> Primitives use {@code ==}, binary uses {@code
 *       Arrays.equals()}, comparable types use {@code compareTo()}, objects use {@code equals()}.
 *   <li><b>Nested equaliser reuse:</b> For ROW fields, a separate equaliser is generated once and
 *       reused across comparisons.
 * </ul>
 *
 * <h2>Supporting Methods</h2>
 *
 * <ul>
 *   <li>Type classification: {@link org.apache.fluss.utils.TypeUtils#isPrimitive}, {@link
 *       org.apache.fluss.utils.TypeUtils#isBinary}, {@link
 *       org.apache.fluss.utils.TypeUtils#isComparable}
 *   <li>Type mapping: {@link #toJavaType}
 *   <li>Field/element access: {@link #genAccess} (unified for Row and Array)
 * </ul>
 */
public class EqualiserCodeGenerator {

    // ==================== Type Name Constants ====================

    private static final String T_RECORD_EQUALISER = typeOf(RecordEqualiser.class);
    private static final String T_ROW_DATA = typeOf(InternalRow.class);
    private static final String T_BINARY_ROW = typeOf(BinaryRow.class);
    private static final String T_BINARY_STRING = typeOf(BinaryString.class);
    private static final String T_DECIMAL = typeOf(Decimal.class);
    private static final String T_TIMESTAMP_NTZ = typeOf(TimestampNtz.class);
    private static final String T_TIMESTAMP_LTZ = typeOf(TimestampLtz.class);
    private static final String T_INTERNAL_ARRAY = typeOf(InternalArray.class);
    private static final String T_INTERNAL_MAP = typeOf(InternalMap.class);
    private static final String T_BINARY_ARRAY = typeOf(BinaryArray.class);
    private static final String T_GENERATED_CLASS = typeOf(GeneratedClass.class);

    private final DataType[] fieldTypes;
    private final int[] fields;

    // ==================== Constructor ====================

    public EqualiserCodeGenerator(DataType[] fieldTypes) {
        this(fieldTypes, IntStream.range(0, fieldTypes.length).toArray());
    }

    public EqualiserCodeGenerator(DataType[] fieldTypes, int[] fields) {
        this.fieldTypes = fieldTypes;
        this.fields = fields;
    }

    // ==================== Public API ====================

    public GeneratedClass<RecordEqualiser> generateRecordEqualiser(String name) {
        CodeGeneratorContext ctx = new CodeGeneratorContext();
        String className = CodeGeneratorContext.newName(name);
        String code = genClass(ctx, className);
        return new GeneratedClass<RecordEqualiser>(className, code, ctx.getReferences());
    }

    // ==================== Class Structure Generation ====================

    private String genClass(CodeGeneratorContext ctx, String className) {
        // Generate field methods first to collect members
        List<String> fieldMethods = genFieldMethods(ctx);

        JavaCodeBuilder b = new JavaCodeBuilder();
        b.beginClass(new Modifier[] {PUBLIC, FINAL}, className, T_RECORD_EQUALISER);

        genMembers(b, ctx);
        genConstructor(b, ctx, className);
        genEqualsMethod(b);
        appendFieldMethods(b, fieldMethods);

        b.endClass();
        return b.build();
    }

    private void genMembers(JavaCodeBuilder b, CodeGeneratorContext ctx) {
        String code = ctx.reuseMemberCode();
        if (!code.isEmpty()) {
            b.rawBlock(code);
            b.newLine();
        }
    }

    private void genConstructor(JavaCodeBuilder b, CodeGeneratorContext ctx, String className) {
        b.beginConstructor(PUBLIC, className, of(arrayOf(Object.class), "references"));
        String code = ctx.reuseInitCode();
        if (!code.isEmpty()) {
            b.raw(code);
        }
        b.endConstructor();
    }

    private void genEqualsMethod(JavaCodeBuilder b) {
        boolean hasProjection = fieldTypes.length > fields.length;

        b.newLine();
        b.override();
        b.beginMethod(PUBLIC, BOOLEAN, "equals", of(T_ROW_DATA, "left"), of(T_ROW_DATA, "right"));

        // BinaryRow fast path
        if (!hasProjection) {
            b.beginIf("left instanceof " + T_BINARY_ROW + " && right instanceof " + T_BINARY_ROW);
            b.returnStmt("left.equals(right)");
            b.endIf();
        }

        // Field comparison
        b.declare(BOOLEAN, "result", "true");
        for (int idx : fields) {
            b.assign("result", "result && equalsField" + idx + "(left, right)");
        }
        b.returnStmt("result");
        b.endMethod();
    }

    private List<String> genFieldMethods(CodeGeneratorContext ctx) {
        List<String> methods = new ArrayList<String>();
        for (int idx : fields) {
            methods.add(genFieldMethod(ctx, idx));
        }
        return methods;
    }

    private void appendFieldMethods(JavaCodeBuilder b, List<String> methods) {
        for (String method : methods) {
            b.newLine();
            b.rawBlock(method);
        }
    }

    // ==================== Field Comparison Method ====================

    private String genFieldMethod(CodeGeneratorContext ctx, int idx) {
        DataType type = fieldTypes[idx];
        String javaType = toJavaType(type);

        JavaCodeBuilder b = new JavaCodeBuilder();
        b.beginMethod(
                PRIVATE,
                BOOLEAN,
                "equalsField" + idx,
                of(T_ROW_DATA, "left"),
                of(T_ROW_DATA, "right"));

        // Null check
        genNullCheck(b, idx);

        // Read values
        b.declare(javaType, "leftVal", genFieldAccess("left", idx, type));
        b.declare(javaType, "rightVal", genFieldAccess("right", idx, type));

        // Compare
        b.returnStmt(genEqualsExpr(ctx, type, "leftVal", "rightVal"));

        b.endMethod();
        return b.build();
    }

    private void genNullCheck(JavaCodeBuilder b, int idx) {
        b.declare(BOOLEAN, "leftNull", "left.isNullAt(" + idx + ")");
        b.declare(BOOLEAN, "rightNull", "right.isNullAt(" + idx + ")");
        b.beginIf("leftNull && rightNull");
        b.returnStmt("true");
        b.endIf();
        b.beginIf("leftNull || rightNull");
        b.returnStmt("false");
        b.endIf();
    }

    // ==================== Equals Expression (Recursive Descent) ====================

    /**
     * Generates equals expression by recursively descending into the type structure. This is the
     * core recursive descent method.
     */
    private String genEqualsExpr(
            CodeGeneratorContext ctx, DataType type, String left, String right) {
        DataTypeRoot root = type.getTypeRoot();

        // Primitive: ==
        if (TypeUtils.isPrimitive(root)) {
            return left + " == " + right;
        }

        // Binary: Arrays.equals
        if (TypeUtils.isBinary(root)) {
            return "java.util.Arrays.equals(" + left + ", " + right + ")";
        }

        // Comparable: compareTo
        if (TypeUtils.isComparable(root)) {
            return left + ".compareTo(" + right + ") == 0";
        }

        // Composite: recursive descent
        if (root == DataTypeRoot.ROW) {
            return genRowEquals(ctx, type, left, right);
        }
        if (root == DataTypeRoot.ARRAY) {
            return genArrayEquals(ctx, type, left, right);
        }
        if (root == DataTypeRoot.MAP) {
            return genMapEquals(ctx, type, left, right);
        }

        // Object: equals()
        return left + ".equals(" + right + ")";
    }

    /** Generates not-equals expression (inverse of genEqualsExpr). */
    private String genNotEqualsExpr(
            CodeGeneratorContext ctx, DataType type, String left, String right) {
        DataTypeRoot root = type.getTypeRoot();

        if (TypeUtils.isPrimitive(root)) {
            return left + " != " + right;
        }
        if (TypeUtils.isBinary(root)) {
            return "!java.util.Arrays.equals(" + left + ", " + right + ")";
        }
        if (TypeUtils.isComparable(root)) {
            return left + ".compareTo(" + right + ") != 0";
        }

        // For composite and object types, negate the equals expression
        return "!" + genEqualsExpr(ctx, type, left, right);
    }

    // ==================== Row Equals ====================

    private String genRowEquals(
            CodeGeneratorContext ctx, DataType rowType, String left, String right) {
        // Generate nested equaliser
        List<DataType> nestedTypes = DataTypeChecks.getFieldTypes(rowType);
        EqualiserCodeGenerator nestedGen =
                new EqualiserCodeGenerator(nestedTypes.toArray(new DataType[0]));
        GeneratedClass<RecordEqualiser> generated =
                nestedGen.generateRecordEqualiser("nestedEqualiser");

        // Register in context
        String genTerm = ctx.addReusableObject(generated, "nestedEqualiser", T_GENERATED_CLASS);
        String instTerm = CodeGeneratorContext.newName("rowEq");

        ctx.addReusableMember("private " + T_RECORD_EQUALISER + " " + instTerm + ";");
        ctx.addReusableInitStatement(
                instTerm
                        + " = ("
                        + T_RECORD_EQUALISER
                        + ") "
                        + genTerm
                        + ".newInstance(this.getClass().getClassLoader());");

        return instTerm + ".equals(" + left + ", " + right + ")";
    }

    // ==================== Array Equals ====================

    private String genArrayEquals(
            CodeGeneratorContext ctx, DataType arrayType, String left, String right) {
        DataType elemType = DataTypeChecks.getArrayElementType(arrayType);
        String methodName = CodeGeneratorContext.newName("arrEq");

        ctx.addReusableMember(genArrayEqualsMethod(ctx, methodName, elemType));
        return methodName + "(" + left + ", " + right + ")";
    }

    private String genArrayEqualsMethod(
            CodeGeneratorContext ctx, String methodName, DataType elemType) {
        String elemJavaType = toJavaType(elemType);

        JavaCodeBuilder b = new JavaCodeBuilder();
        b.beginMethod(
                PRIVATE,
                BOOLEAN,
                methodName,
                of(T_INTERNAL_ARRAY, "left"),
                of(T_INTERNAL_ARRAY, "right"));

        // Fast path
        b.beginIf("left instanceof " + T_BINARY_ARRAY + " && right instanceof " + T_BINARY_ARRAY);
        b.returnStmt("left.equals(right)");
        b.endIf();

        // Size check
        b.beginIf("left.size() != right.size()");
        b.returnStmt("false");
        b.endIf();

        // Element loop
        b.beginFor("int i = 0", "i < left.size()", "i++");
        genArrayElemComparison(b, ctx, elemType, elemJavaType);
        b.endFor();

        b.returnStmt("true");
        b.endMethod();
        return b.build();
    }

    private void genArrayElemComparison(
            JavaCodeBuilder b, CodeGeneratorContext ctx, DataType elemType, String elemJavaType) {
        // Null check
        b.beginIf("left.isNullAt(i) && right.isNullAt(i)");
        b.continueStmt();
        b.endIf();
        b.beginIf("left.isNullAt(i) || right.isNullAt(i)");
        b.returnStmt("false");
        b.endIf();

        // Read and compare (recursive descent into element type)
        b.declare(elemJavaType, "l", genArrayAccess("left", "i", elemType));
        b.declare(elemJavaType, "r", genArrayAccess("right", "i", elemType));
        b.beginIf(genNotEqualsExpr(ctx, elemType, "l", "r"));
        b.returnStmt("false");
        b.endIf();
    }

    // ==================== Map Equals ====================

    private String genMapEquals(
            CodeGeneratorContext ctx, DataType mapType, String left, String right) {
        DataType keyType = DataTypeChecks.getMapKeyType(mapType);
        DataType valType = DataTypeChecks.getMapValueType(mapType);
        String methodName = CodeGeneratorContext.newName("mapEq");

        ctx.addReusableMember(genMapEqualsMethod(ctx, methodName, keyType, valType));
        return methodName + "(" + left + ", " + right + ")";
    }

    private String genMapEqualsMethod(
            CodeGeneratorContext ctx, String methodName, DataType keyType, DataType valType) {
        String keyJavaType = toJavaType(keyType);
        String valJavaType = toJavaType(valType);

        JavaCodeBuilder b = new JavaCodeBuilder();
        b.beginMethod(
                PRIVATE,
                BOOLEAN,
                methodName,
                of(T_INTERNAL_MAP, "left"),
                of(T_INTERNAL_MAP, "right"));

        // Size check
        b.beginIf("left.size() != right.size()");
        b.returnStmt("false");
        b.endIf();

        // Get arrays
        b.declare(T_INTERNAL_ARRAY, "lk", "left.keyArray()");
        b.declare(T_INTERNAL_ARRAY, "lv", "left.valueArray()");
        b.declare(T_INTERNAL_ARRAY, "rk", "right.keyArray()");
        b.declare(T_INTERNAL_ARRAY, "rv", "right.valueArray()");

        // O(n²) comparison
        b.beginFor("int i = 0", "i < left.size()", "i++");
        genMapEntryComparison(b, ctx, keyType, valType, keyJavaType, valJavaType);
        b.endFor();

        b.returnStmt("true");
        b.endMethod();
        return b.build();
    }

    private void genMapEntryComparison(
            JavaCodeBuilder b,
            CodeGeneratorContext ctx,
            DataType keyType,
            DataType valType,
            String keyJavaType,
            String valJavaType) {
        b.declare(keyJavaType, "lKey", genArrayAccess("lk", "i", keyType));
        b.declare(BOOLEAN, "found", "false");

        // Inner loop
        b.beginFor("int j = 0", "j < right.size()", "j++");
        b.declare(keyJavaType, "rKey", genArrayAccess("rk", "j", keyType));

        // Key match (recursive descent into key type)
        b.beginIf(genEqualsExpr(ctx, keyType, "lKey", "rKey"));
        genMapValueComparison(b, ctx, valType, valJavaType);
        b.endIf();

        b.endFor();

        b.beginIf("!found");
        b.returnStmt("false");
        b.endIf();
    }

    private void genMapValueComparison(
            JavaCodeBuilder b, CodeGeneratorContext ctx, DataType valType, String valJavaType) {
        // Null check
        b.beginIf("lv.isNullAt(i) && rv.isNullAt(j)");
        b.assign("found", "true");
        b.breakStmt();
        b.endIf();
        b.beginIf("lv.isNullAt(i) || rv.isNullAt(j)");
        b.returnStmt("false");
        b.endIf();

        // Value comparison (recursive descent into value type)
        b.declare(valJavaType, "lVal", genArrayAccess("lv", "i", valType));
        b.declare(valJavaType, "rVal", genArrayAccess("rv", "j", valType));
        b.beginIf(genNotEqualsExpr(ctx, valType, "lVal", "rVal"));
        b.returnStmt("false");
        b.endIf();

        b.assign("found", "true");
        b.breakStmt();
    }

    // ==================== Type Mapping ====================

    private String toJavaType(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return PrimitiveType.BOOLEAN.toString();
            case TINYINT:
                return PrimitiveType.BYTE.toString();
            case SMALLINT:
                return PrimitiveType.SHORT.toString();
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return INT.toString();
            case BIGINT:
                return PrimitiveType.LONG.toString();
            case FLOAT:
                return PrimitiveType.FLOAT.toString();
            case DOUBLE:
                return PrimitiveType.DOUBLE.toString();
            case CHAR:
            case STRING:
                return T_BINARY_STRING;
            case BINARY:
            case BYTES:
                return "byte[]";
            case DECIMAL:
                return T_DECIMAL;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return T_TIMESTAMP_NTZ;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return T_TIMESTAMP_LTZ;
            case ARRAY:
                return T_INTERNAL_ARRAY;
            case MAP:
                return T_INTERNAL_MAP;
            case ROW:
                return T_ROW_DATA;
            default:
                throw new CodeGenException("Unsupported type: " + type);
        }
    }

    // ==================== Field/Element Access ====================

    private String genFieldAccess(String row, int idx, DataType type) {
        return genAccess(row, String.valueOf(idx), type, true);
    }

    private String genArrayAccess(String arr, String idx, DataType type) {
        return genAccess(arr, idx, type, false);
    }

    /**
     * Unified access code generation for both row fields and array elements.
     *
     * @param container the row or array variable name
     * @param idx the index expression
     * @param type the data type
     * @param isRow true for row field access, false for array element access
     */
    private String genAccess(String container, String idx, DataType type, boolean isRow) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return container + ".getBoolean(" + idx + ")";
            case TINYINT:
                return container + ".getByte(" + idx + ")";
            case SMALLINT:
                return container + ".getShort(" + idx + ")";
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return container + ".getInt(" + idx + ")";
            case BIGINT:
                return container + ".getLong(" + idx + ")";
            case FLOAT:
                return container + ".getFloat(" + idx + ")";
            case DOUBLE:
                return container + ".getDouble(" + idx + ")";
            case CHAR:
                int charLen = DataTypeChecks.getLength(type);
                String charAccess = container + ".getChar(" + idx + ", " + charLen + ")";
                return isRow ? "((" + T_BINARY_STRING + ") " + charAccess + ")" : charAccess;
            case STRING:
                String strAccess = container + ".getString(" + idx + ")";
                return isRow ? "((" + T_BINARY_STRING + ") " + strAccess + ")" : strAccess;
            case BINARY:
                int binLen = DataTypeChecks.getLength(type);
                return container + ".getBinary(" + idx + ", " + binLen + ")";
            case BYTES:
                return container + ".getBytes(" + idx + ")";
            case DECIMAL:
                int p = DataTypeChecks.getPrecision(type);
                int s = DataTypeChecks.getScale(type);
                return container + ".getDecimal(" + idx + ", " + p + ", " + s + ")";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int ntzP = DataTypeChecks.getPrecision(type);
                return container + ".getTimestampNtz(" + idx + ", " + ntzP + ")";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int ltzP = DataTypeChecks.getPrecision(type);
                return container + ".getTimestampLtz(" + idx + ", " + ltzP + ")";
            case ARRAY:
                return container + ".getArray(" + idx + ")";
            case MAP:
                return container + ".getMap(" + idx + ")";
            case ROW:
                int fc = DataTypeChecks.getFieldCount(type);
                return container + ".getRow(" + idx + ", " + fc + ")";
            default:
                throw new CodeGenException("Unsupported type: " + type);
        }
    }
}
