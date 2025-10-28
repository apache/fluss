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

package org.apache.fluss.flink.utils;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

/**
 * Manual test to verify Flink POJO converter with mixed numeric types and widening conversions.
 * This is for Phase 3 manual verification.
 */
public class ManualPojoWideningTest {

    /** Test POJO with mixed numeric types (smaller types). */
    public static class MixedNumericPojo {
        public Byte byteVal;
        public Short shortVal;
        public Integer intVal;
        public Float floatVal;

        public MixedNumericPojo() {}

        public MixedNumericPojo(Byte byteVal, Short shortVal, Integer intVal, Float floatVal) {
            this.byteVal = byteVal;
            this.shortVal = shortVal;
            this.intVal = intVal;
            this.floatVal = floatVal;
        }

        @Override
        public String toString() {
            return "MixedNumericPojo{"
                    + "byteVal="
                    + byteVal
                    + ", shortVal="
                    + shortVal
                    + ", intVal="
                    + intVal
                    + ", floatVal="
                    + floatVal
                    + '}';
        }
    }

    public static void main(String[] args) {
        System.out.println("=== Phase 3 Manual Verification: Flink POJO Widening ===\n");

        // Create a schema with larger types (BIGINT, DOUBLE)
        RowType schema =
                RowType.builder()
                        .field("byteVal", DataTypes.BIGINT())
                        .field("shortVal", DataTypes.BIGINT())
                        .field("intVal", DataTypes.BIGINT())
                        .field("floatVal", DataTypes.DOUBLE())
                        .build();

        System.out.println("Schema: " + schema);
        System.out.println(
                "  byteVal: BIGINT (expecting widening from Byte)\n"
                        + "  shortVal: BIGINT (expecting widening from Short)\n"
                        + "  intVal: BIGINT (expecting widening from Integer)\n"
                        + "  floatVal: DOUBLE (expecting widening from Float)\n");

        // Step 1: Create converter (this should succeed with widening support)
        System.out.println("Step 1: Creating converter...");
        PojoToRowConverter<MixedNumericPojo> converter;
        try {
            converter = new PojoToRowConverter<>(MixedNumericPojo.class, schema);
            System.out.println("✓ Converter created successfully!\n");
        } catch (Exception e) {
            System.out.println("✗ FAILED to create converter: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // Step 2: Create test POJO with mixed numeric values
        System.out.println("Step 2: Creating test POJO...");
        MixedNumericPojo pojo = new MixedNumericPojo((byte) 10, (short) 100, 1000, 10.5f);
        System.out.println("POJO: " + pojo + "\n");

        // Step 3: Convert POJO to GenericRow
        System.out.println("Step 3: Converting POJO to GenericRow...");
        GenericRow row;
        try {
            row = converter.convert(pojo);
            System.out.println("✓ Conversion successful!\n");
        } catch (Exception e) {
            System.out.println("✗ FAILED to convert: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // Step 4: Verify values
        System.out.println("Step 4: Verifying converted values...");
        boolean allCorrect = true;

        Long byteVal = row.getLong(0);
        System.out.println("  byteVal: " + byteVal + " (expected: 10)");
        if (byteVal != 10L) {
            System.out.println("    ✗ INCORRECT!");
            allCorrect = false;
        } else {
            System.out.println("    ✓ Correct");
        }

        Long shortVal = row.getLong(1);
        System.out.println("  shortVal: " + shortVal + " (expected: 100)");
        if (shortVal != 100L) {
            System.out.println("    ✗ INCORRECT!");
            allCorrect = false;
        } else {
            System.out.println("    ✓ Correct");
        }

        Long intVal = row.getLong(2);
        System.out.println("  intVal: " + intVal + " (expected: 1000)");
        if (intVal != 1000L) {
            System.out.println("    ✗ INCORRECT!");
            allCorrect = false;
        } else {
            System.out.println("    ✓ Correct");
        }

        Double floatVal = row.getDouble(3);
        System.out.println("  floatVal: " + floatVal + " (expected: ~10.5)");
        if (Math.abs(floatVal - 10.5) > 0.01) {
            System.out.println("    ✗ INCORRECT!");
            allCorrect = false;
        } else {
            System.out.println("    ✓ Correct");
        }

        // Step 5: Test null handling
        System.out.println("\nStep 5: Testing null handling...");
        MixedNumericPojo nullPojo = new MixedNumericPojo(null, null, null, null);
        GenericRow nullRow = converter.convert(nullPojo);
        boolean nullsCorrect = true;

        for (int i = 0; i < 4; i++) {
            if (!nullRow.isNullAt(i)) {
                System.out.println("  ✗ Field " + i + " should be null but isn't!");
                nullsCorrect = false;
            }
        }
        if (nullsCorrect) {
            System.out.println("  ✓ All null values handled correctly");
        }

        // Final result
        System.out.println("\n=== Manual Verification Result ===");
        if (allCorrect && nullsCorrect) {
            System.out.println("✓ ALL TESTS PASSED!");
            System.out.println("✓ Converter creation succeeded with widening");
            System.out.println("✓ All values converted correctly");
            System.out.println("✓ Null handling works properly");
            System.out.println("\nPhase 3 manual verification: SUCCESSFUL");
        } else {
            System.out.println("✗ SOME TESTS FAILED - Review output above");
        }
    }
}
