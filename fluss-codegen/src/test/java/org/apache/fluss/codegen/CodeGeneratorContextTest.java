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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CodeGeneratorContext}.
 *
 * <p>Test coverage includes:
 *
 * <ul>
 *   <li>Name generation uniqueness (including concurrent scenarios)
 *   <li>Member statement management
 *   <li>Reusable object handling (serialization, deep copy)
 *   <li>Init statement management
 *   <li>References array correctness
 *   <li>Edge cases and error handling
 * </ul>
 */
public class CodeGeneratorContextTest {

    private CodeGeneratorContext context;

    @BeforeEach
    public void setUp() {
        context = new CodeGeneratorContext();
    }

    // ==================== Name Generation Tests ====================

    @Test
    public void testNewNameGeneratesUniqueNames() {
        String name1 = CodeGeneratorContext.newName("field");
        String name2 = CodeGeneratorContext.newName("field");
        String name3 = CodeGeneratorContext.newName("field");

        assertThat(name1).startsWith("field$");
        assertThat(name2).startsWith("field$");
        assertThat(name3).startsWith("field$");

        // All names should be unique
        assertThat(name1).isNotEqualTo(name2);
        assertThat(name2).isNotEqualTo(name3);
        assertThat(name1).isNotEqualTo(name3);
    }

    @Test
    public void testNewNameWithDifferentPrefixes() {
        String name1 = CodeGeneratorContext.newName("left");
        String name2 = CodeGeneratorContext.newName("right");

        assertThat(name1).startsWith("left$");
        assertThat(name2).startsWith("right$");
        assertThat(name1).isNotEqualTo(name2);
    }

    @Test
    public void testNewNameThreadSafety() throws InterruptedException {
        int threadCount = 10;
        int namesPerThread = 100;
        Set<String> allNames = new HashSet<>();
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(
                    () -> {
                        try {
                            Set<String> localNames = new HashSet<>();
                            for (int i = 0; i < namesPerThread; i++) {
                                localNames.add(CodeGeneratorContext.newName("concurrent"));
                            }
                            synchronized (allNames) {
                                allNames.addAll(localNames);
                            }
                        } finally {
                            latch.countDown();
                        }
                    });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // All names should be unique across all threads
        assertThat(allNames).hasSize(threadCount * namesPerThread);
    }

    // ==================== Member Statement Tests ====================

    @Test
    public void testAddReusableMember() {
        context.addReusableMember("private int count;");
        context.addReusableMember("private String name;");

        String code = context.reuseMemberCode();
        assertThat(code).contains("private int count;");
        assertThat(code).contains("private String name;");
    }

    @Test
    public void testMemberStatementDeduplication() {
        context.addReusableMember("private int count;");
        context.addReusableMember("private int count;");
        context.addReusableMember("private int count;");

        String code = context.reuseMemberCode();
        // Should only appear once due to LinkedHashSet
        // Count occurrences by finding all matches
        int count = countOccurrences(code, "private int count;");
        assertThat(count).isEqualTo(1);
    }

    @Test
    public void testMemberStatementOrdering() {
        context.addReusableMember("private int a;");
        context.addReusableMember("private int b;");
        context.addReusableMember("private int c;");

        String code = context.reuseMemberCode();
        // LinkedHashSet preserves insertion order
        int posA = code.indexOf("private int a;");
        int posB = code.indexOf("private int b;");
        int posC = code.indexOf("private int c;");

        assertThat(posA).isLessThan(posB);
        assertThat(posB).isLessThan(posC);
    }

    // ==================== Init Statement Tests ====================

    @Test
    public void testAddReusableInitStatement() {
        context.addReusableInitStatement("this.count = 0;");
        context.addReusableInitStatement("this.name = \"default\";");

        String code = context.reuseInitCode();
        assertThat(code).contains("this.count = 0;");
        assertThat(code).contains("this.name = \"default\";");
    }

    @Test
    public void testInitStatementDeduplication() {
        context.addReusableInitStatement("this.count = 0;");
        context.addReusableInitStatement("this.count = 0;");

        String code = context.reuseInitCode();
        int count = countOccurrences(code, "this.count = 0;");
        assertThat(count).isEqualTo(1);
    }

    // ==================== Reusable Object Tests ====================

    @Test
    public void testAddReusableObject() {
        TestSerializable obj = new TestSerializable("test", 42);
        String fieldTerm = context.addReusableObject(obj, "testObj", "TestSerializable");

        assertThat(fieldTerm).startsWith("testObj$");

        // Check member code
        String memberCode = context.reuseMemberCode();
        assertThat(memberCode).contains("private transient TestSerializable " + fieldTerm + ";");

        // Check init code
        String initCode = context.reuseInitCode();
        assertThat(initCode).contains(fieldTerm + " = ((TestSerializable) references[0]);");

        // Check references
        Object[] refs = context.getReferences();
        assertThat(refs).hasSize(1);
        assertThat(refs[0]).isInstanceOf(TestSerializable.class);
        TestSerializable refObj = (TestSerializable) refs[0];
        assertThat(refObj.name).isEqualTo("test");
        assertThat(refObj.value).isEqualTo(42);
    }

    @Test
    public void testAddMultipleReusableObjects() {
        TestSerializable obj1 = new TestSerializable("first", 1);
        TestSerializable obj2 = new TestSerializable("second", 2);

        String field1 = context.addReusableObject(obj1, "obj", "TestSerializable");
        String field2 = context.addReusableObject(obj2, "obj", "TestSerializable");

        Object[] refs = context.getReferences();
        assertThat(refs).hasSize(2);

        String initCode = context.reuseInitCode();
        assertThat(initCode).contains(field1 + " = ((TestSerializable) references[0]);");
        assertThat(initCode).contains(field2 + " = ((TestSerializable) references[1]);");
    }

    @Test
    public void testReusableObjectDeepCopy() {
        TestSerializable original = new TestSerializable("original", 100);
        context.addReusableObject(original, "obj", "TestSerializable");

        // Modify original
        original.name = "modified";
        original.value = 999;

        // Reference should still have original values (deep copy)
        Object[] refs = context.getReferences();
        TestSerializable refObj = (TestSerializable) refs[0];
        assertThat(refObj.name).isEqualTo("original");
        assertThat(refObj.value).isEqualTo(100);
    }

    // ==================== References Tests ====================

    @Test
    public void testGetReferences() {
        // Empty context returns empty array
        assertThat(context.getReferences()).isEmpty();

        // Add object and verify
        TestSerializable obj = new TestSerializable("test", 1);
        context.addReusableObject(obj, "obj", "TestSerializable");

        Object[] refs1 = context.getReferences();
        Object[] refs2 = context.getReferences();

        // Should return new array each time but with same content
        assertThat(refs1).isNotSameAs(refs2);
        assertThat(refs1).containsExactly(refs2);
    }

    // ==================== Integration Tests ====================

    @Test
    public void testCompleteCodeGeneration() {
        // Simulate a complete code generation scenario
        TestSerializable comparator = new TestSerializable("comparator", 1);
        String comparatorField =
                context.addReusableObject(comparator, "comparator", "TestSerializable");

        context.addReusableMember("private int cachedHash;");

        // Verify all code sections
        String memberCode = context.reuseMemberCode();
        assertThat(memberCode).contains("private transient TestSerializable " + comparatorField);
        assertThat(memberCode).contains("private int cachedHash;");

        String initCode = context.reuseInitCode();
        assertThat(initCode).contains(comparatorField + " = ((TestSerializable) references[0]);");

        Object[] refs = context.getReferences();
        assertThat(refs).hasSize(1);
    }

    @Test
    public void testEmptyContext() {
        // Fresh context should return empty strings/arrays
        assertThat(context.reuseMemberCode()).isEmpty();
        assertThat(context.reuseInitCode()).isEmpty();
        assertThat(context.getReferences()).isEmpty();
    }

    // ==================== Helper Classes ====================

    /** Test serializable class for object reference tests. */
    private static class TestSerializable implements Serializable {
        private static final long serialVersionUID = 1L;
        String name;
        int value;

        TestSerializable(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }

    // ==================== Helper Methods ====================

    /** Counts the number of occurrences of a substring in a string. */
    private static int countOccurrences(String str, String sub) {
        int count = 0;
        int idx = 0;
        while ((idx = str.indexOf(sub, idx)) != -1) {
            count++;
            idx += sub.length();
        }
        return count;
    }
}
