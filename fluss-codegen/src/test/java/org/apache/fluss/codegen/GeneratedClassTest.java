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

import org.apache.fluss.utils.InstantiationUtils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link GeneratedClass}.
 *
 * <p>Test coverage includes:
 *
 * <ul>
 *   <li>Instance creation with and without references
 *   <li>Compilation caching within GeneratedClass
 *   <li>Serialization and deserialization
 *   <li>Error handling for invalid code
 *   <li>Null parameter handling
 *   <li>Getter methods
 *   <li>Reference passing to constructor
 * </ul>
 */
public class GeneratedClassTest {

    private static final AtomicInteger CLASS_COUNTER = new AtomicInteger(0);

    // Note: We don't clear the cache in @BeforeEach/@AfterEach because:
    // 1. Tests use unique class names via uniqueClassName()
    // 2. Clearing cache can interfere with parallel test execution
    // 3. The cache is designed to be persistent for performance    // ==================== Basic
    // Instantiation Tests ====================

    @Test
    public void testNewInstanceSimple() {
        String className = uniqueClassName("SimpleGenerated");
        String code =
                "public class "
                        + className
                        + " {\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {}\n"
                        + "  public String hello() { return \"Hello\"; }\n"
                        + "}";

        GeneratedClass<Object> generated = new GeneratedClass<Object>(className, code);
        Object instance = generated.newInstance(Thread.currentThread().getContextClassLoader());

        assertThat(instance).isNotNull();
        assertThat(instance.getClass().getName()).isEqualTo(className);
    }

    @Test
    public void testNewInstanceWithReferences() throws Exception {
        String className = uniqueClassName("WithRefs");
        String code =
                "public class "
                        + className
                        + " {\n"
                        + "  private String message;\n"
                        + "  private int number;\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {\n"
                        + "    this.message = (String) refs[0];\n"
                        + "    this.number = (Integer) refs[1];\n"
                        + "  }\n"
                        + "  public String getMessage() { return message; }\n"
                        + "  public int getNumber() { return number; }\n"
                        + "}";

        Object[] refs = new Object[] {"Hello World", 42};
        GeneratedClass<Object> generated = new GeneratedClass<Object>(className, code, refs);
        Object instance = generated.newInstance(Thread.currentThread().getContextClassLoader());

        String message = (String) instance.getClass().getMethod("getMessage").invoke(instance);
        int number = (int) instance.getClass().getMethod("getNumber").invoke(instance);

        assertThat(message).isEqualTo("Hello World");
        assertThat(number).isEqualTo(42);
    }

    @Test
    public void testNewInstanceWithEmptyReferences() {
        String className = uniqueClassName("EmptyRefs");
        String code =
                "public class "
                        + className
                        + " {\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {\n"
                        + "    if (refs.length != 0) throw new RuntimeException(\"Expected empty refs\");\n"
                        + "  }\n"
                        + "}";

        GeneratedClass<Object> generated = new GeneratedClass<Object>(className, code);
        Object instance = generated.newInstance(Thread.currentThread().getContextClassLoader());

        assertThat(instance).isNotNull();
    }

    // ==================== Compilation Caching Tests ====================

    @Test
    public void testCompileCachesClass() {
        String className = uniqueClassName("CachedCompile");
        String code =
                "public class "
                        + className
                        + " {\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {}\n"
                        + "}";

        GeneratedClass<Object> generated = new GeneratedClass<Object>(className, code);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        Class<Object> class1 = generated.compile(classLoader);
        Class<Object> class2 = generated.compile(classLoader);

        // Should return the same cached class
        assertThat(class1).isSameAs(class2);
    }

    @Test
    public void testMultipleNewInstancesUseSameClass() {
        String className = uniqueClassName("MultiInstance");
        String code =
                "public class "
                        + className
                        + " {\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {}\n"
                        + "}";

        GeneratedClass<Object> generated = new GeneratedClass<Object>(className, code);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        Object instance1 = generated.newInstance(classLoader);
        Object instance2 = generated.newInstance(classLoader);

        // Different instances but same class
        assertThat(instance1).isNotSameAs(instance2);
        assertThat(instance1.getClass()).isSameAs(instance2.getClass());
    }

    // ==================== Serialization Tests ====================

    @Test
    public void testSerializationRoundTrip() throws Exception {
        String className = uniqueClassName("Serializable");
        String code =
                "public class "
                        + className
                        + " implements java.io.Serializable {\n"
                        + "  private static final long serialVersionUID = 1L;\n"
                        + "  private String value;\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {\n"
                        + "    this.value = (String) refs[0];\n"
                        + "  }\n"
                        + "  public String getValue() { return value; }\n"
                        + "}";

        Object[] refs = new Object[] {"test-value"};
        GeneratedClass<Object> original = new GeneratedClass<Object>(className, code, refs);

        // Serialize and deserialize
        byte[] serialized = InstantiationUtils.serializeObject(original);
        GeneratedClass<Object> deserialized =
                InstantiationUtils.deserializeObject(
                        serialized, Thread.currentThread().getContextClassLoader());

        // Verify deserialized GeneratedClass works
        assertThat(deserialized.getClassName()).isEqualTo(className);
        assertThat(deserialized.getCode()).isEqualTo(code);
        assertThat(deserialized.getReferences()).containsExactly(refs);

        // Create instance from deserialized
        Object instance = deserialized.newInstance(Thread.currentThread().getContextClassLoader());
        String value = (String) instance.getClass().getMethod("getValue").invoke(instance);
        assertThat(value).isEqualTo("test-value");
    }

    @Test
    public void testSerializationClearsCompiledClass() throws Exception {
        String className = uniqueClassName("SerialClear");
        String code =
                "public class "
                        + className
                        + " {\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {}\n"
                        + "}";

        GeneratedClass<Object> original = new GeneratedClass<Object>(className, code);

        // Compile the class first
        original.compile(Thread.currentThread().getContextClassLoader());

        // Serialize and deserialize
        byte[] serialized = InstantiationUtils.serializeObject(original);
        GeneratedClass<Object> deserialized =
                InstantiationUtils.deserializeObject(
                        serialized, Thread.currentThread().getContextClassLoader());

        // The deserialized instance should still work (compiledClass is transient)
        Object instance = deserialized.newInstance(Thread.currentThread().getContextClassLoader());
        assertThat(instance).isNotNull();
    }

    // ==================== Getter Tests ====================

    @Test
    public void testGetters() {
        String className = uniqueClassName("TestGetters");
        String code =
                "public class "
                        + className
                        + " {\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {}\n"
                        + "}";
        Object[] refs = new Object[] {"a", 1, true};

        // Test with references
        GeneratedClass<Object> withRefs = new GeneratedClass<Object>(className, code, refs);
        assertThat(withRefs.getClassName()).isEqualTo(className);
        assertThat(withRefs.getCode()).isEqualTo(code);
        assertThat(withRefs.getReferences()).containsExactly("a", 1, true);

        // Test without references
        String className2 = uniqueClassName("TestGetters");
        String code2 =
                "public class " + className2 + " { public " + className2 + "(Object[] refs) {} }";
        GeneratedClass<Object> noRefs = new GeneratedClass<Object>(className2, code2);
        assertThat(noRefs.getReferences()).isEmpty();
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testNewInstanceWithInvalidCode() {
        String className = "InvalidCode";
        String code = "public class " + className + " { invalid syntax }";

        GeneratedClass<Object> generated = new GeneratedClass<Object>(className, code);

        assertThatThrownBy(
                        () -> generated.newInstance(Thread.currentThread().getContextClassLoader()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Could not instantiate generated class");
    }

    @Test
    public void testNewInstanceWithMissingConstructor() {
        String className = uniqueClassName("NoConstructor");
        // Class without Object[] constructor
        String code =
                "public class " + className + " {\n" + "  public " + className + "() {}\n" + "}";

        GeneratedClass<Object> generated = new GeneratedClass<Object>(className, code);

        assertThatThrownBy(
                        () -> generated.newInstance(Thread.currentThread().getContextClassLoader()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Could not instantiate generated class");
    }

    @Test
    public void testNewInstanceWithConstructorException() {
        String className = uniqueClassName("ThrowingConstructor");
        String code =
                "public class "
                        + className
                        + " {\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {\n"
                        + "    throw new RuntimeException(\"Constructor failed\");\n"
                        + "  }\n"
                        + "}";

        GeneratedClass<Object> generated = new GeneratedClass<Object>(className, code);

        assertThatThrownBy(
                        () -> generated.newInstance(Thread.currentThread().getContextClassLoader()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Could not instantiate generated class");
    }

    @Test
    public void testNullParametersThrow() {
        assertThatThrownBy(() -> new GeneratedClass<Object>(null, "code"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("className must not be null");

        assertThatThrownBy(() -> new GeneratedClass<Object>("ClassName", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("code must not be null");

        assertThatThrownBy(() -> new GeneratedClass<Object>("ClassName", "code", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("references must not be null");
    }

    // ==================== Interface Implementation Tests ====================

    @Test
    public void testGeneratedClassImplementsInterface() {
        String className = uniqueClassName("RunnableImpl");
        String code =
                "public class "
                        + className
                        + " implements Runnable {\n"
                        + "  private boolean executed = false;\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {}\n"
                        + "  public void run() { executed = true; }\n"
                        + "  public boolean isExecuted() { return executed; }\n"
                        + "}";

        GeneratedClass<Runnable> generated = new GeneratedClass<Runnable>(className, code);
        Runnable instance = generated.newInstance(Thread.currentThread().getContextClassLoader());

        assertThat(instance).isInstanceOf(Runnable.class);
        instance.run();
    }

    // ==================== Complex Reference Tests ====================

    @Test
    public void testComplexReferences() throws Exception {
        String className = uniqueClassName("ComplexRefs");
        String code =
                "import java.util.List;\n"
                        + "import java.util.Map;\n"
                        + "public class "
                        + className
                        + " {\n"
                        + "  private List<String> list;\n"
                        + "  private Map<String, Integer> map;\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {\n"
                        + "    this.list = (List<String>) refs[0];\n"
                        + "    this.map = (Map<String, Integer>) refs[1];\n"
                        + "  }\n"
                        + "  public int getListSize() { return list.size(); }\n"
                        + "  public int getMapSize() { return map.size(); }\n"
                        + "}";

        java.util.List<String> list = java.util.Arrays.asList("a", "b", "c");
        java.util.Map<String, Integer> map = new java.util.HashMap<String, Integer>();
        map.put("x", 1);
        map.put("y", 2);

        Object[] refs = new Object[] {list, map};
        GeneratedClass<Object> generated = new GeneratedClass<Object>(className, code, refs);
        Object instance = generated.newInstance(Thread.currentThread().getContextClassLoader());

        int listSize = (int) instance.getClass().getMethod("getListSize").invoke(instance);
        int mapSize = (int) instance.getClass().getMethod("getMapSize").invoke(instance);

        assertThat(listSize).isEqualTo(3);
        assertThat(mapSize).isEqualTo(2);
    }

    // ==================== Helper Methods ====================

    private static String uniqueClassName(String prefix) {
        return prefix + "_" + CLASS_COUNTER.incrementAndGet();
    }
}
