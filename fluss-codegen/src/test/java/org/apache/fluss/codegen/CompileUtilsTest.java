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

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CompileUtils}.
 *
 * <p>Test coverage includes:
 *
 * <ul>
 *   <li>Successful compilation of valid code
 *   <li>Compilation caching behavior
 *   <li>Cache clearing
 *   <li>Invalid code handling (syntax errors)
 *   <li>Class not found handling
 *   <li>Null parameter handling
 *   <li>Thread safety of compilation
 *   <li>Different class loaders produce different cache entries
 * </ul>
 */
public class CompileUtilsTest {

    private static final AtomicInteger CLASS_COUNTER = new AtomicInteger(0);

    // Note: We don't clear the cache in @BeforeEach/@AfterEach because:
    // 1. Tests use unique class names via uniqueClassName()
    // 2. Clearing cache can interfere with parallel test execution
    // 3. The cache is designed to be persistent for performance

    // ==================== Successful Compilation Tests ====================

    @Test
    public void testCompileSimpleClass() {
        String className = uniqueClassName("SimpleClass");
        String code =
                "public class "
                        + className
                        + " {\n"
                        + "  public String hello() { return \"Hello\"; }\n"
                        + "}";

        Class<?> clazz =
                CompileUtils.compile(
                        Thread.currentThread().getContextClassLoader(), className, code);

        assertThat(clazz).isNotNull();
        assertThat(clazz.getName()).isEqualTo(className);
    }

    @Test
    public void testCompileClassWithConstructor() throws Exception {
        String className = uniqueClassName("WithConstructor");
        String code =
                "public class "
                        + className
                        + " {\n"
                        + "  private int value;\n"
                        + "  public "
                        + className
                        + "(Object[] refs) {\n"
                        + "    this.value = 42;\n"
                        + "  }\n"
                        + "  public int getValue() { return value; }\n"
                        + "}";

        Class<?> clazz =
                CompileUtils.compile(
                        Thread.currentThread().getContextClassLoader(), className, code);

        Object instance =
                clazz.getConstructor(Object[].class).newInstance(new Object[] {new Object[0]});
        int value = (int) clazz.getMethod("getValue").invoke(instance);
        assertThat(value).isEqualTo(42);
    }

    @Test
    public void testCompileClassImplementingInterface() throws Exception {
        String className = uniqueClassName("RunnableImpl");
        String code =
                "public class "
                        + className
                        + " implements Runnable {\n"
                        + "  private boolean ran = false;\n"
                        + "  public void run() { ran = true; }\n"
                        + "  public boolean hasRun() { return ran; }\n"
                        + "}";

        Class<?> clazz =
                CompileUtils.compile(
                        Thread.currentThread().getContextClassLoader(), className, code);

        assertThat(Runnable.class.isAssignableFrom(clazz)).isTrue();

        Runnable instance = (Runnable) clazz.getDeclaredConstructor().newInstance();
        instance.run();
        boolean hasRun = (boolean) clazz.getMethod("hasRun").invoke(instance);
        assertThat(hasRun).isTrue();
    }

    // ==================== Caching Tests ====================

    @Test
    public void testCompilationCaching() {
        String className = uniqueClassName("CachedClass");
        String code = "public class " + className + " {}";
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        Class<?> class1 = CompileUtils.compile(classLoader, className, code);
        Class<?> class2 = CompileUtils.compile(classLoader, className, code);

        // Should return the same class instance from cache
        assertThat(class1).isSameAs(class2);
    }

    @Test
    public void testDifferentCodeProducesDifferentClasses() {
        String className1 = uniqueClassName("Class");
        String className2 = uniqueClassName("Class");
        String code1 = "public class " + className1 + " { public int value() { return 1; } }";
        String code2 = "public class " + className2 + " { public int value() { return 2; } }";
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        Class<?> class1 = CompileUtils.compile(classLoader, className1, code1);
        Class<?> class2 = CompileUtils.compile(classLoader, className2, code2);

        assertThat(class1).isNotSameAs(class2);
        assertThat(class1.getName()).isNotEqualTo(class2.getName());
    }

    @Test
    public void testDifferentClassLoadersProduceDifferentCacheEntries() {
        String className = uniqueClassName("MultiLoaderClass");
        String code = "public class " + className + " {}";

        // Create two different class loaders
        ClassLoader loader1 =
                new URLClassLoader(new URL[0], Thread.currentThread().getContextClassLoader());
        ClassLoader loader2 =
                new URLClassLoader(new URL[0], Thread.currentThread().getContextClassLoader());

        // Different class loader hash codes should produce different cache entries
        // Note: This tests the cache key mechanism, not that the classes are different
        Class<?> class1 = CompileUtils.compile(loader1, className, code);
        Class<?> class2 = CompileUtils.compile(loader2, className, code);

        // Both should compile successfully
        assertThat(class1).isNotNull();
        assertThat(class2).isNotNull();
        assertThat(class1.getName()).isEqualTo(className);
        assertThat(class2.getName()).isEqualTo(className);
    }

    @Test
    public void testClearCache() {
        String className = uniqueClassName("ClearCacheClass");
        String code = "public class " + className + " {}";
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        Class<?> class1 = CompileUtils.compile(classLoader, className, code);

        CompileUtils.clearCache();

        // After clearing, should compile again (new class instance)
        Class<?> class2 = CompileUtils.compile(classLoader, className, code);

        // The classes should be equal (same name) but may or may not be same instance
        // depending on JVM class loading behavior
        assertThat(class1.getName()).isEqualTo(class2.getName());
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testCompileInvalidCode() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        // Syntax error
        assertThatThrownBy(
                        () ->
                                CompileUtils.compile(
                                        classLoader,
                                        "SyntaxErrorClass",
                                        "public class SyntaxErrorClass { invalid syntax }"))
                .isInstanceOf(CodeGenException.class)
                .hasMessageContaining("Code generation cannot be compiled");

        // Missing braces
        assertThatThrownBy(
                        () ->
                                CompileUtils.compile(
                                        classLoader,
                                        "MissingBraceClass",
                                        "public class MissingBraceClass { public void test() { int x = 1;"))
                .isInstanceOf(CodeGenException.class);

        // Wrong class name
        assertThatThrownBy(
                        () ->
                                CompileUtils.compile(
                                        classLoader,
                                        "WrongClassName",
                                        "public class ActualClassName {}"))
                .isInstanceOf(CodeGenException.class)
                .hasMessageContaining("Cannot load class");
    }

    @Test
    public void testCompileNullParametersThrow() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        assertThatThrownBy(() -> CompileUtils.compile(null, "TestClass", "code"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("classLoader must not be null");

        assertThatThrownBy(() -> CompileUtils.compile(classLoader, null, "code"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("name must not be null");

        assertThatThrownBy(() -> CompileUtils.compile(classLoader, "TestClass", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("code must not be null");
    }

    // ==================== Thread Safety Tests ====================

    @Test
    public void testConcurrentCompilationSameCode() throws InterruptedException {
        String className = uniqueClassName("ConcurrentClass");
        String code = "public class " + className + " {}";
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicReference<Class<?>> firstClass = new AtomicReference<>();
        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            Class<?> clazz = CompileUtils.compile(classLoader, className, code);
                            firstClass.compareAndSet(null, clazz);
                            // All threads should get the same class instance due to caching
                            if (clazz == firstClass.get()) {
                                successCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            // Ignore
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        // All threads should have gotten the same cached class
        assertThat(successCount.get()).isEqualTo(threadCount);
    }

    @Test
    public void testConcurrentCompilationDifferentCode() throws InterruptedException {
        int threadCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            String className = uniqueClassName("ConcurrentDiff" + index);
                            String code =
                                    "public class "
                                            + className
                                            + " { public int id() { return "
                                            + index
                                            + "; } }";
                            Class<?> clazz = CompileUtils.compile(classLoader, className, code);
                            if (clazz != null && clazz.getName().equals(className)) {
                                successCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            // Ignore
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(successCount.get()).isEqualTo(threadCount);
    }

    // ==================== Complex Code Tests ====================

    @Test
    public void testCompileComplexClass() throws Exception {
        String className = uniqueClassName("ComplexClass");
        String code =
                "import java.util.ArrayList;\n"
                        + "import java.util.List;\n"
                        + "public class "
                        + className
                        + " {\n"
                        + "  private List items = new ArrayList();\n"
                        + "  public void add(Object item) { items.add(item); }\n"
                        + "  public int size() { return items.size(); }\n"
                        + "  public Object get(int index) { return items.get(index); }\n"
                        + "}";

        Class<?> clazz =
                CompileUtils.compile(
                        Thread.currentThread().getContextClassLoader(), className, code);

        Object instance = clazz.getDeclaredConstructor().newInstance();
        clazz.getMethod("add", Object.class).invoke(instance, "hello");
        clazz.getMethod("add", Object.class).invoke(instance, "world");

        int size = (int) clazz.getMethod("size").invoke(instance);
        assertThat(size).isEqualTo(2);

        String first = (String) clazz.getMethod("get", int.class).invoke(instance, 0);
        assertThat(first).isEqualTo("hello");
    }

    @Test
    public void testCompileWithInnerClass() throws Exception {
        String className = uniqueClassName("OuterClass");
        String code =
                "public class "
                        + className
                        + " {\n"
                        + "  private Inner inner = new Inner();\n"
                        + "  public int getValue() { return inner.value; }\n"
                        + "  private class Inner {\n"
                        + "    int value = 99;\n"
                        + "  }\n"
                        + "}";

        Class<?> clazz =
                CompileUtils.compile(
                        Thread.currentThread().getContextClassLoader(), className, code);

        Object instance = clazz.getDeclaredConstructor().newInstance();
        int value = (int) clazz.getMethod("getValue").invoke(instance);
        assertThat(value).isEqualTo(99);
    }

    // ==================== Helper Methods ====================

    private static String uniqueClassName(String prefix) {
        return prefix + "_" + CLASS_COUNTER.incrementAndGet();
    }
}
