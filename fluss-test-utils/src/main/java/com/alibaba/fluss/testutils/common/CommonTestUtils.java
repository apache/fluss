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

package com.alibaba.fluss.testutils.common;

import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Fail.fail;

/** This class contains auxiliary methods for unit tests. */
public class CommonTestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CommonTestUtils.class);

    /**
     * Wait util the given condition is met or timeout.
     *
     * <p>Note: use {@code #waitUtil(ThrowingSupplier, Duration, String)} if waiting test to reach a
     * condition and no assertion is needed. Otherwise, use {@link #retry(Duration, Executable)} if
     * the there is assertion expected to succeed eventually.
     *
     * @param condition the condition to wait for.
     * @param timeout the maximum time to wait for the condition to become true.
     * @param pause delay between condition checks.
     * @param errorMsg the error message to include in the <code>TimeoutException</code> if the
     *     condition was not met before timeout.
     */
    public static void waitUtil(
            ThrowingSupplier<Boolean> condition,
            Duration timeout,
            Duration pause,
            String errorMsg) {
        long timeoutMs = timeout.toMillis();
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("The timeout must be positive.");
        }
        long startingTime = System.currentTimeMillis();
        boolean conditionResult;
        try {
            conditionResult = condition.get();
            while (!conditionResult && System.currentTimeMillis() - startingTime < timeoutMs) {
                conditionResult = condition.get();
                //noinspection BusyWait
                Thread.sleep(pause.toMillis());
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        if (!conditionResult) {
            fail(errorMsg);
        }
    }

    /**
     * Wait util the given condition is met or timeout.
     *
     * <p>Note: use {@code #waitUtil(ThrowingSupplier, Duration, String)} if waiting test to reach a
     * condition and no assertion is needed. Otherwise, use {@link #retry(Duration, Executable)} if
     * the there is assertion expected to succeed eventually.
     *
     * @param condition the condition to wait for.
     * @param timeout the maximum time to wait for the condition to become true.
     * @param errorMsg the error message to include in the <code>TimeoutException</code> if the
     *     condition was not met before timeout.
     */
    public static void waitUtil(
            ThrowingSupplier<Boolean> condition, Duration timeout, String errorMsg) {
        waitUtil(condition, timeout, Duration.ofMillis(1), errorMsg);
    }

    /**
     * Wait for the presence of an optional value.
     *
     * @param supplier The function defining the optional value
     * @param timeout Maximum time to wait
     * @param errorMsg Error message in the case that the value never appears
     * @return The unwrapped value returned by the function
     */
    public static <T> T waitValue(
            ThrowingSupplier<Optional<T>> supplier, Duration timeout, String errorMsg) {
        AtomicReference<T> result = new AtomicReference<>();
        waitUtil(
                () -> {
                    Optional<T> opt = supplier.get();
                    if (opt.isPresent()) {
                        result.set(opt.get());
                        return true;
                    } else {
                        return false;
                    }
                },
                timeout,
                errorMsg);
        return result.get();
    }

    /**
     * Execute the given assertion. If it throws an assert error, retry. Repeat until no error is
     * thrown or the time limit elapses.
     *
     * <p>Note: use {@code retry(Duration, Executable)} if the assertion is expected to succeed
     * eventually. If waiting test to reach a condition and no assertion is needed, use {@link
     * #waitUtil(ThrowingSupplier, Duration, String)} instead.
     */
    public static void retry(Duration timeout, Executable assertion) {
        final long maxWaitMs = timeout.toMillis();
        long waitMs = 1L;
        long startTime = System.currentTimeMillis();
        while (true) {
            try {
                assertion.execute();
                return;
            } catch (AssertionError t) {
                if (System.currentTimeMillis() - startTime >= maxWaitMs) {
                    throw t;
                }
                LOG.info("Attempt failed, sleeping for {} ms, and then retrying.", waitMs);
                try {
                    //noinspection BusyWait
                    Thread.sleep(waitMs);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                waitMs += Math.min(waitMs, 1000L);
            } catch (Throwable t) {
                throw new AssertionError("Attempt failed.", t);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Manipulation of environment
    // ------------------------------------------------------------------------
    public static void setEnv(Map<String, String> newenv) {
        setEnv(newenv, true);
    }

    // This code is taken slightly modified from: http://stackoverflow.com/a/7201825/568695
    // it changes the environment variables of this JVM. Use only for testing purposes!
    @SuppressWarnings("unchecked")
    public static void setEnv(Map<String, String> newenv, boolean clearExisting) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> clazz = env.getClass();
            Field field = clazz.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> map = (Map<String, String>) field.get(env);
            if (clearExisting) {
                map.clear();
            }
            map.putAll(newenv);
            // only for Windows
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            try {
                Field theCaseInsensitiveEnvironmentField =
                        processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
                theCaseInsensitiveEnvironmentField.setAccessible(true);
                Map<String, String> cienv =
                        (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
                if (clearExisting) {
                    cienv.clear();
                }
                cienv.putAll(newenv);
            } catch (NoSuchFieldException ignored) {
            }
        } catch (Exception e1) {
            throw new RuntimeException(e1);
        }
    }

    /**
     * Gets the classpath with which the current JVM was started.
     *
     * @return The classpath with which the current JVM was started.
     */
    public static String getCurrentClasspath() {
        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        return bean.getClassPath();
    }

    /**
     * Tries to get the java executable command with which the current JVM was started. Returns
     * null, if the command could not be found.
     *
     * @return The java executable command.
     */
    public static String getJavaCommandPath() {
        File javaHome = new File(System.getProperty("java.home"));

        String path1 = new File(javaHome, "java").getAbsolutePath();
        String path2 = new File(new File(javaHome, "bin"), "java").getAbsolutePath();

        try {
            ProcessBuilder bld = new ProcessBuilder(path1, "-version");
            Process process = bld.start();
            if (process.waitFor() == 0) {
                return path1;
            }
        } catch (Throwable t) {
            // ignore and try the second path
        }

        try {
            ProcessBuilder bld = new ProcessBuilder(path2, "-version");
            Process process = bld.start();
            if (process.waitFor() == 0) {
                return path2;
            }
        } catch (Throwable tt) {
            // no luck
        }
        return null;
    }

    public static void printLog4jDebugConfig(File file) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("rootLogger.level = DEBUG");
            writer.println("rootLogger.appenderRef.console.ref = ConsoleAppender");
            writer.println("appender.console.name = ConsoleAppender");
            writer.println("appender.console.type = CONSOLE");
            writer.println("appender.console.target = SYSTEM_OUT");
            writer.println("appender.console.layout.type = PatternLayout");
            writer.println(
                    "appender.console.layout.pattern = %d{HH:mm:ss,SSS} %-4r [%t] %-5p %c %x - %m%n");
            writer.println("logger.jetty.name = org.eclipse.jetty.util.log");
            writer.println("logger.jetty.level = OFF");
            writer.println("logger.zookeeper.name = org.apache.zookeeper");
            writer.println("logger.zookeeper.level = OFF");
            writer.flush();
        }
    }

    /** Utility class to read the output of a process stream and forward it into a StringWriter. */
    public static class PipeForwarder extends Thread {

        private final StringWriter target;
        private final InputStream source;

        public PipeForwarder(InputStream source, StringWriter target) {
            super("Pipe Forwarder");
            setDaemon(true);

            this.source = source;
            this.target = target;

            start();
        }

        @Override
        public void run() {
            try {
                int next;
                while ((next = source.read()) != -1) {
                    target.write(next);
                }
            } catch (IOException e) {
                // terminate
            }
        }
    }
}
