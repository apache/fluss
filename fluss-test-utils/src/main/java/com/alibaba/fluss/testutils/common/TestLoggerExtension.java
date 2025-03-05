/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.testutils.common;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A JUnit-5-style test logger. */
public class TestLoggerExtension implements TestWatcher, BeforeEachCallback {
    private static final Logger LOG = LoggerFactory.getLogger(TestLoggerExtension.class);

    @Override
    public void beforeEach(ExtensionContext context) {
        LOG.info(
                "\n================================================================================"
                        + "\nTest {}.{}[{}] is running."
                        + "\n--------------------------------------------------------------------------------",
                context.getRequiredTestClass().getCanonicalName(),
                context.getRequiredTestMethod().getName(),
                context.getDisplayName());
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        LOG.info(
                "\n--------------------------------------------------------------------------------"
                        + "\nTest {}.{}[{}] successfully run."
                        + "\n================================================================================\n",
                context.getRequiredTestClass().getCanonicalName(),
                context.getRequiredTestMethod().getName(),
                context.getDisplayName());
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        LOG.error(
                "\n--------------------------------------------------------------------------------"
                        + "\nTest {}.{}[{}] failed with:\n{}"
                        + "\n================================================================================\n",
                context.getRequiredTestClass().getCanonicalName(),
                context.getRequiredTestMethod().getName(),
                context.getDisplayName(),
                exceptionToString(cause));
    }

    private static String exceptionToString(Throwable t) {
        if (t == null) {
            return "(null)";
        }

        try {
            StringWriter stm = new StringWriter();
            PrintWriter wrt = new PrintWriter(stm);
            t.printStackTrace(wrt);
            wrt.close();
            return stm.toString();
        } catch (Throwable ignored) {
            return t.getClass().getName() + " (error while printing stack trace)";
        }
    }
}
