/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.testutils.common;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.annotation.Testable;
import org.junit.platform.commons.support.AnnotationSupport;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

/**
 * Filters JUnit tests not marked with {@link MultiVersionTest} in a non-default version module.
 *
 * <p>The filter can be enabled for a module with {@value #ENABLE_PROPERTY}, or for an individual
 * test class with {@link RunMultiVersionTestsOnly}.
 */
public final class MultiVersionTestCondition implements ExecutionCondition {

    /** JUnit configuration parameter that enables multi-version filtering for a test module. */
    public static final String ENABLE_PROPERTY = "fluss.tests.multiversion.enabled";

    private static final ConditionEvaluationResult ENABLED =
            ConditionEvaluationResult.enabled("Multi-version filtering is not enabled");

    private static final ConditionEvaluationResult ENABLED_CLASS =
            ConditionEvaluationResult.enabled("Class contains selected multi-version tests");

    private static final ConditionEvaluationResult ENABLED_MULTI_VERSION =
            ConditionEvaluationResult.enabled("Test is marked as multi-version");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (!context.getTestClass().isPresent()) {
            return ENABLED;
        }

        Class<?> testClass = context.getRequiredTestClass();
        if (!isFilteringEnabled(context, testClass)) {
            return ENABLED;
        }

        if (!context.getTestMethod().isPresent()) {
            return hasSelectedTests(testClass)
                    ? ENABLED_CLASS
                    : ConditionEvaluationResult.disabled("Class has no multi-version tests");
        }

        Method testMethod = context.getRequiredTestMethod();
        if (context.getTags().contains(MultiVersionTest.TAG)) {
            return ENABLED_MULTI_VERSION;
        }

        return ConditionEvaluationResult.disabled(
                String.format(
                        "Test '%s' is not marked with @MultiVersionTest", testMethod.getName()));
    }

    private static boolean isFilteringEnabled(ExtensionContext context, Class<?> testClass) {
        boolean enabledByConfiguration =
                context.getConfigurationParameter(ENABLE_PROPERTY)
                        .map(Boolean::parseBoolean)
                        .orElse(false);
        return enabledByConfiguration
                || AnnotationSupport.isAnnotated(testClass, RunMultiVersionTestsOnly.class);
    }

    static boolean hasSelectedTests(Class<?> testClass) {
        return hasSelectedTests(testClass, new HashSet<>());
    }

    private static boolean hasSelectedTests(Class<?> testClass, Set<Class<?>> inspectedClasses) {
        Class<?> currentClass = testClass;
        while (currentClass != null && currentClass != Object.class) {
            if (inspectedClasses.add(currentClass)) {
                if (AnnotationSupport.isAnnotated(currentClass, MultiVersionTest.class)) {
                    return true;
                }

                for (Method method : currentClass.getDeclaredMethods()) {
                    if (AnnotationSupport.isAnnotated(method, Testable.class)
                            && AnnotationSupport.isAnnotated(method, MultiVersionTest.class)) {
                        return true;
                    }
                }

                for (Class<?> nestedClass : currentClass.getDeclaredClasses()) {
                    if (AnnotationSupport.isAnnotated(nestedClass, Nested.class)
                            && hasSelectedTests(nestedClass, inspectedClasses)) {
                        return true;
                    }
                }
            }
            currentClass = currentClass.getSuperclass();
        }
        return false;
    }
}
