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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MultiVersionTestCondition}. */
@RunMultiVersionTestsOnly
class MultiVersionTestConditionTest extends MultiVersionTestConditionTestBase {

    @Test
    @MultiVersionTest
    void testMarkedDeclaredMethodRuns() {}

    @Test
    @MultiVersionTest
    void testSelectedNestedTestsAreDetected() {
        assertThat(MultiVersionTestCondition.hasSelectedTests(OnlyMarkedNestedMethodTest.class))
                .isTrue();
        assertThat(MultiVersionTestCondition.hasSelectedTests(OnlyMarkedNestedClassTest.class))
                .isTrue();
        assertThat(MultiVersionTestCondition.hasSelectedTests(OnlyUnmarkedNestedTest.class))
                .isFalse();
    }

    @Test
    void testUnmarkedDeclaredMethodDoesNotRun() {
        throw new AssertionError("Unmarked declared test should have been disabled");
    }

    private static class OnlyMarkedNestedMethodTest {

        @Nested
        class NestedTests {

            @Test
            @MultiVersionTest
            void testMarkedNestedMethod() {}
        }
    }

    private static class OnlyMarkedNestedClassTest {

        @Nested
        @MultiVersionTest
        class NestedTests {

            @Test
            void testNestedClassMethod() {}
        }
    }

    private static class OnlyUnmarkedNestedTest {

        @Nested
        class NestedTests {

            @Test
            void testUnmarkedNestedMethod() {}
        }
    }
}

abstract class MultiVersionTestConditionTestBase {

    @Test
    @MultiVersionTest
    void testMarkedInheritedMethodRuns() {}

    @Test
    void testUnmarkedInheritedMethodDoesNotRun() {
        throw new AssertionError("Unmarked inherited test should have been disabled");
    }
}

@RunMultiVersionTestsOnly
class OnlyUnmarkedInheritedMultiVersionTestConditionTest
        extends OnlyUnmarkedInheritedMultiVersionTestConditionTestBase {

    @BeforeAll
    static void failIfClassLifecycleStarts() {
        throw new AssertionError("Class without selected tests should have been disabled");
    }
}

abstract class OnlyUnmarkedInheritedMultiVersionTestConditionTestBase {

    @Test
    void testUnmarkedInheritedMethodDoesNotStartClass() {
        throw new AssertionError("Class without selected tests should have been disabled");
    }
}
