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

package com.alibaba.fluss.utils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link InstantiationUtils}. */
public class InstantiationUtilTest {

    @Test
    void testSerDeserializeObject() throws Exception {
        TestClass testClass = new TestClass(1, "f2");
        byte[] bytes = InstantiationUtils.serializeObject(testClass);
        // deserialize with classloader
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes)) {
            TestClass deserializedTestClass =
                    InstantiationUtils.deserializeObject(
                            byteArrayInputStream, this.getClass().getClassLoader());
            assertThat(deserializedTestClass).isEqualTo(testClass);
        }

        // deserialize without classloader
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes)) {
            TestClass deserializedTestClass =
                    InstantiationUtils.deserializeObject(byteArrayInputStream, null);
            assertThat(deserializedTestClass).isEqualTo(testClass);
        }
    }

    @Test
    void testClone() throws Exception {
        // test clone null
        Object clonedObj = InstantiationUtils.clone(null);
        assertThat(clonedObj).isNull();
        // test clone null with classloader
        clonedObj = InstantiationUtils.clone(null, Thread.currentThread().getContextClassLoader());
        assertThat(clonedObj).isNull();
        // test clone a string
        String testString = "testString";
        assertThat(InstantiationUtils.clone(testString)).isEqualTo(testString);
        // test clone a string with classloader
        assertThat(
                        InstantiationUtils.clone(
                                testString, Thread.currentThread().getContextClassLoader()))
                .isEqualTo(testString);
    }

    private static final class TestClass implements Serializable {

        private static final long serialVersionUID = 1L;
        private final int f1;
        private final String f2;

        public TestClass(int f1, String f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestClass testClass = (TestClass) o;
            return f1 == testClass.f1 && Objects.equals(f2, testClass.f2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(f1, f2);
        }
    }
}
