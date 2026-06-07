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

package org.apache.fluss.client.converter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Basic tests for {@link PojoType}. */
class PojoTypeTest {
    @Test
    void test() {
        assertThatThrownBy(() -> PojoType.of(ClassWithNoPublicConstructor.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must have a public default constructor.");

        assertThatThrownBy(() -> PojoType.of(ClassWithNonWithNonPublicField.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(" must be public.");

        assertThatThrownBy(() -> PojoType.of(PublicClass.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Primitive types are not allowed; all fields must be nullable (use wrapper types).");

        assertThatThrownBy(() -> PojoType.of(PublicClass.InnerClass.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Primitive types are not allowed; all fields must be nullable (use wrapper types).");

        assertThatThrownBy(() -> PojoType.of(PublicWithNonPrimitive.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be public or have both getter and setter");

        assertThatThrownBy(() -> PojoType.of(PublicWithPublicWithBoolean.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be public or have both getter and setter");

        assertThatThrownBy(() -> PojoType.of(PublicWithPublicWithBooleanWithGetterOnly.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be public or have both getter and setter");

        PojoType.of(PublicWithPublicWithBooleanWithGetterAndSetter.class);
        PojoType.of(PublicWithPublicWithBooleanWithIsAndSetter.class);
        PojoType.of(PublicWithPublicWithBooleanWithHasAndSetter.class);
        PojoType.of(PublicWithPublicNonPrimitive.class);
    }

    public class ClassWithNoPublicConstructor {
        int f;
        int j;

        private ClassWithNoPublicConstructor() {}
    }

    class ClassWithNonWithNonPublicField {
        int f;
        int j;
    }

    public class PublicClass {
        public class InnerClass {
            final int e;

            public InnerClass() {
                e = 2;
            }
        }

        final int f;
        final int j;

        public PublicClass() {
            f = 1;
            j = 1;
        }
    }

    public class PublicWithNonPrimitive {
        String s;

        public PublicWithNonPrimitive() {}
    }

    public class PublicWithPublicNonPrimitive {
        public String s;

        public PublicWithPublicNonPrimitive() {}
    }

    public class PublicWithPublicWithBoolean {
        private Boolean b;

        public PublicWithPublicWithBoolean() {}
    }

    public class PublicWithPublicWithBooleanWithGetterOnly {
        private Boolean b;

        public PublicWithPublicWithBooleanWithGetterOnly() {}

        public Boolean getB() {
            return b;
        }
    }

    public class PublicWithPublicWithBooleanWithGetterAndSetter {
        private Boolean b;

        public PublicWithPublicWithBooleanWithGetterAndSetter() {}

        public Boolean getB() {
            return b;
        }

        public void setB(boolean b) {
            this.b = b;
        }
    }

    public class PublicWithPublicWithBooleanWithIsAndSetter {
        private Boolean b;

        public PublicWithPublicWithBooleanWithIsAndSetter() {}

        public Boolean isB() {
            return b;
        }

        public void setB(boolean b) {
            this.b = b;
        }
    }

    public class PublicWithPublicWithBooleanWithHasAndSetter {
        private Boolean b;

        public PublicWithPublicWithBooleanWithHasAndSetter() {}

        public Boolean hasB() {
            return b;
        }

        public void setB(boolean b) {
            this.b = b;
        }
    }
}
