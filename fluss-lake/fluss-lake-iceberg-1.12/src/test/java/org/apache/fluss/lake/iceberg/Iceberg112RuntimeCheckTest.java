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

package org.apache.fluss.lake.iceberg;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Iceberg112RuntimeCheck}. */
class Iceberg112RuntimeCheckTest {

    @ParameterizedTest
    @ValueSource(strings = {"17", "21", "25"})
    void testAcceptsJava17OrLater(String javaVersion) {
        assertThatCode(() -> Iceberg112RuntimeCheck.checkJavaVersion(javaVersion))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.8", "11", "16"})
    void testRejectsJavaOlderThan17(String javaVersion) {
        assertThatThrownBy(() -> Iceberg112RuntimeCheck.checkJavaVersion(javaVersion))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("requires Java 17 or later")
                .hasMessageContaining("runs Java " + javaVersion)
                .hasMessageContaining("fluss-lake-iceberg");
    }
}
