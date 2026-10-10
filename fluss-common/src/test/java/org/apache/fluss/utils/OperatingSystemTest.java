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

package org.apache.fluss.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OperatingSystem}. */
class OperatingSystemTest {

    @Test
    void testGetCurrentOperatingSystemIsNotNull() {
        OperatingSystem os = OperatingSystem.getCurrentOperatingSystem();
        assertThat(os).isNotNull();
    }

    @Test
    void testIsWindowsAndIsMacAreConsistentWithCurrentOs() {
        OperatingSystem current = OperatingSystem.getCurrentOperatingSystem();

        if (current == OperatingSystem.WINDOWS) {
            assertThat(OperatingSystem.isWindows()).isTrue();
            assertThat(OperatingSystem.isMac()).isFalse();
        } else if (current == OperatingSystem.MAC_OS) {
            assertThat(OperatingSystem.isMac()).isTrue();
            assertThat(OperatingSystem.isWindows()).isFalse();
        } else {
            assertThat(OperatingSystem.isWindows()).isFalse();
            assertThat(OperatingSystem.isMac()).isFalse();
        }
    }

    @Test
    void testEnumValues() {
        OperatingSystem[] values = OperatingSystem.values();
        assertThat(values)
                .contains(
                        OperatingSystem.LINUX,
                        OperatingSystem.WINDOWS,
                        OperatingSystem.MAC_OS,
                        OperatingSystem.FREE_BSD,
                        OperatingSystem.SOLARIS,
                        OperatingSystem.UNKNOWN);
    }

    @Test
    void testCurrentOsIsDetectedOnThisMachine() {
        // On the CI runner / developer Mac we at least expect a known OS
        OperatingSystem os = OperatingSystem.getCurrentOperatingSystem();
        assertThat(os).isNotEqualTo(OperatingSystem.UNKNOWN);
    }
}
