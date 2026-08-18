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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link org.apache.fluss.utils.VersionInfo}. */
class VersionInfoTest {

    @Test
    void testGetVersionReadsTheFilteredProjectVersion() {
        // fluss-common's pom.xml filters fluss-version.properties with ${project.version}, and
        // that filtering already ran by the time surefire loads classes from target/classes, so
        // this asserts the real project version rather than the "unknown" fallback.
        assertThat(VersionInfo.getVersion()).isNotEqualTo("unknown").matches("^\\d+\\.\\d+.*");
    }

    @Test
    void testGetVersionIsReadOnceAndCached() {
        assertThat(VersionInfo.getVersion()).isSameAs(VersionInfo.getVersion());
    }

    @Test
    void testParseVersionReadsTheVersionKey() {
        assertThat(parse("version=0.10.0")).isEqualTo("0.10.0");
    }

    @Test
    void testParseVersionFallsBackToUnknown() {
        assertThat(VersionInfo.parseVersion(null)).isEqualTo("unknown");
        // A resource copied without Maven filtering still carries the raw token.
        assertThat(parse("version=${project.version}")).isEqualTo("unknown");
        assertThat(parse("other=0.10.0")).isEqualTo("unknown");
        // Properties.load rejects a malformed unicode escape with IllegalArgumentException.
        assertThat(parse("version=\\uZZZZ")).isEqualTo("unknown");
    }

    private static String parse(String content) {
        return VersionInfo.parseVersion(
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
    }
}
