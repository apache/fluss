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

import org.apache.iceberg.IcebergBuild;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Verifies how the Iceberg 1.12 plugin is built: JDK 17 toolchain, Java 11 bytecode. */
class Iceberg112BuildTest {

    /** Class file major version of Java 11. */
    private static final int JAVA_11_CLASS_FILE_VERSION = 55;

    @Test
    void testRunsOnJava17OrLater() {
        assertThatCode(Iceberg112RuntimeCheck::checkJavaVersion).doesNotThrowAnyException();
    }

    @Test
    void testUsesIceberg112() {
        assertThat(IcebergBuild.version()).startsWith("1.12.");
    }

    @Test
    void testFlussClassesTargetJava11() throws IOException {
        // Java 11 bytecode lets the plugin entry point load on Java 11 and fail with a clear
        // message, instead of an UnsupportedClassVersionError during plugin discovery.
        assertThat(classFileMajorVersion(Iceberg112RuntimeCheck.class))
                .isEqualTo(JAVA_11_CLASS_FILE_VERSION);
    }

    private static int classFileMajorVersion(Class<?> clazz) throws IOException {
        String resource = clazz.getSimpleName() + ".class";
        try (InputStream in = clazz.getResourceAsStream(resource);
                DataInputStream data = new DataInputStream(in)) {
            assertThat(data.readInt()).as("class file magic").isEqualTo(0xCAFEBABE);
            data.readUnsignedShort(); // minor version
            return data.readUnsignedShort();
        }
    }
}
