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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;

/**
 * Checks that the current JVM can run the Iceberg 1.12 lake plugin.
 *
 * <p>Iceberg 1.11+ is compiled for Java 17. This class must not reference any Iceberg class, so
 * that it can be loaded on an older JVM and report the problem instead of failing with an {@link
 * UnsupportedClassVersionError}.
 */
@Internal
public final class Iceberg112RuntimeCheck {

    @VisibleForTesting static final int MIN_JAVA_VERSION = 17;

    private Iceberg112RuntimeCheck() {}

    /** Throws if the current JVM is older than Java 17. */
    public static void checkJavaVersion() {
        checkJavaVersion(System.getProperty("java.specification.version"));
    }

    @VisibleForTesting
    static void checkJavaVersion(String javaSpecificationVersion) {
        if (javaMajorVersion(javaSpecificationVersion) < MIN_JAVA_VERSION) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The Iceberg 1.12 lake plugin requires Java %d or later, but this JVM "
                                    + "runs Java %s. Run this process on Java %d or later, or "
                                    + "install the default Iceberg lake plugin "
                                    + "(fluss-lake-iceberg), which supports Java 11.",
                            MIN_JAVA_VERSION, javaSpecificationVersion, MIN_JAVA_VERSION));
        }
    }

    /** Parses "1.8" as 8, and "11", "17" or "21" as themselves. */
    private static int javaMajorVersion(String javaSpecificationVersion) {
        String version = javaSpecificationVersion;
        if (version.startsWith("1.")) {
            version = version.substring(2);
        }
        int dot = version.indexOf('.');
        return Integer.parseInt(dot < 0 ? version : version.substring(0, dot));
    }
}
