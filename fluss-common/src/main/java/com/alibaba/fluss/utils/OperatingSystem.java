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

import com.alibaba.fluss.annotation.Internal;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** An enumeration indicating the operating system that the JVM runs on. */
@Internal
public enum OperatingSystem {
    LINUX,
    WINDOWS,
    MAC_OS,
    FREE_BSD,
    SOLARIS,
    UNKNOWN;

    // ------------------------------------------------------------------------

    /**
     * Gets the operating system that the JVM runs on from the java system properties. this method
     * returns <tt>UNKNOWN</tt>, if the operating system was not successfully determined.
     *
     * @return The enum constant for the operating system, or <tt>UNKNOWN</tt>, if it was not
     *     possible to determine.
     */
    public static OperatingSystem getCurrentOperatingSystem() {
        return os;
    }

    /**
     * Checks whether the operating system this JVM runs on is Windows.
     *
     * @return <code>true</code> if the operating system this JVM runs on is Windows, <code>false
     *     </code> otherwise
     */
    public static boolean isWindows() {
        return getCurrentOperatingSystem() == WINDOWS;
    }

    /**
     * Checks whether the operating system this JVM runs on is Windows.
     *
     * @return <code>true</code> if the operating system this JVM runs on is Windows, <code>false
     *     </code> otherwise
     */
    public static boolean isMac() {
        return getCurrentOperatingSystem() == MAC_OS;
    }

    /** The enum constant for the operating system. */
    private static final OperatingSystem os = readOSFromSystemProperties();

    /**
     * Parses the operating system that the JVM runs on from the java system properties. If the
     * operating system was not successfully determined, this method returns {@code UNKNOWN}.
     *
     * @return The enum constant for the operating system, or {@code UNKNOWN}, if it was not
     *     possible to determine.
     */
    private static OperatingSystem readOSFromSystemProperties() {
        String osName = System.getProperty(OS_KEY);

        if (osName.startsWith(LINUX_OS_PREFIX)) {
            return LINUX;
        }
        if (osName.startsWith(WINDOWS_OS_PREFIX)) {
            return WINDOWS;
        }
        if (osName.startsWith(MAC_OS_PREFIX)) {
            return MAC_OS;
        }
        if (osName.startsWith(FREEBSD_OS_PREFIX)) {
            return FREE_BSD;
        }
        String osNameLowerCase = osName.toLowerCase();
        if (osNameLowerCase.contains(SOLARIS_OS_INFIX_1)
                || osNameLowerCase.contains(SOLARIS_OS_INFIX_2)) {
            return SOLARIS;
        }

        return UNKNOWN;
    }

    // --------------------------------------------------------------------------------------------
    //  Constants to extract the OS type from the java environment
    // --------------------------------------------------------------------------------------------

    /** The key to extract the operating system name from the system properties. */
    private static final String OS_KEY = "os.name";

    /** The expected prefix for Linux operating systems. */
    private static final String LINUX_OS_PREFIX = "Linux";

    /** The expected prefix for Windows operating systems. */
    private static final String WINDOWS_OS_PREFIX = "Windows";

    /** The expected prefix for Mac OS operating systems. */
    private static final String MAC_OS_PREFIX = "Mac";

    /** The expected prefix for FreeBSD. */
    private static final String FREEBSD_OS_PREFIX = "FreeBSD";

    /** One expected infix for Solaris. */
    private static final String SOLARIS_OS_INFIX_1 = "sunos";

    /** One expected infix for Solaris. */
    private static final String SOLARIS_OS_INFIX_2 = "solaris";
}
