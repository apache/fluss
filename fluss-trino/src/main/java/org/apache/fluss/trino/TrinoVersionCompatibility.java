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

package org.apache.fluss.trino;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class TrinoVersionCompatibility {

    private static final int MIN_SUPPORTED_VERSION = 483;
    private static final int MAX_SUPPORTED_VERSION = 483;

    private static final int MIN_SUPPORTED_SNAPSHOT_VERSION = 483;
    private static final int MAX_SUPPORTED_SNAPSHOT_VERSION = 483;

    private static final Pattern VERSION_PATTERN =
            Pattern.compile("^(\\d+)(-SNAPSHOT)?$", Pattern.CASE_INSENSITIVE);

    private TrinoVersionCompatibility() {}

    static void verifyCompatibleVersion(String spiVersion) {
        ParsedVersion version = parseVersion(spiVersion);

        if (version.isSnapshot()) {
            verifySnapshotVersion(spiVersion, version.getVersion());
            return;
        }

        verifyReleaseVersion(spiVersion, version.getVersion());
    }

    private static void verifyReleaseVersion(String spiVersion, int version) {
        if (version < MIN_SUPPORTED_VERSION || version > MAX_SUPPORTED_VERSION) {
            throw new IllegalStateException(
                    String.format(
                            "Unsupported Trino SPI version %s; supported release versions are %s through %s",
                            spiVersion, MIN_SUPPORTED_VERSION, MAX_SUPPORTED_VERSION));
        }
    }

    private static void verifySnapshotVersion(String spiVersion, int version) {
        if (version < MIN_SUPPORTED_SNAPSHOT_VERSION || version > MAX_SUPPORTED_SNAPSHOT_VERSION) {
            throw new IllegalStateException(
                    String.format(
                            "Unsupported Trino SPI version %s; supported snapshot versions are %s-SNAPSHOT through %s-SNAPSHOT",
                            spiVersion,
                            MIN_SUPPORTED_SNAPSHOT_VERSION,
                            MAX_SUPPORTED_SNAPSHOT_VERSION));
        }
    }

    private static ParsedVersion parseVersion(String spiVersion) {
        if (spiVersion == null) {
            throw new IllegalStateException("Unsupported Trino SPI version format: null");
        }

        Matcher matcher = VERSION_PATTERN.matcher(spiVersion);
        if (!matcher.matches()) {
            throw new IllegalStateException("Unsupported Trino SPI version format: " + spiVersion);
        }

        try {
            return new ParsedVersion(Integer.parseInt(matcher.group(1)), matcher.group(2) != null);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Unsupported Trino SPI version format: " + spiVersion);
        }
    }

    private static final class ParsedVersion {

        private final int version;
        private final boolean snapshot;

        private ParsedVersion(int version, boolean snapshot) {
            this.version = version;
            this.snapshot = snapshot;
        }

        private int getVersion() {
            return version;
        }

        private boolean isSnapshot() {
            return snapshot;
        }
    }
}
