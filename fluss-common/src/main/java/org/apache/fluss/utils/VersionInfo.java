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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;

import javax.annotation.Nullable;

import java.io.InputStream;
import java.util.Properties;

/** Utility for looking up the human-readable Fluss version of the running build. */
@Internal
public class VersionInfo {

    // Resolved relative to this class so that a downstream shade relocating org.apache.fluss moves
    // the resource along with it.
    private static final String VERSION_RESOURCE = "fluss-version.properties";
    private static final String VERSION_KEY = "version";
    private static final String UNKNOWN_VERSION = "unknown";

    private static final String VERSION = readVersion();

    private VersionInfo() {}

    /**
     * Returns the Fluss version (e.g. {@code "0.10.0"} for a release, {@code "1.0-SNAPSHOT"} for a
     * snapshot build), read once at class initialization from the build-time-filtered {@code
     * fluss-version.properties} resource on the classpath.
     *
     * <p>Returns {@code "unknown"} when the resource is missing, unreadable, has no {@code version}
     * key, or was copied without Maven resource filtering and so still carries the literal {@code
     * ${project.version}} token.
     */
    public static String getVersion() {
        return VERSION;
    }

    private static String readVersion() {
        try (InputStream stream = VersionInfo.class.getResourceAsStream(VERSION_RESOURCE)) {
            return parseVersion(stream);
        } catch (Exception e) {
            return UNKNOWN_VERSION;
        }
    }

    @VisibleForTesting
    static String parseVersion(@Nullable InputStream stream) {
        if (stream == null) {
            return UNKNOWN_VERSION;
        }
        try {
            Properties properties = new Properties();
            properties.load(stream);
            String version = properties.getProperty(VERSION_KEY, UNKNOWN_VERSION);
            // An unfiltered copy of the resource still carries the raw Maven token.
            return version.startsWith("${") ? UNKNOWN_VERSION : version;
        } catch (Exception e) {
            return UNKNOWN_VERSION;
        }
    }
}
