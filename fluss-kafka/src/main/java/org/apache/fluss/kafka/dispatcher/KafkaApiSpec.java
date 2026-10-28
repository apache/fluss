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

package org.apache.fluss.kafka.dispatcher;

import org.apache.fluss.annotation.Internal;

import org.apache.kafka.common.protocol.ApiKeys;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Describes the versions actually supported by a Kafka API handler. */
@Internal
public final class KafkaApiSpec {

    private final ApiKeys apiKey;
    private final short minVersion;
    private final short maxVersion;
    private final boolean advertised;

    /** Creates an API specification. */
    public KafkaApiSpec(ApiKeys apiKey, short minVersion, short maxVersion, boolean advertised) {
        this.apiKey = checkNotNull(apiKey);
        checkArgument(minVersion >= 0, "Minimum version must not be negative.");
        checkArgument(
                minVersion <= maxVersion,
                "Minimum version %s must not exceed maximum version %s.",
                minVersion,
                maxVersion);
        checkArgument(
                minVersion >= apiKey.oldestVersion() && maxVersion <= apiKey.latestVersion(),
                "Version range [%s, %s] is outside the Kafka library range [%s, %s] for %s.",
                minVersion,
                maxVersion,
                apiKey.oldestVersion(),
                apiKey.latestVersion(),
                apiKey);
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
        this.advertised = advertised;
    }

    /** Returns the Kafka API key. */
    public ApiKeys apiKey() {
        return apiKey;
    }

    /** Returns the oldest supported request version. */
    public short minVersion() {
        return minVersion;
    }

    /** Returns the newest supported request version. */
    public short maxVersion() {
        return maxVersion;
    }

    /** Returns whether this API is allowed to be routed and advertised. */
    public boolean advertised() {
        return advertised;
    }

    /** Returns whether the supplied request version is supported. */
    public boolean supportsVersion(short version) {
        return version >= minVersion && version <= maxVersion;
    }
}
