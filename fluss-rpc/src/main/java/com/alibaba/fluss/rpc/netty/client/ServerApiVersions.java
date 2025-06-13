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

package com.alibaba.fluss.rpc.netty.client;

import com.alibaba.fluss.exception.UnsupportedVersionException;
import com.alibaba.fluss.rpc.messages.PbApiVersion;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.utils.types.Either;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/** An internal class which represents the API versions supported by a particular server node. */
final class ServerApiVersions {
    // A map of the usable versions of each API, keyed by the ApiKeys instance
    private final Map<ApiKeys, Either<Short, UnsupportedVersionException>>
            highestAvailableVersions = new EnumMap<>(ApiKeys.class);

    ServerApiVersions(Collection<PbApiVersion> serverApiVersions) {
        for (PbApiVersion serverVersion : serverApiVersions) {
            if (!ApiKeys.hasId(serverVersion.getApiKey())) {
                continue;
            }
            ApiKeys apiKey = ApiKeys.forId(serverVersion.getApiKey());
            Optional<Short> version =
                    availableMaxVersion(
                            apiKey.lowestSupportedVersion,
                            apiKey.highestSupportedVersion,
                            (short) serverVersion.getMinVersion(),
                            (short) serverVersion.getMaxVersion());
            if (version.isPresent()) {
                highestAvailableVersions.put(apiKey, Either.left(version.get()));
            } else {
                highestAvailableVersions.put(
                        apiKey,
                        Either.right(
                                new UnsupportedVersionException(
                                        "The server does not support "
                                                + apiKey
                                                + " with version in range ["
                                                + apiKey.lowestSupportedVersion
                                                + ","
                                                + apiKey.highestSupportedVersion
                                                + "]. The supported"
                                                + " range is ["
                                                + serverVersion.getMinVersion()
                                                + ","
                                                + serverVersion.getMaxVersion()
                                                + "].")));
            }
        }
    }

    /** Get the latest version supported by the server within an allowed range of versions. */
    public short highestAvailableVersion(ApiKeys apiKey) {
        if (!highestAvailableVersions.containsKey(apiKey)) {
            throw new UnsupportedVersionException("The server does not support " + apiKey);
        }
        Either<Short, UnsupportedVersionException> version = highestAvailableVersions.get(apiKey);
        if (version.isLeft()) {
            return version.left();
        } else {
            throw version.right();
        }
    }

    static Optional<Short> availableMaxVersion(
            short clientMinVersion,
            short clientMaxVersion,
            short serverMinVersion,
            short serverMaxVersion) {
        short minVersion = (short) Math.max(clientMinVersion, serverMinVersion);
        short maxVersion = (short) Math.min(clientMaxVersion, serverMaxVersion);
        return minVersion > maxVersion ? Optional.empty() : Optional.of(maxVersion);
    }
}
