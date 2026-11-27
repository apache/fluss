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

package org.apache.fluss.client.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.metadata.TableInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utils for Fluss Client. */
public final class ClientUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ClientUtils.class);

    private static final Pattern HOST_PORT_PATTERN =
            Pattern.compile(".*?\\[?([0-9a-zA-Z\\-%._:]*)\\]?:([0-9]+)");

    private ClientUtils() {}

    // todo: may add DnsLookup
    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls) {
        if (urls == null) {
            throw new IllegalConfigurationException(
                    ConfigOptions.BOOTSTRAP_SERVERS.key() + " should be set.");
        }
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (String url : urls) {
            if (url != null && !url.isEmpty()) {
                try {
                    String host = getHost(url);
                    Integer port = getPort(url);
                    if (host == null || port == null) {
                        throw new IllegalConfigurationException(
                                "Invalid url in "
                                        + ConfigOptions.BOOTSTRAP_SERVERS.key()
                                        + ": "
                                        + url);
                    }
                    InetSocketAddress address = new InetSocketAddress(host, port);
                    if (address.isUnresolved()) {
                        LOG.warn(
                                "Couldn't resolve server {} from {} as DNS resolution failed for {}",
                                url,
                                ConfigOptions.BOOTSTRAP_SERVERS.key(),
                                host);
                    } else {
                        addresses.add(address);
                    }
                } catch (IllegalArgumentException e) {
                    throw new IllegalConfigurationException(
                            "Invalid port in "
                                    + ConfigOptions.BOOTSTRAP_SERVERS.key()
                                    + ": "
                                    + url);
                }
            }
        }
        if (addresses.isEmpty()) {
            throw new IllegalConfigurationException(
                    "No resolvable bootstrap urls given in "
                            + ConfigOptions.BOOTSTRAP_SERVERS.key());
        }
        return addresses;
    }

    /**
     * Extracts the hostname from a "host:port" address string.
     *
     * @param address address string to parse
     * @return hostname or null if the given address is incorrect
     */
    public static String getHost(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? matcher.group(1) : null;
    }

    /**
     * Extracts the port number from a "host:port" address string.
     *
     * @param address address string to parse
     * @return port number or null if the given address is incorrect
     */
    public static Integer getPort(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? Integer.parseInt(matcher.group(2)) : null;
    }

    /**
     * Resolves the routing bucket count when it is unavailable in the metadata. Falling back to the
     * table-level count is safe only when {@code bucketCountEpoch == 0}, which proves the table was
     * never rescaled; otherwise this fails instead of silently returning a wrong answer.
     */
    public static int fallbackBucketCountOrFail(TableInfo tableInfo, Object target) {
        long epoch = tableInfo.getBucketCountEpoch();
        if (epoch > 0) {
            throw new IllegalStateException(
                    "Routing bucket count is unavailable for "
                            + target
                            + " at bucketCountEpoch "
                            + epoch
                            + "; refusing to fall back to the table-level count.");
        }
        return tableInfo.getNumBuckets();
    }
}
