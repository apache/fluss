/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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
import com.alibaba.fluss.exception.IllegalConfigurationException;
import com.alibaba.fluss.shaded.guava32.com.google.common.net.InetAddresses;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Utility for various network related tasks (such as finding free ports). */
@Internal
public class NetUtils {

    /** The wildcard address to listen on all interfaces (either 0.0.0.0 or ::). */
    private static final String WILDCARD_ADDRESS =
            new InetSocketAddress(0).getAddress().getHostAddress();

    // ------------------------------------------------------------------------
    //  Encoding of IP addresses for URLs
    // ------------------------------------------------------------------------

    /**
     * Returns an address in a normalized format for Pekko. When an IPv6 address is specified, it
     * normalizes the IPv6 address to avoid complications with the exact URL match policy of Pekko.
     *
     * @param host The hostname, IPv4 or IPv6 address
     * @return host which will be normalized if it is an IPv6 address
     */
    public static String unresolvedHostToNormalizedString(String host) {
        // Return loopback interface address if host is null
        // This represents the behavior of {@code InetAddress.getByName } and RFC 3330
        if (host == null) {
            host = InetAddress.getLoopbackAddress().getHostAddress();
        } else {
            host = host.trim().toLowerCase();
            if (host.startsWith("[") && host.endsWith("]")) {
                String address = host.substring(1, host.length() - 1);
                if (InetAddresses.isInetAddress(address)) {
                    host = address;
                }
            }
        }

        // normalize and valid address
        if (InetAddresses.isInetAddress(host)) {
            InetAddress inetAddress = InetAddresses.forString(host);
            if (inetAddress instanceof Inet6Address) {
                byte[] ipV6Address = inetAddress.getAddress();
                host = getIPv6UrlRepresentation(ipV6Address);
            }
        } else {
            try {
                // We don't allow these in hostnames
                checkArgument(!host.startsWith("."));
                checkArgument(!host.endsWith("."));
                checkArgument(!host.contains(":"));
            } catch (Exception e) {
                throw new IllegalConfigurationException("The configured hostname is not valid", e);
            }
        }

        return host;
    }

    /**
     * Returns a valid address for Pekko. It returns a String of format 'host:port'. When an IPv6
     * address is specified, it normalizes the IPv6 address to avoid complications with the exact
     * URL match policy of Pekko.
     *
     * @param host The hostname, IPv4 or IPv6 address
     * @param port The port
     * @return host:port where host will be normalized if it is an IPv6 address
     */
    public static String unresolvedHostAndPortToNormalizedString(String host, int port) {
        checkArgument(isValidHostPort(port), "Port is not within the valid range,");
        return unresolvedHostToNormalizedString(host) + ":" + port;
    }

    /**
     * Creates a compressed URL style representation of an Inet6Address.
     *
     * <p>This method copies and adopts code from Google's Guava library. We re-implement this here
     * in order to reduce dependency on Guava. The Guava library has frequently caused dependency
     * conflicts in the past.
     */
    private static String getIPv6UrlRepresentation(byte[] addressBytes) {
        // first, convert bytes to 16 bit chunks
        int[] hextets = new int[8];
        for (int i = 0; i < hextets.length; i++) {
            hextets[i] = (addressBytes[2 * i] & 0xFF) << 8 | (addressBytes[2 * i + 1] & 0xFF);
        }

        // now, find the sequence of zeros that should be compressed
        int bestRunStart = -1;
        int bestRunLength = -1;
        int runStart = -1;
        for (int i = 0; i < hextets.length + 1; i++) {
            if (i < hextets.length && hextets[i] == 0) {
                if (runStart < 0) {
                    runStart = i;
                }
            } else if (runStart >= 0) {
                int runLength = i - runStart;
                if (runLength > bestRunLength) {
                    bestRunStart = runStart;
                    bestRunLength = runLength;
                }
                runStart = -1;
            }
        }
        if (bestRunLength >= 2) {
            Arrays.fill(hextets, bestRunStart, bestRunStart + bestRunLength, -1);
        }

        // convert into text form
        StringBuilder buf = new StringBuilder(40);
        buf.append('[');

        boolean lastWasNumber = false;
        for (int i = 0; i < hextets.length; i++) {
            boolean thisIsNumber = hextets[i] >= 0;
            if (thisIsNumber) {
                if (lastWasNumber) {
                    buf.append(':');
                }
                buf.append(Integer.toHexString(hextets[i]));
            } else {
                if (i == 0 || lastWasNumber) {
                    buf.append("::");
                }
            }
            lastWasNumber = thisIsNumber;
        }
        buf.append(']');
        return buf.toString();
    }

    // ------------------------------------------------------------------------
    //  Port range parsing
    // ------------------------------------------------------------------------

    /**
     * Returns an iterator over available ports defined by the range definition.
     *
     * @param rangeDefinition String describing a single port, a range of ports or multiple ranges.
     * @return Set of ports from the range definition
     * @throws NumberFormatException If an invalid string is passed.
     */
    public static Iterator<Integer> getPortRangeFromString(String rangeDefinition)
            throws NumberFormatException {
        final String[] ranges = rangeDefinition.trim().split(",");

        UnionIterator<Integer> iterators = new UnionIterator<>();

        for (String rawRange : ranges) {
            Iterator<Integer> rangeIterator;
            String range = rawRange.trim();
            int dashIdx = range.indexOf('-');
            if (dashIdx == -1) {
                // only one port in range:
                final int port = Integer.parseInt(range);
                if (!isValidHostPort(port)) {
                    throw new IllegalConfigurationException(
                            "Invalid port configuration. Port must be between 0"
                                    + "and 65535, but was "
                                    + port
                                    + ".");
                }
                rangeIterator = Collections.singleton(Integer.valueOf(range)).iterator();
            } else {
                // evaluate range
                final int start = Integer.parseInt(range.substring(0, dashIdx));
                if (!isValidHostPort(start)) {
                    throw new IllegalConfigurationException(
                            "Invalid port configuration. Port must be between 0"
                                    + "and 65535, but was "
                                    + start
                                    + ".");
                }
                final int end = Integer.parseInt(range.substring(dashIdx + 1));
                if (!isValidHostPort(end)) {
                    throw new IllegalConfigurationException(
                            "Invalid port configuration. Port must be between 0"
                                    + "and 65535, but was "
                                    + end
                                    + ".");
                }
                rangeIterator =
                        new Iterator<Integer>() {
                            int i = start;

                            @Override
                            public boolean hasNext() {
                                return i <= end;
                            }

                            @Override
                            public Integer next() {
                                return i++;
                            }

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException("Remove not supported");
                            }
                        };
            }
            iterators.add(rangeIterator);
        }

        return iterators;
    }

    /**
     * Find a non-occupied port.
     *
     * @return A non-occupied port.
     */
    public static Port getAvailablePort() {
        for (int i = 0; i < 50; i++) {
            try (ServerSocket serverSocket = new ServerSocket(0)) {
                int port = serverSocket.getLocalPort();
                if (port != 0) {
                    FileLock fileLock = new FileLock(NetUtils.class.getName() + port);
                    if (fileLock.tryLock()) {
                        return new Port(port, fileLock);
                    } else {
                        fileLock.unlockAndDestroy();
                    }
                }
            } catch (IOException ignored) {
            }
        }

        throw new RuntimeException("Could not find a free permitted port on the machine.");
    }

    /**
     * Returns the wildcard address to listen on all interfaces.
     *
     * @return Either 0.0.0.0 or :: depending on the IP setup.
     */
    public static String getWildcardIPAddress() {
        return WILDCARD_ADDRESS;
    }

    /**
     * Check whether the given port is in right range when connecting to somewhere.
     *
     * @param port the port to check
     * @return true if the number in the range 1 to 65535
     */
    public static boolean isValidClientPort(int port) {
        return 1 <= port && port <= 65535;
    }

    /**
     * check whether the given port is in right range when getting port from local system.
     *
     * @param port the port to check
     * @return true if the number in the range 0 to 65535
     */
    public static boolean isValidHostPort(int port) {
        return 0 <= port && port <= 65535;
    }

    /**
     * Port wrapper class which holds a {@link FileLock} until it releases. Used to avoid race
     * condition among multiple threads/processes.
     */
    public static class Port implements AutoCloseable {
        private final int port;
        private final FileLock fileLock;

        public Port(int port, FileLock fileLock) throws IOException {
            checkNotNull(fileLock, "FileLock should not be null");
            checkState(fileLock.isValid(), "FileLock should be locked");
            this.port = port;
            this.fileLock = fileLock;
        }

        public int getPort() {
            return port;
        }

        @Override
        public void close() throws Exception {
            fileLock.unlockAndDestroy();
        }
    }
}
