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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/** Tests for the {@link com.alibaba.fluss.utils.NetUtils}. */
class NetUtilsTest {

    @Test
    public void testFreePortRangeUtility() {
        // inspired by Hadoop's example for "yarn.app.mapreduce.am.job.client.port-range"
        String rangeDefinition =
                "50000-50050, 50100-50200,51234 "; // this also contains some whitespaces
        Iterator<Integer> portsIter = NetUtils.getPortRangeFromString(rangeDefinition);
        Set<Integer> ports = new HashSet<>();
        while (portsIter.hasNext()) {
            assertThat(ports.add(portsIter.next())).as("Duplicate element").isTrue();
        }

        assertThat(ports.size()).isEqualTo(51 + 101 + 1);
        // check first range
        assertThat(ports).contains(50000, 50001, 50002, 50050);
        // check second range and last point
        assertThat(ports).contains(50100, 50101, 50110, 50200, 51234);
        // check that only ranges are included
        assertThat(ports).doesNotContain(50051, 50052, 1337, 50201, 49999, 50099);

        // test single port "range":
        portsIter = NetUtils.getPortRangeFromString(" 51234");
        assertThat(portsIter.hasNext()).isTrue();
        assertThat((int) portsIter.next()).isEqualTo(51234);
        assertThat(portsIter.hasNext()).isFalse();

        // test port list
        portsIter = NetUtils.getPortRangeFromString("5,1,2,3,4");
        assertThat(portsIter.hasNext()).isTrue();
        assertThat((int) portsIter.next()).isEqualTo(5);
        assertThat((int) portsIter.next()).isEqualTo(1);
        assertThat((int) portsIter.next()).isEqualTo(2);
        assertThat((int) portsIter.next()).isEqualTo(3);
        assertThat((int) portsIter.next()).isEqualTo(4);
        assertThat(portsIter.hasNext()).isFalse();

        Throwable error = null;

        // try some wrong values: String
        try {
            NetUtils.getPortRangeFromString("localhost");
        } catch (Throwable t) {
            error = t;
        }
        assertThat(error instanceof NumberFormatException).isTrue();
        error = null;

        // incomplete range
        try {
            NetUtils.getPortRangeFromString("5-");
        } catch (Throwable t) {
            error = t;
        }
        assertThat(error instanceof NumberFormatException).isTrue();
        error = null;

        // incomplete range
        try {
            NetUtils.getPortRangeFromString("-5");
        } catch (Throwable t) {
            error = t;
        }
        assertThat(error instanceof NumberFormatException).isTrue();
        error = null;

        // empty range
        try {
            NetUtils.getPortRangeFromString(",5");
        } catch (Throwable t) {
            error = t;
        }
        assertThat(error instanceof NumberFormatException).isTrue();
    }

    @Test
    public void testFormatAddress() {
        {
            // null
            String host = null;
            int port = 42;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo("127.0.0.1" + ":" + port);
        }
        {
            // IPv4
            String host = "1.2.3.4";
            int port = 42;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo(host + ":" + port);
        }
        {
            // IPv6
            String host = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";
            int port = 42;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo("[2001:db8:85a3::8a2e:370:7334]:" + port);
        }
        {
            // [IPv6]
            String host = "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]";
            int port = 42;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo("[2001:db8:85a3::8a2e:370:7334]:" + port);
        }
        {
            // Hostnames
            String host = "somerandomhostname";
            int port = 99;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo(host + ":" + port);
        }
        {
            // Whitespace
            String host = "  somerandomhostname  ";
            int port = 99;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo(host.trim() + ":" + port);
        }
        {
            // Illegal hostnames
            String host = "illegalhost.";
            int port = 42;
            try {
                NetUtils.unresolvedHostAndPortToNormalizedString(host, port);
                fail("should throw exception for Illegal hostnames: " + host);
            } catch (Exception ignored) {
            }
            // Illegal hostnames
            host = "illegalhost:fasf";
            try {
                NetUtils.unresolvedHostAndPortToNormalizedString(host, port);
                fail("should throw exception for Illegal hostnames: " + host);
            } catch (Exception ignored) {
            }
        }
        {
            // Illegal port ranges
            String host = "1.2.3.4";
            int port = -1;
            try {
                NetUtils.unresolvedHostAndPortToNormalizedString(host, port);
                fail("should throw exception for Illegal hostnames: " + host);
            } catch (Exception ignored) {
            }
        }
        {
            // lower case conversion of hostnames
            String host = "CamelCaseHostName";
            int port = 99;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo(host.toLowerCase() + ":" + port);
        }
    }
}
