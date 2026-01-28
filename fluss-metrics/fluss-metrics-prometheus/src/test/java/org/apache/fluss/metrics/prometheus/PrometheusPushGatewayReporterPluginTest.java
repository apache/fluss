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

package org.apache.fluss.metrics.prometheus;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Test for {@link PrometheusPushGatewayReporterPlugin}. */
public class PrometheusPushGatewayReporterPluginTest {

    @Test
    void testParseGroupingKey() {
        Map<String, String> groupingKey =
                PrometheusPushGatewayReporterPlugin.parseGroupingKey("k1=v1;k2=v2");
        assertThat(groupingKey).containsEntry("k1", "v1");
        assertThat(groupingKey).containsEntry("k2", "v2");
    }

    @Test
    void testParseIncompleteGroupingKey() {
        Map<String, String> groupingKey =
                PrometheusPushGatewayReporterPlugin.parseGroupingKey("k1=");
        assertThat(groupingKey).isEmpty();

        groupingKey = PrometheusPushGatewayReporterPlugin.parseGroupingKey("=v1");
        assertThat(groupingKey).isEmpty();

        groupingKey = PrometheusPushGatewayReporterPlugin.parseGroupingKey("k1");
        assertThat(groupingKey).isEmpty();
    }
}
