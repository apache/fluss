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

package org.apache.fluss.client.metrics;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ScannerMetricGroup}. */
class ScannerMetricGroupTest {

    private static final TablePath TABLE_PATH = TablePath.of("db", "table");

    @Test
    void testScannerMetricGroupContainsDatabaseAndTable() {
        ScannerMetricGroup scannerMetricGroup =
                new ScannerMetricGroup(TestingClientMetricGroup.newInstance(), TABLE_PATH);
        try {
            assertThat(scannerMetricGroup.getAllVariables())
                    .containsEntry("database", "db")
                    .containsEntry("table", "table");
        } finally {
            scannerMetricGroup.close();
        }
    }

    /**
     * Two scanners reading the same table must be distinguishable, otherwise their metrics are
     * reported under the same name and overwrite each other.
     */
    @Test
    void testScannerIdIsUniquePerScannerMetricGroup() {
        ClientMetricGroup clientMetricGroup = TestingClientMetricGroup.newInstance();
        ScannerMetricGroup scannerMetricGroup1 =
                new ScannerMetricGroup(clientMetricGroup, TABLE_PATH);
        ScannerMetricGroup scannerMetricGroup2 =
                new ScannerMetricGroup(clientMetricGroup, TABLE_PATH);
        try {
            Map<String, String> variables1 = scannerMetricGroup1.getAllVariables();
            Map<String, String> variables2 = scannerMetricGroup2.getAllVariables();

            assertThat(variables1).containsKey("scanner_id");
            assertThat(variables2).containsKey("scanner_id");
            assertThat(variables1.get("scanner_id")).isNotEqualTo(variables2.get("scanner_id"));
        } finally {
            scannerMetricGroup1.close();
            scannerMetricGroup2.close();
        }
    }
}
