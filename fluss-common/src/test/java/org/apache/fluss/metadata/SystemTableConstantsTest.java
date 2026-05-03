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

package org.apache.fluss.metadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SystemTableConstants}. */
class SystemTableConstantsTest {

    @Test
    void testSystemDatabaseName() {
        assertThat(SystemTableConstants.SYSTEM_DATABASE).isEqualTo("sys");
    }

    @Test
    void testIsSystemDatabase() {
        assertThat(SystemTableConstants.isSystemDatabase("sys")).isTrue();
        assertThat(SystemTableConstants.isSystemDatabase("fluss")).isFalse();
        assertThat(SystemTableConstants.isSystemDatabase(null)).isFalse();
    }

    @Test
    void testIsSystemTableName() {
        // Known system view
        assertThat(SystemTableConstants.isSystemTableName("sys", "tablet_servers")).isTrue();
        // Unknown table in sys database should return false
        assertThat(SystemTableConstants.isSystemTableName("sys", "unknown_table")).isFalse();
        // User database
        assertThat(SystemTableConstants.isSystemTableName("fluss", "my_table")).isFalse();
        // Null inputs
        assertThat(SystemTableConstants.isSystemTableName(null, null)).isFalse();
        assertThat(SystemTableConstants.isSystemTableName("sys", null)).isFalse();
        assertThat(SystemTableConstants.isSystemTableName(null, "tablet_servers")).isFalse();
    }

    @Test
    void testIsSystemTablePath() {
        TablePath viewPath = TablePath.of("sys", "tablet_servers");
        TablePath userPath = TablePath.of("fluss", "my_table");
        TablePath unknownSysPath = TablePath.of("sys", "user_table");

        assertThat(SystemTableConstants.isSystemTablePath(viewPath)).isTrue();
        assertThat(SystemTableConstants.isSystemTablePath(userPath)).isFalse();
        // Unknown table in sys database should return false
        assertThat(SystemTableConstants.isSystemTablePath(unknownSysPath)).isFalse();
        assertThat(SystemTableConstants.isSystemTablePath(null)).isFalse();
    }

    @Test
    void testTabletServersViewConstant() {
        assertThat(SystemTableConstants.TABLET_SERVERS_VIEW).isEqualTo("tablet_servers");
    }

    @Test
    void testIsSystemView() {
        assertThat(SystemTableConstants.isSystemView("sys", "tablet_servers")).isTrue();
        assertThat(SystemTableConstants.isSystemView("sys", "user_table")).isFalse();
        assertThat(SystemTableConstants.isSystemView("fluss", "tablet_servers")).isFalse();
        assertThat(SystemTableConstants.isSystemView(null, null)).isFalse();
    }
}
