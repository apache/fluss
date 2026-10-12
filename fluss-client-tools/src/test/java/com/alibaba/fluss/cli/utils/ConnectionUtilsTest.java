/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.cli.utils;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class ConnectionUtilsTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @AfterEach
    public void tearDown() throws Exception {
        new ConnectionUtils().close();
    }

    @Test
    public void testGetConnectionCreatesNewConnection() {
        Connection mockConnection = mock(Connection.class);
        try (MockedStatic<ConnectionFactory> mockedFactory = mockStatic(ConnectionFactory.class)) {
            mockedFactory
                    .when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(mockConnection);
            Connection conn1 = ConnectionUtils.getConnection(BOOTSTRAP_SERVERS);
            Connection conn2 = ConnectionUtils.getConnection(BOOTSTRAP_SERVERS);
            assertNotNull(conn1);
            assertSame(conn1, conn2);
            mockedFactory.verify(
                    () -> ConnectionFactory.createConnection(any(Configuration.class)));
        }
    }

    @Test
    public void testGetAdminReturnsAdminFromConnection() {
        Connection mockConnection = mock(Connection.class);
        Admin mockAdmin = mock(Admin.class);
        when(mockConnection.getAdmin()).thenReturn(mockAdmin);
        try (MockedStatic<ConnectionFactory> mockedFactory = mockStatic(ConnectionFactory.class)) {
            mockedFactory
                    .when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(mockConnection);
            Admin admin = ConnectionUtils.getAdmin(BOOTSTRAP_SERVERS);
            assertNotNull(admin);
            assertSame(mockAdmin, admin);
            verify(mockConnection, times(1)).getAdmin();
        }
    }

    @Test
    public void testCloseClosesAllConnections() throws Exception {
        Connection mockConnection = mock(Connection.class);
        try (MockedStatic<ConnectionFactory> mockedFactory = mockStatic(ConnectionFactory.class)) {
            mockedFactory
                    .when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(mockConnection);
            ConnectionUtils.getConnection(BOOTSTRAP_SERVERS);
            ConnectionUtils utils = new ConnectionUtils();
            utils.close();
            verify(mockConnection, times(1)).close();
        }
    }
}
