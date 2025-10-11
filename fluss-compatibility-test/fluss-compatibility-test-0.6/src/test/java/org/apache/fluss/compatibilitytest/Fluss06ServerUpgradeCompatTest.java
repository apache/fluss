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

package org.apache.fluss.compatibilitytest;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_06_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_07_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_LATEST_VERSION_MAGIC;

/** Compatibility testing for upgrading from fluss-0.6 to different higher versions . */
@Testcontainers
public class Fluss06ServerUpgradeCompatTest extends ServerUpgradeCompatTest {

    private @Nullable Connection flussConnection;
    private @Nullable Admin admin;
    private final Map<TablePath, LogScanner> logScannerMap = new HashMap<>();

    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
        if (!logScannerMap.isEmpty()) {
            Collection<LogScanner> logScanners = logScannerMap.values();
            for (LogScanner scanner : logScanners) {
                scanner.close();
            }
        }
    }

    @Override
    boolean verifyServerReady(int serverVersion) throws Exception {
        return TestingUtils.serverReady(coordinatorServerPort, 0, false)
                && TestingUtils.serverReady(tabletServerPort, 0, true);
    }

    @Test
    void testUpgradeFluss06ServerTo07() throws Exception {
        serverUpgradeTestPipeline(
                FLUSS_06_VERSION_MAGIC, FLUSS_06_VERSION_MAGIC, FLUSS_07_VERSION_MAGIC);
    }

    @Test
    void testUpgradeFluss06ServerToLatest() throws Exception {
        serverUpgradeTestPipeline(
                FLUSS_06_VERSION_MAGIC, FLUSS_06_VERSION_MAGIC, FLUSS_LATEST_VERSION_MAGIC);
    }

    @Override
    void initFlussConnection(int serverVersion) {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:" + coordinatorServerPort);
        flussConnection = ConnectionFactory.createConnection(conf);
    }

    @Override
    void initFlussAdmin() {
        if (flussConnection == null) {
            throw new RuntimeException("flussConnection is null, please init it first.");
        }
        admin = flussConnection.getAdmin();
    }

    @Override
    void createDatabase(String dbName) throws Exception {
        Admin admin = getAdmin();
        admin.createDatabase(dbName, DatabaseDescriptor.EMPTY, false).get();
    }

    @Override
    boolean tableExists(String dbName, String tableName) throws Exception {
        Admin admin = getAdmin();
        return admin.tableExists(TablePath.of(dbName, tableName)).get();
    }

    @Override
    void createTable(TestingTableDescriptor tableDescriptor) throws Exception {
        Admin admin = getAdmin();
        TestingUtils.createTable(admin, tableDescriptor);
    }

    @Override
    void produceLog(String dbName, String tableName, List<Object[]> records) throws Exception {
        Connection flussConnection = getFlussConnection();
        TestingUtils.produceLog(flussConnection, TablePath.of(dbName, tableName), records);
    }

    @Override
    void subscribe(
            String dbName,
            String tableName,
            @Nullable Integer partitionId,
            int bucketId,
            long offset) {
        TablePath tablePath = TablePath.of(dbName, tableName);
        Connection flussConnection = getFlussConnection();
        LogScanner logScanner;
        if (logScannerMap.containsKey(tablePath)) {
            logScanner = logScannerMap.get(tablePath);
        } else {
            Table table = flussConnection.getTable(TablePath.of(dbName, tableName));
            logScanner = table.newScan().createLogScanner();
            logScannerMap.put(tablePath, logScanner);
        }

        if (partitionId != null) {
            logScanner.subscribe(partitionId, bucketId, offset);
        } else {
            logScanner.subscribe(bucketId, offset);
        }
    }

    @Override
    List<Object[]> poll(String dbName, String tableName, Duration timeout) throws Exception {
        TablePath tablePath = TablePath.of(dbName, tableName);
        if (!logScannerMap.containsKey(tablePath)) {
            throw new RuntimeException("Please subscribe this table first.");
        }
        LogScanner scanner = logScannerMap.get(tablePath);
        return TestingUtils.poll(flussConnection, tablePath, scanner, timeout);
    }

    @Override
    void putKv(String dbName, String tableName, List<Object[]> records) throws Exception {
        Connection flussConnection = getFlussConnection();
        TestingUtils.putKv(flussConnection, TablePath.of(dbName, tableName), records);
    }

    @Override
    @Nullable
    Object[] lookup(String dbName, String tableName, Object[] key) throws Exception {
        Connection flussConnection = getFlussConnection();
        return TestingUtils.lookup(flussConnection, TablePath.of(dbName, tableName), key);
    }

    private Admin getAdmin() {
        if (admin == null) {
            throw new RuntimeException("admin is null, please init it first.");
        }
        return admin;
    }

    private Connection getFlussConnection() {
        if (flussConnection == null) {
            throw new RuntimeException("flussConnection is null, please init it first.");
        }
        return flussConnection;
    }
}
