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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.RetryOneTime;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.compatibilitytest.CompatEnvironment.CLIENT_SALS_PROPERTIES;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_06_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_07_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_LATEST_VERSION_MAGIC;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Basic class for fluss server upgrade compatibility test. */
@Testcontainers
public class ServerUpgradeCompatTest extends CompatTest {
    public static final Schema DATA1_LOG_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .build();

    private @Nullable Connection flussConnection;
    private @Nullable Admin admin;

    // ------------------------------------------------------------------------------------------
    // Fluss-0.6 upgrade to different fluss versions
    // ------------------------------------------------------------------------------------------

    @Test
    void testUpgradeFrom06To07() throws Exception {
        serverUpgradeTestPipeline(FLUSS_06_VERSION_MAGIC, FLUSS_07_VERSION_MAGIC);
    }

    @Test
    void testUpgradeFrom06ToLatest() throws Exception {
        serverUpgradeTestPipeline(FLUSS_06_VERSION_MAGIC, FLUSS_LATEST_VERSION_MAGIC);
    }

    // ------------------------------------------------------------------------------------------
    // Fluss-0.7 upgrade to different fluss versions
    // ------------------------------------------------------------------------------------------

    @Test
    void testUpgradeFrom07ToLatest() throws Exception {
        serverUpgradeTestPipeline(FLUSS_07_VERSION_MAGIC, FLUSS_LATEST_VERSION_MAGIC);
    }

    // ------------------------------------------------------------------------------------------
    // Common methods.
    // ------------------------------------------------------------------------------------------

    protected void serverUpgradeTestPipeline(
            int lowerServerVersionMagic, int higherServerVersionMagic) throws Exception {
        // start the lower server.
        initAndStartFlussServer(lowerServerVersionMagic);
        initFlussConnection(lowerServerVersionMagic);

        Admin admin = getAdmin();
        // create table and insert data.
        String dbName = "upgrade-test-db-1";
        String tableName = "upgrade-test-table-1";
        TablePath tablePath = TablePath.of(dbName, tableName);
        createDatabase(admin, dbName);
        TableDescriptor logTableDescriptor =
                TableDescriptor.builder().schema(DATA1_LOG_SCHEMA).build();
        createTable(admin, tablePath, logTableDescriptor);
        int recordSize = 10;
        List<GenericRow> expectedRows = appendLog(tablePath, 10);
        assertThat(expectedRows).hasSize(recordSize);

        // stop the lower server.
        stopServer();

        // verify zookeeper and fs to check the data.

        // Starting from Fluss-0.8, Fluss supports actively retrying when TabletServer and
        // CoordinatorServer have left registration meta in ZooKeeper, to prevent the servers from
        // failing to start. To avoid startup errors, we proactively clean up any leftover server
        // registration meta in ZooKeeper here.
        cleanServerMetaFromZk();

        // start the higher server.
        initAndStartFlussServer(higherServerVersionMagic);
        initFlussConnection(higherServerVersionMagic);

        // get table and get data.
        List<GenericRow> actualRecords = fetchLog(tablePath, recordSize);
        assertThat(actualRecords).hasSize(recordSize);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRows);

        // stop the higher server.
        stopServer();
    }

    private void initFlussConnection(int serverVersion) {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:" + coordinatorServerPort);
        if (serverVersion >= FLUSS_07_VERSION_MAGIC) {
            CLIENT_SALS_PROPERTIES.forEach(conf::setString);
        }
        flussConnection = ConnectionFactory.createConnection(conf);
        admin = flussConnection.getAdmin();
    }

    private List<GenericRow> appendLog(TablePath tablePath, int recordSize) throws Exception {
        List<GenericRow> expectedRows = new ArrayList<>();
        try (Table table = getFlussConnection().getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < recordSize; i++) {
                GenericRow row = row(i, "a");
                expectedRows.add(row);
                appendWriter.append(row).get();
            }
        }

        List<GenericRow> actualRecords = fetchLog(tablePath, recordSize);
        assertThat(actualRecords).hasSize(recordSize);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRows);
        return expectedRows;
    }

    private List<GenericRow> fetchLog(TablePath tablePath, int expectedSize) throws Exception {
        List<GenericRow> expectedRows = new ArrayList<>();
        try (Table table = getFlussConnection().getTable(tablePath)) {
            LogScanner logScanner = table.newScan().createLogScanner();
            logScanner.subscribe(0, LogScanner.EARLIEST_OFFSET);

            while (expectedRows.size() < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    InternalRow row = scanRecord.getRow();
                    expectedRows.add(row(row.getInt(0), row.getString(1)));
                }
            }

            logScanner.close();
        }

        return expectedRows;
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

    private void cleanServerMetaFromZk() {
        String coordinatorPath = "/fluss/coordinators";
        String coordinatorActivePath = coordinatorPath + "/active";
        String tabletServerPath = "/fluss/tabletservers";
        String idsPath = tabletServerPath + "/ids";

        String connectionString = ZOOKEEPER.getHost() + ":" + ZOOKEEPER.getMappedPort(2181);
        CuratorFramework client =
                CuratorFrameworkFactory.builder()
                        .connectString(connectionString)
                        .retryPolicy(new RetryOneTime(100))
                        .build();
        client.start();

        // wait coordinatorServer meta removed from ZooKeeper.
        waitUntil(
                () -> {
                    if (client.checkExists().forPath(coordinatorPath) != null) {
                        if (client.checkExists().forPath(coordinatorActivePath) != null) {
                            try {
                                client.delete().forPath(coordinatorActivePath);
                            } catch (Exception e) {
                                Thread.sleep(1000);
                                return false;
                            }
                        } else {
                            return true;
                        }
                    }

                    return true;
                },
                Duration.ofMinutes(1),
                "error while cleaning coordinator server meta from ZooKeeper");

        waitUntil(
                () -> {
                    if (client.checkExists().forPath(tabletServerPath) != null) {
                        if (client.checkExists().forPath(idsPath) != null) {
                            List<String> idsChildren = client.getChildren().forPath(idsPath);
                            for (String child : idsChildren) {
                                String childPath = idsPath + "/" + child;
                                try {
                                    client.delete().forPath(childPath);
                                } catch (Exception e) {
                                    Thread.sleep(1000);
                                    return false;
                                }
                            }
                        } else {
                            return true;
                        }
                    }
                    return true;
                },
                Duration.ofMinutes(1),
                "error while cleaning tablet server meta from ZooKeeper");

        client.close();
    }
}
