/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.alibaba.fluss.config.ConfigOptions.WRITER_ID_EXPIRATION_CHECK_INTERVAL;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.InternalRowListAssert.assertThatRows;

/** IT case for {@link FlussTable}. */
class FlussWriterIdExpireITCase {
    private static final ManualClock CLOCK = new ManualClock();

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .setClock(CLOCK)
                    .build();

    protected Connection conn;
    protected Admin admin;
    protected Configuration clientConf;

    @BeforeEach
    protected void setup() throws Exception {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    @Test
    void testWriterIdExpire() throws Exception {
        createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false);
        CLOCK.advanceTime(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        List<InternalRow> expectedRows = new ArrayList<>();
        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            // first, write some data
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < 20; i++) {
                InternalRow row = row(i, "a" + i);
                appendWriter.append(row).get();
                expectedRows.add(row);
            }

            // then, advance clock to make the writer id expiration
            CLOCK.advanceTime(Duration.ofDays(10));

            // wait for 1s to make writer id expiration check happen
            Thread.sleep(1_000);

            // decrease the time to avoid writer id expiration
            CLOCK.rewindTime(Duration.ofDays(10));

            // then write some data again,
            for (int i = 20; i < 30; i++) {
                InternalRow row = row(i, "a" + i);
                appendWriter.append(row).get();
                expectedRows.add(row);
            }
        }

        // check the records
        List<InternalRow> actualRows = new ArrayList<>();
        try (Table table = conn.getTable(DATA1_TABLE_PATH);
                LogScanner logScanner = table.newScan().createLogScanner()) {
            for (int i = 0; i < 3; i++) {
                logScanner.subscribeFromBeginning(i);
            }
            int count = 0;
            int expectedSize = 30;
            while (count < expectedSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    InternalRow row = scanRecord.getRow();
                    count++;
                    actualRows.add(row);
                }
            }
        }
        actualRows.sort(Comparator.comparing(row -> row.getInt(0)));
        assertThatRows(actualRows).withSchema(DATA1_ROW_TYPE).isEqualTo(expectedRows);
    }

    protected long createTable(
            TablePath tablePath, TableDescriptor tableDescriptor, boolean ignoreIfExists)
            throws Exception {
        admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, ignoreIfExists)
                .get();
        admin.createTable(tablePath, tableDescriptor, ignoreIfExists).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // to a small check interval to make writer id expiration happen asap
        conf.set(WRITER_ID_EXPIRATION_CHECK_INTERVAL, Duration.ofMillis(100));
        return conf;
    }
}
