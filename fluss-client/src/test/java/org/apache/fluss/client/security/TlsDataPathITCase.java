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

package org.apache.fluss.client.security;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.netty.ssl.TestSslUtils;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.fluss.utils.FileUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads and writes through a cluster whose only listener is {@code TLS}.
 *
 * <p>The client connections and the connections between servers, including follower fetches, all
 * use TLS.
 */
class TlsDataPathITCase {

    private static final String LISTENER = "FLUSS";
    private static final int NUM_ROWS = 2_000;
    // A payload with 2000 rows each 1 KiB. This exceeds both the 32 KiB copy chunks and the channel
    // write-buffer high watermark of the TLS file-region path many times over.
    private static final String PAYLOAD = new String(new char[1024]).replace('\0', 'x');

    private static final Path TEMP_DIR;
    private static final Path TRUSTSTORE;

    @RegisterExtension static final FlussClusterExtension CLUSTER;

    static {
        try {
            TEMP_DIR = Files.createTempDirectory("fluss-tls-itcase");
            SelfSignedCertificate cert = TestSslUtils.generateCertificate("localhost");
            Path keyStore = TestSslUtils.createKeyStore(TEMP_DIR, "keystore.jks", cert);
            TRUSTSTORE = TestSslUtils.createTrustStore(TEMP_DIR, "truststore.jks", cert);

            Configuration conf = new Configuration();
            TestSslUtils.setServerSslConfig(conf, keyStore, null);
            conf.setString(ConfigOptions.SERVER_SSL_ENABLED_LISTENERS.key(), LISTENER);
            setClientSslConfig(conf);
            conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
            CLUSTER =
                    FlussClusterExtension.builder()
                            .setNumOfTabletServers(3)
                            .setClusterConf(conf)
                            .build();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static Connection connection;
    private static Admin admin;

    @BeforeAll
    static void beforeAll() throws Exception {
        // The client TLS settings come with the cluster's client configuration.
        connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
        admin = connection.getAdmin();
    }

    @AfterAll
    static void afterAll() throws Exception {
        admin.close();
        connection.close();
        FileUtils.deleteDirectoryQuietly(TEMP_DIR.toFile());
    }

    @Test
    void testAppendAndScanLogTable() throws Exception {
        TablePath path = TablePath.of("tls", "log_table");
        try (Table table = createTable(path, DATA1_SCHEMA)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < NUM_ROWS; i++) {
                writer.append(row(i, PAYLOAD));
            }
            writer.flush();
            assertReplicatedToAllFollowers(table);

            Set<Integer> seen = new HashSet<>();
            try (LogScanner scanner = table.newScan().createLogScanner()) {
                scanner.subscribeFromBeginning(0);
                while (seen.size() < NUM_ROWS) {
                    for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                        assertThat(record.getRow().getString(1).toString()).isEqualTo(PAYLOAD);
                        seen.add(record.getRow().getInt(0));
                    }
                }
            }
            assertThat(seen).hasSize(NUM_ROWS);
        }
    }

    @Test
    void testUpsertAndLookupPrimaryKeyTable() throws Exception {
        TablePath path = TablePath.of("tls", "pk_table");
        try (Table table = createTable(path, DATA1_SCHEMA_PK)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int i = 0; i < NUM_ROWS; i++) {
                writer.upsert(row(i, PAYLOAD));
            }
            writer.flush();
            assertReplicatedToAllFollowers(table);

            Lookuper lookuper = table.newLookup().createLookuper();
            for (int key : Arrays.asList(0, NUM_ROWS / 2, NUM_ROWS - 1)) {
                assertThatRow(lookuper.lookup(row(key)).get().getSingletonRow())
                        .withSchema(DATA1_ROW_TYPE)
                        .isEqualTo(row(key, PAYLOAD));
            }
        }
    }

    /**
     * Without advertised listeners the servers advertise their bound IP address, which the
     * certificate for {@code localhost} does not name, so hostname verification is disabled here.
     * {@code NettyClientTlsTest} covers it.
     */
    private static void setClientSslConfig(Configuration conf) {
        TestSslUtils.setClientSslConfig(conf, TRUSTSTORE, null);
        conf.setString(ConfigOptions.CLIENT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM.key(), "");
    }

    private static Table createTable(TablePath path, Schema schema) throws Exception {
        admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(
                        path,
                        TableDescriptor.builder().schema(schema).distributedBy(1).build(),
                        false)
                .get();
        Table table = connection.getTable(path);
        CLUSTER.waitUntilAllReplicaReady(new TableBucket(table.getTableInfo().getTableId(), 0));
        return table;
    }

    /**
     * With acks 'all' the writes only complete once every in-sync follower has fetched them, so an
     * in-sync replica set that still holds all three replicas shows that the follower fetches went
     * through.
     */
    private static void assertReplicatedToAllFollowers(Table table) {
        TableBucket bucket = new TableBucket(table.getTableInfo().getTableId(), 0);
        assertThat(CLUSTER.waitAndGetLeaderReplica(bucket).getIsr()).hasSize(3);
    }
}
