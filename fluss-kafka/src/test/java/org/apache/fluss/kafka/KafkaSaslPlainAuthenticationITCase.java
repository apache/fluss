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

package org.apache.fluss.kafka;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration test for Kafka SASL_PLAINTEXT with the PLAIN mechanism. */
public class KafkaSaslPlainAuthenticationITCase {

    private static final String DATABASE = "kafka";
    private static final String TOPIC = "sasl-plain-topic";
    private static final String USERNAME = "writer";
    private static final String PASSWORD = "writer-secret";
    private static final byte[] KEY = "authenticated-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE = "authenticated-value".getBytes(StandardCharsets.UTF_8);

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    private Connection connection;
    private org.apache.fluss.client.admin.Admin flussAdmin;
    private String bootstrapServers;

    @BeforeEach
    public void setup() throws Exception {
        connection = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        flussAdmin = connection.getAdmin();
        flussAdmin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
        bootstrapServers =
                FLUSS_CLUSTER_EXTENSION.getTabletServerNodes("KAFKA").stream()
                        .map(node -> node.host() + ":" + node.port())
                        .collect(Collectors.joining(","));
    }

    @AfterEach
    public void teardown() throws Exception {
        if (flussAdmin != null) {
            try {
                flussAdmin.dropTable(TablePath.of(DATABASE, TOPIC), true).get();
            } catch (Exception ignored) {
                // Preserve the primary test failure when cleanup cannot complete.
            }
            try {
                flussAdmin.dropAcls(Collections.singletonList(AclBindingFilter.ANY)).all().get();
            } catch (Exception ignored) {
                // Preserve the primary test failure when cleanup cannot complete.
            }
            flussAdmin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testAuthenticatedAdminAndProducerLifecycle() throws Exception {
        grantWriterDatabaseAccess();
        Map<String, Object> clientConfig = kafkaClientConfig(USERNAME, PASSWORD);
        try (Admin admin = Admin.create(clientConfig)) {
            admin.createTopics(Collections.singleton(new NewTopic(TOPIC, 1, (short) 1)))
                    .all()
                    .get(30, TimeUnit.SECONDS);

            Map<String, Object> producerConfig = new HashMap<>(clientConfig);
            producerConfig.put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            producerConfig.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
            producerConfig.put(ProducerConfig.ACKS_CONFIG, "1");
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig)) {
                assertThat(producer.send(new ProducerRecord<>(TOPIC, KEY, VALUE)).get())
                        .isNotNull();
            }

            assertFlussRecord();
            admin.deleteTopics(Collections.singleton(TOPIC)).all().get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAuthenticatedUserWithoutAdminAclCannotCreateTopic() throws Exception {
        try (Admin admin = Admin.create(kafkaClientConfig(USERNAME, PASSWORD))) {
            assertThatThrownBy(
                            () ->
                                    admin.createTopics(
                                                    Collections.singleton(
                                                            new NewTopic(TOPIC, 1, (short) 1)))
                                            .all()
                                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TopicAuthorizationException.class);
        }
        assertThat(flussAdmin.tableExists(TablePath.of(DATABASE, TOPIC)).get()).isFalse();
    }

    @Test
    public void testWrongPasswordIsRejectedAsAuthenticationFailure() {
        try (Admin admin = Admin.create(kafkaClientConfig(USERNAME, "wrong-password"))) {
            assertThatThrownBy(() -> admin.describeCluster().nodes().get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(SaslAuthenticationException.class);
        }
    }

    private void assertFlussRecord() throws Exception {
        try (Table table = connection.getTable(TablePath.of(DATABASE, TOPIC));
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            for (int attempt = 0; attempt < 30; attempt++) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    assertThat(record.getRow().getBytes(0)).containsExactly(KEY);
                    assertThat(record.getRow().getBytes(1)).containsExactly(VALUE);
                    return;
                }
            }
        }
        throw new AssertionError("Authenticated Kafka record was not visible through Fluss.");
    }

    private void grantWriterDatabaseAccess() throws Exception {
        AclBinding aclBinding =
                new AclBinding(
                        Resource.database(DATABASE),
                        new AccessControlEntry(
                                new FlussPrincipal(USERNAME, "User"),
                                AccessControlEntry.WILD_CARD_HOST,
                                OperationType.ALL,
                                PermissionType.ALLOW));
        flussAdmin.createAcls(Collections.singletonList(aclBinding)).all().get();
        FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(
                Collections.singletonList(aclBinding), true);
    }

    private Map<String, Object> kafkaClientConfig(String username, String password) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        config.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        config.put(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        config.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        username, password));
        return config;
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.KAFKA_DATABASE, DATABASE);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        config.set(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                Collections.singletonMap("KAFKA", "sasl"));
        config.set(
                ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList("PLAIN"));
        config.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS,
                Collections.singletonMap(USERNAME, PASSWORD));
        config.set(ConfigOptions.AUTHORIZER_ENABLED, true);
        config.set(ConfigOptions.SUPER_USERS, "User:ANONYMOUS");
        return config;
    }
}
