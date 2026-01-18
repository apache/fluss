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

package org.apache.fluss.cli.config;

import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectionConfigTest {

    @Test
    void testCreateWithBootstrapServers() {
        ConnectionConfig config = new ConnectionConfig("localhost:9123");

        assertThat(config.getConfiguration().toMap())
                .containsEntry("bootstrap.servers", "localhost:9123");
    }

    @Test
    void testCreateWithMultipleBootstrapServers() {
        ConnectionConfig config = new ConnectionConfig("host1:9123,host2:9123,host3:9123");

        assertThat(config.getConfiguration().toMap())
                .containsEntry("bootstrap.servers", "host1:9123,host2:9123,host3:9123");
    }

    @Test
    void testCreateFromPropertiesFile(@TempDir Path tempDir) throws IOException {
        File propsFile = tempDir.resolve("fluss.properties").toFile();

        try (FileWriter writer = new FileWriter(propsFile)) {
            writer.write("bootstrap.servers=localhost:9123\n");
            writer.write("client.id=test-client\n");
            writer.write("request.timeout.ms=30000\n");
        }

        ConnectionConfig config = new ConnectionConfig(propsFile);

        assertThat(config.getConfiguration().toMap())
                .containsEntry("bootstrap.servers", "localhost:9123")
                .containsEntry("client.id", "test-client")
                .containsEntry("request.timeout.ms", "30000");
    }

    @Test
    void testCreateFromNonExistentFile() {
        File nonExistentFile = new File("/nonexistent/path/fluss.properties");

        assertThatThrownBy(() -> new ConnectionConfig(nonExistentFile))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testCreateFromEmptyPropertiesFile(@TempDir Path tempDir) throws IOException {
        File propsFile = tempDir.resolve("empty.properties").toFile();
        propsFile.createNewFile();

        ConnectionConfig config = new ConnectionConfig(propsFile);

        assertThat(config.getConfiguration()).isNotNull();
    }

    @Test
    void testCreateFromConfiguration() {
        Configuration originalConf = new Configuration();
        originalConf.setString("bootstrap.servers", "test:9123");
        originalConf.setString("custom.property", "value");

        ConnectionConfig config = new ConnectionConfig(originalConf);

        assertThat(config.getConfiguration().toMap())
                .containsEntry("bootstrap.servers", "test:9123")
                .containsEntry("custom.property", "value");
    }

    @Test
    void testAddProperty() {
        ConnectionConfig config = new ConnectionConfig("localhost:9123");

        config.addProperty("client.id", "my-client");
        config.addProperty("retries", "3");

        assertThat(config.getConfiguration().toMap())
                .containsEntry("client.id", "my-client")
                .containsEntry("retries", "3");
    }

    @Test
    void testAddPropertyOverridesExisting() {
        ConnectionConfig config = new ConnectionConfig("localhost:9123");

        assertThat(config.getConfiguration().toMap())
                .containsEntry("bootstrap.servers", "localhost:9123");

        config.addProperty("bootstrap.servers", "newhost:9123");

        assertThat(config.getConfiguration().toMap())
                .containsEntry("bootstrap.servers", "newhost:9123");
    }

    @Test
    void testPropertiesFileWithComments(@TempDir Path tempDir) throws IOException {
        File propsFile = tempDir.resolve("commented.properties").toFile();

        try (FileWriter writer = new FileWriter(propsFile)) {
            writer.write("# This is a comment\n");
            writer.write("bootstrap.servers=localhost:9123\n");
            writer.write("! Another comment style\n");
            writer.write("client.id=test\n");
        }

        ConnectionConfig config = new ConnectionConfig(propsFile);

        assertThat(config.getConfiguration().toMap())
                .containsEntry("bootstrap.servers", "localhost:9123")
                .containsEntry("client.id", "test");
    }

    @Test
    void testPropertiesFileWithSpecialCharacters(@TempDir Path tempDir) throws IOException {
        File propsFile = tempDir.resolve("special.properties").toFile();

        try (FileWriter writer = new FileWriter(propsFile)) {
            writer.write("bootstrap.servers=localhost:9123\n");
            writer.write("property.with.dots=value\n");
            writer.write("property-with-dashes=value\n");
            writer.write("property_with_underscores=value\n");
        }

        ConnectionConfig config = new ConnectionConfig(propsFile);

        assertThat(config.getConfiguration().toMap())
                .containsEntry("property.with.dots", "value")
                .containsEntry("property-with-dashes", "value")
                .containsEntry("property_with_underscores", "value");
    }
}
