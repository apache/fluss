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

package org.apache.fluss.config;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GlobalConfiguration}. */
public class GlobalConfigurationTest {

    @Test
    void testSensitiveConfigurationValuesAreHiddenInLogs(@TempDir Path tempDir) throws Exception {
        Files.write(
                tempDir.resolve(GlobalConfiguration.FLUSS_CONF_FILENAME),
                Arrays.asList(
                        "fs.s3a.access.key: s3-access-key",
                        "fs.gs.auth.service.account.private.key: gs-private-key",
                        "fs.oss.accessKeyId: oss-access-key",
                        "client.security.sasl.password: sasl-password",
                        "client.filesystem.security.token.renewal.backoff: 10 s",
                        "client.request.timeout: 30 s"),
                StandardCharsets.UTF_8);

        Logger logger = (Logger) LogManager.getLogger(GlobalConfiguration.class);
        Level previousLevel = logger.getLevel();
        boolean previousAdditive = logger.isAdditive();
        TestAppender appender = new TestAppender();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
        logger.setAdditive(false);

        try {
            GlobalConfiguration.loadConfiguration(tempDir.toString(), null);
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previousLevel);
            logger.setAdditive(previousAdditive);
            appender.stop();
        }

        String logs = String.join("\n", appender.getMessages());
        assertThat(logs)
                .contains("Loading configuration property: fs.s3a.access.key=******")
                .contains(
                        "Loading configuration property: "
                                + "fs.gs.auth.service.account.private.key=******")
                .contains("Loading configuration property: fs.oss.accessKeyId=******")
                .contains("Loading configuration property: client.security.sasl.password=******")
                .contains(
                        "Loading configuration property: "
                                + "client.filesystem.security.token.renewal.backoff=10 s")
                .contains("Loading configuration property: client.request.timeout=30 s")
                .doesNotContain(
                        "s3-access-key", "gs-private-key", "oss-access-key", "sasl-password");
    }

    private static class TestAppender extends AbstractAppender {

        private final List<String> messages = new ArrayList<>();

        TestAppender() {
            super("test-appender", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }

        private List<String> getMessages() {
            return messages;
        }
    }
}
