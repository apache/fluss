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

package org.apache.fluss.metrics.opentelemetry;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.TestContainerExtension;
import org.apache.fluss.testutils.common.TestLoggerExtension;
import org.apache.fluss.utils.function.ThrowingConsumer;
import org.apache.fluss.utils.function.ThrowingRunnable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Tests for {@link OpenTelemetryReporter}. */
@ExtendWith(TestLoggerExtension.class)
public class OpenTelemetryReporterITCaseBase {
    public static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryReporterITCaseBase.class);

    private static final Duration TIME_OUT = Duration.ofMinutes(2);

    @RegisterExtension
    @Order(1)
    private static final AllCallbackWrapper<TestContainerExtension<OpenTelemetryTestContainer>>
            OPENTELEMETRY_EXTENSION =
                    new AllCallbackWrapper<>(
                            new TestContainerExtension<>(OpenTelemetryTestContainer::new));

    @BeforeEach
    public void setup() {
        Slf4jLevelLogConsumer logConsumer = new Slf4jLevelLogConsumer(LOG);
        OPENTELEMETRY_EXTENSION.getCustomExtension().getTestContainer().followOutput(logConsumer);
    }

    public static OpenTelemetryTestContainer getOpenTelemetryContainer() {
        return OPENTELEMETRY_EXTENSION.getCustomExtension().getTestContainer();
    }

    public static void eventuallyConsumeJson(ThrowingConsumer<JsonNode, Exception> jsonConsumer)
            throws Exception {
        eventually(
                () -> {
                    // opentelemetry-collector dumps every report in a new line, so in order to
                    // re-use the
                    // same collector across multiple tests, let's read only the last line
                    getOpenTelemetryContainer()
                            .copyFileFromContainer(
                                    getOpenTelemetryContainer().getOutputLogPath().toString(),
                                    inputStream -> {
                                        List<String> lines = new ArrayList<>();
                                        BufferedReader input =
                                                new BufferedReader(
                                                        new InputStreamReader(inputStream));
                                        String last = "";
                                        String line;

                                        while ((line = input.readLine()) != null) {
                                            lines.add(line);
                                            last = line;
                                        }

                                        ObjectMapper mapper = new ObjectMapper();
                                        JsonNode json = mapper.readValue(last, JsonNode.class);
                                        try {
                                            jsonConsumer.accept(json);
                                        } catch (Throwable t) {
                                            throw new ConsumeDataLogException(t, lines);
                                        }
                                        return null;
                                    });
                });
    }

    public static void eventually(ThrowingRunnable<Exception> runnable) throws Exception {
        eventually(Math.addExact(System.nanoTime(), TIME_OUT.toNanos()), runnable);
    }

    static void eventually(long deadline, ThrowingRunnable<Exception> runnable) throws Exception {
        Thread.sleep(10);
        while (true) {
            try {
                runnable.run();
                break;
            } catch (Throwable e) {
                if (System.nanoTime() >= deadline) {
                    if (e instanceof ConsumeDataLogException) {
                        LOG.error("Failure while the following data log:");
                        ((ConsumeDataLogException) e).getDataLog().forEach(LOG::error);
                    }
                    throw e;
                }
            }
            Thread.sleep(100);
        }
    }

    public static List<String> extractMetricNames(JsonNode json) {
        return json
                .findPath("resourceMetrics")
                .findPath("scopeMetrics")
                .findPath("metrics")
                .findValues("name")
                .stream()
                .map(JsonNode::asText)
                .collect(Collectors.toList());
    }

    private static class ConsumeDataLogException extends Exception {
        private final List<String> dataLog;

        public ConsumeDataLogException(Throwable cause, List<String> dataLog) {
            super(cause);
            this.dataLog = dataLog;
        }

        public List<String> getDataLog() {
            return dataLog;
        }
    }

    /**
     * Similar to {@link Slf4jLevelLogConsumer} but parses output lines and tries to log them with
     * appropriate levels.
     */
    private static class Slf4jLevelLogConsumer extends BaseConsumer<Slf4jLevelLogConsumer> {

        private final Logger logger;

        public Slf4jLevelLogConsumer(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void accept(OutputFrame outputFrame) {
            final OutputFrame.OutputType outputType = outputFrame.getType();
            final String utf8String = outputFrame.getUtf8StringWithoutLineEnding();

            String lowerCase = utf8String.toLowerCase();
            if (lowerCase.contains("error") || lowerCase.contains("exception")) {
                logger.error("{}: {}", outputType, utf8String);
            } else if (lowerCase.contains("warn") || lowerCase.contains("fail")) {
                logger.warn("{}: {}", outputType, utf8String);
            } else {
                logger.info("{}: {}", outputType, utf8String);
            }
        }
    }
}
