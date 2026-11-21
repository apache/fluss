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

package org.apache.fluss.client.table.scanner.log.internals;

import org.apache.fluss.config.ConfigOption;

import java.time.Duration;

import static org.apache.fluss.config.ConfigBuilder.key;

/** Configuration options for consumer group offset management. */
public final class ConsumerConfig {

    public static final ConfigOption<String> AUTO_OFFSET_RESET =
            key("client.consumer.auto-offset-reset")
                    .stringType()
                    .defaultValue("earliest")
                    .withDescription(
                            "Controls the starting position when no committed offset exists. "
                                    + "Supported values are earliest, latest, and none.");

    public static final ConfigOption<Boolean> ENABLE_AUTO_COMMIT =
            key("client.consumer.auto-commit.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether consumed offsets are committed periodically in the background.");

    public static final ConfigOption<Duration> AUTO_COMMIT_INTERVAL =
            key("client.consumer.auto-commit.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription("The interval between automatic consumer offset commits.");

    public static final ConfigOption<String> GROUP_ID =
            key("client.consumer.group.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A unique identifier for the consumer group used to store offsets.");

    private ConsumerConfig() {}
}
