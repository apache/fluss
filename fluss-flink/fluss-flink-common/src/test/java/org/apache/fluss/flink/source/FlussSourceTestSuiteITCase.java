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

package org.apache.fluss.flink.source;

import org.apache.fluss.flink.source.testutils.FlussSourceExternalContext;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.external.ExternalContextFactory;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Runs Flink's standard source test suite ({@link SourceTestSuiteBase}) against {@link
 * FlussSource}, covering reading single and multiple splits, restoring from a savepoint, scaling up
 * and down on restore, source metrics, idle readers, and TaskManager failover.
 */
abstract class FlussSourceTestSuiteITCase extends SourceTestSuiteBase<String> {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @TestEnv MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

    @TestContext
    ExternalContextFactory<FlussSourceExternalContext> logTableContextFactory =
            testName -> {
                try {
                    return new FlussSourceExternalContext(FLUSS_CLUSTER_EXTENSION);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create Fluss external context", e);
                }
            };

    // The deprecated CheckpointingMode is used on purpose: it is the only type accepted by the
    // testing framework in Flink 1.18 and 1.19, and Flink 1.20+ still accepts it as a fallback.
    @SuppressWarnings("deprecation")
    @TestSemantics
    org.apache.flink.streaming.api.CheckpointingMode[] semantics =
            new org.apache.flink.streaming.api.CheckpointingMode[] {
                org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE
            };
}
