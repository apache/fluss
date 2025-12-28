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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;

/**
 * UT for {@link TieringCommitOperator}. Test the compatibility of the `getAttemptNumber` method in
 * flink 1.18.
 */
public class Flink118TieringCommitOperatorTest extends TieringCommitOperatorTest {

    @BeforeEach
    @Override
    void beforeEach() throws Exception {
        mockOperatorEventGateway = new MockOperatorEventGateway();
        MockOperatorEventDispatcher mockOperatorEventDispatcher =
                new MockOperatorEventDispatcher(mockOperatorEventGateway);
        parameters =
                new StreamOperatorParameters<>(
                        new SourceOperatorStreamTask<String>(new DummyEnvironment()),
                        new MockStreamConfig(new Configuration(), 1),
                        new MockOutput<>(new ArrayList<>()),
                        null,
                        mockOperatorEventDispatcher);

        committerOperator =
                new TieringCommitOperator<>(
                        parameters,
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        new org.apache.fluss.config.Configuration(),
                        new TestingLakeTieringFactory());
        committerOperator.open();
    }
}
