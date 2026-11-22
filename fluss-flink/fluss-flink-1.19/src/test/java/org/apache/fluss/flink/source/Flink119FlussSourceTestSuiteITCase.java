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

import org.apache.fluss.testutils.common.MultiVersionTest;

import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;

/** IT case for Flink's source test suite against {@link FlussSource} in Flink 1.19. */
public class Flink119FlussSourceTestSuiteITCase extends FlussSourceTestSuiteITCase {

    // Restoring from a savepoint is the representative multi-version test of this suite; the
    // other inherited tests run with the default Flink version only.
    @Override
    @TestTemplate
    @MultiVersionTest
    @DisplayName("Test source restarting from a savepoint")
    public void testSavepoint(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<String> externalContext,
            CheckpointingMode semantic)
            throws Exception {
        super.testSavepoint(testEnv, externalContext, semantic);
    }
}
