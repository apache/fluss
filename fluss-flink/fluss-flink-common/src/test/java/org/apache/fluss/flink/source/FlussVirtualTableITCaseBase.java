/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;

/**
 * Base class for virtual table integration tests that require checkpoint and savepoint support.
 * This class provides shared utilities for tests that need to configure file-based checkpoints and
 * savepoints, waits for checkpoint completion before creating savepoints Test job restart scenarios
 * from savepoints. Subclasses are responsible for managing their own Flink MiniCluster lifecycle to
 * enable full control over checkpoint/savepoint behavior.
 */
abstract class FlussVirtualTableITCaseBase {

    /** Temporary directory for storing checkpoints during tests. */
    @TempDir public static File checkpointDir;

    /** Temporary directory for storing savepoints during tests. */
    @TempDir public static File savepointDir;

    /** Creates a Flink configuration with file-based checkpoints and savepoints. */
    protected static Configuration getFileBasedCheckpointsConfig(File savepointDir) {
        return getFileBasedCheckpointsConfig(savepointDir.toURI().toString());
    }

    /** Creates a Flink configuration with file-based checkpoints and savepoints. */
    protected static Configuration getFileBasedCheckpointsConfig(final String savepointDir) {
        final Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        return config;
    }

    /**
     * Waits for at least one checkpoint to complete for the given job. This method polls the
     * checkpoint directory until a checkpoint subdirectory appears (prefixed with "chk-"). It is
     * essential to call this before creating a savepoint to ensure the job has completed at least
     * one checkpoint.
     */
    protected static void waitForCheckpoint(JobID jobId) {
        String jobIdStr = jobId.toHexString();
        waitUntil(
                () -> {
                    File jobCheckpointDir = new File(checkpointDir, jobIdStr);
                    if (!jobCheckpointDir.exists()) {
                        return false;
                    }
                    File[] checkpoints =
                            jobCheckpointDir.listFiles(
                                    f -> f.isDirectory() && f.getName().startsWith("chk-"));
                    return checkpoints != null && checkpoints.length > 0;
                },
                Duration.ofSeconds(60),
                "Timeout waiting for checkpoint for job " + jobIdStr);
    }
}
