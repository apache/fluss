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

package org.apache.fluss.flink.action.orphan;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.Action;
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.action.orphan.job.CleanStats;
import org.apache.fluss.flink.action.orphan.job.OrphanFilesCleanJob;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orphan files cleanup action. Delegates to a distributed Flink Batch job ({@link
 * OrphanFilesCleanJob}) that executes a 3-stage DAG:
 *
 * <ol>
 *   <li>ScopeEnumerator (p=1): coordinator RPCs to enumerate scope and emit per-bucket work items.
 *   <li>ScanAndClean (p=N): parallel FS scan + rate-limited delete.
 *   <li>StatsAggregate (p=1): merge per-task stats into final summary.
 * </ol>
 */
@Internal
public class OrphanFilesCleanAction implements Action {

    private static final Logger LOG = LoggerFactory.getLogger(OrphanFilesCleanAction.class);

    private final OrphanCleanConfig config;

    public OrphanFilesCleanAction(OrphanCleanConfig config) {
        this.config = config;
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CleanStats stats =
                OrphanFilesCleanJob.execute(env, config, config.parallelism().orElse(null));
        LOG.info(
                "remove_orphan_files done: scope={} scanned={} deletedTotal={}"
                        + " emptyDirsRemoved={} failures={} bytesReclaimed={} dryRun={}",
                scopeDescription(),
                stats.scanned(),
                stats.deleted(),
                stats.emptyDirsRemoved(),
                stats.deleteFailures(),
                stats.bytesReclaimed(),
                config.dryRun());
    }

    private String scopeDescription() {
        String scope =
                config.allDatabases() ? "all-databases" : config.database().orElse("unknown");
        if (config.table().isPresent()) {
            return scope + "." + config.table().get();
        }
        return scope;
    }
}
