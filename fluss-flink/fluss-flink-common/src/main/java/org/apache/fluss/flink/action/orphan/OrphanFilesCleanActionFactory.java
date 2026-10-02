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
import org.apache.fluss.flink.action.ActionFactory;
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;

import java.util.Optional;

/** Factory for the shell-mode orphan files cleanup action. */
@Internal
public class OrphanFilesCleanActionFactory implements ActionFactory {

    @Override
    public String identifier() {
        return "remove_orphan_files";
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        return Optional.<Action>of(
                new OrphanFilesCleanAction(OrphanCleanConfig.fromParams(params)));
    }

    @Override
    public String help() {
        return "Usage: remove_orphan_files --bootstrap-server <host:port>\n"
                + "  (--database <db> [--table <name>] | --all-databases)\n"
                + "  [--older-than '<ISO-8601 with offset>']\n"
                + "  [--remote-fs-op-rate-limit-per-second 100]\n"
                + "  [--dry-run]\n"
                + "  [--allow-delete-manifest]\n"
                + "  [--allow-clean-orphan-tables]\n"
                + "  [--allow-clean-orphan-partitions]\n"
                + "  [--conf <key>=<value>]...\n"
                + "\n"
                + "Notes:\n"
                + "  --older-than is an absolute wall-clock cutoff in ISO-8601 with explicit\n"
                + "    offset (e.g. '2024-01-01T00:00:00+08:00' or '2024-01-01T00:00:00Z').\n"
                + "    Files with mtime strictly less than the cutoff are deletion-eligible.\n"
                + "    Default: now - 3d, computed once at startup. The cutoff is frozen for the\n"
                + "    run, so a long scan cannot accidentally pull in files written after the\n"
                + "    action started. The cutoff must be at least 1d before now (closer cutoffs\n"
                + "    would race with mid-write files).\n"
                + "  Orphan directory detection (table/partition) relies solely on ID guards\n"
                + "    (maxKnownTableId / maxKnownPartitionId), not mtime.\n"
                + "  --table also disables the orphan-table scan (no sibling orphan-table scan in\n"
                + "    the db).\n"
                + "  --conf passes filesystem configuration for remote storage authentication.\n"
                + "    Keys use the same format as server.yaml (e.g. fs.oss.accessKeyId,\n"
                + "    fs.oss.accessKeySecret, fs.oss.endpoint, fs.oss.region). Repeatable.\n"
                + "\n"
                + "Examples:\n"
                + "  remove_orphan_files --bootstrap-server host:9123 --all-databases\n"
                + "    --conf fs.oss.accessKeyId=XXXX --conf fs.oss.accessKeySecret=YYYY\n"
                + "    --conf fs.oss.endpoint=oss-cn-hangzhou-internal.aliyuncs.com\n"
                + "    --conf fs.oss.region=cn-hangzhou";
    }
}
