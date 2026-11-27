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

package org.apache.fluss.flink.action.load;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.action.Action;
import org.apache.fluss.flink.action.ActionFactory;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.StringUtils;

import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.PropertiesUtils.extractAndRemovePrefix;

/** Factory for loading Paimon data using the Manager's existing action arguments. */
@Internal
public class LoadLakeDataToFlussActionFactory implements ActionFactory {

    @Override
    public String identifier() {
        return "load_lake_data_to_fluss";
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        if (params.has("help") || params.has("h")) {
            return Optional.empty();
        }
        String table = params.get("table");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(table), "--table is required.");
        String[] identifiers = table.trim().split("\\.", -1);
        checkArgument(identifiers.length == 2, "--table must be in database.table format.");
        TablePath tablePath = TablePath.of(identifiers[0], identifiers[1]);
        checkArgument(tablePath.isValid(), "--table must contain a valid database and table name.");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(params.get("fluss.bootstrap.servers")),
                "--fluss.bootstrap.servers is required.");
        checkArgument(
                !params.has("datalake.format") || "paimon".equals(params.get("datalake.format")),
                "--datalake.format must be paimon.");
        Map<String, String> options = params.toMap();
        String snapshotId = params.get("source-snapshot-id");
        String parallelism = params.get("parallelism");
        String expectedTableId = params.get("expected-table-id");
        return Optional.of(
                new LoadLakeDataToFlussAction(
                        Configuration.fromMap(extractAndRemovePrefix(options, "fluss.")),
                        Configuration.fromMap(extractAndRemovePrefix(options, "datalake.paimon.")),
                        tablePath,
                        snapshotId == null ? null : Long.valueOf(snapshotId),
                        parallelism == null ? null : Integer.valueOf(parallelism),
                        params.get("bulk-load-id"),
                        expectedTableId == null ? null : Long.valueOf(expectedTableId)));
    }

    @Override
    public String help() {
        return "Usage: load_lake_data_to_fluss --table <database.table>\n"
                + "  --fluss.bootstrap.servers <host:port> [--fluss.<option> <value>]...\n"
                + "  [--datalake.format paimon] [--datalake.paimon.<option> <value>]...\n"
                + "  [--source-snapshot-id <id> --bulk-load-id <id>] [--expected-table-id <id>]\n"
                + "  [--parallelism <n>]\n"
                + "\n"
                + "The Fluss primary-key table must be newly created with lake access disabled.\n"
                + "Prevent ordinary Fluss writes until initialization completes, including"
                + " resumptions.\n"
                + "The source must have a readable Paimon snapshot. Without"
                + " --source-snapshot-id,\n"
                + "the latest snapshot is selected once when the action starts.\n"
                + "The job selects all source partitions for regular tables, or the retention\n"
                + "window and future source partitions for auto-partitioned tables.\n"
                + "Load parallelism defaults to the table's bucket count; --parallelism"
                + " overrides it.\n"
                + "Use --fluss.client.fs.<option> for writable remote-storage credentials.\n"
                + "The job registers the loaded snapshot and enables lake access after commit.\n"
                + "An omitted Bulk Load ID is generated once before Begin and carried in the"
                + " JobGraph.\n"
                + "Supply the snapshot ID and Bulk Load ID together, preserving the original"
                + " pair on retries.\n"
                + "Retries use the original transaction's frozen partitions.\n"
                + "Deleted physical targets are skipped, including when their names are reused.\n"
                + "Stop all old writers before resubmission. Failures preserve the original"
                + " transaction.\n"
                + "The job preserves the historical-partition setting and configured restart"
                + " strategy.\n"
                + "For whole-job retries, configure jobmanager.execution.failover-strategy=full"
                + " on the Flink cluster.\n";
    }
}
