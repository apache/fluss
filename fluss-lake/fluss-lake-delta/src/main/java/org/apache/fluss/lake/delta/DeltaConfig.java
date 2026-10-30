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

package org.apache.fluss.lake.delta;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;

import java.util.Map;

/**
 * Options of the Delta Lake storage.
 *
 * <p>The options come from the {@code datalake.delta.*} cluster options with the prefix removed.
 * {@code warehouse} is required. All options are also passed to the Hadoop configuration of the
 * Delta Kernel engine, so file system options such as {@code fs.s3a.endpoint} can be set the same
 * way.
 */
public class DeltaConfig {

    /** The root path that holds one directory per Delta table: {@code warehouse/db/table}. */
    public static final String WAREHOUSE = "warehouse";

    /**
     * The number of rows a lake writer buffers before it writes them out as Parquet files. Each
     * flush writes at least one file, so a larger value gives fewer and bigger files at the cost of
     * memory.
     */
    public static final String WRITE_BATCH_SIZE = "write.batch-size";

    /** The engine name recorded in the {@code commitInfo} of every Delta commit. */
    public static final String ENGINE_INFO = "Apache-Fluss";

    static final int DEFAULT_WRITE_BATCH_SIZE = 100_000;

    private DeltaConfig() {}

    /** Returns the path of the Delta table for the given Fluss table. */
    public static String tablePath(Configuration options, TablePath tablePath) {
        String warehouse = options.toMap().get(WAREHOUSE);
        if (warehouse == null) {
            throw new IllegalArgumentException("Missing required option " + WAREHOUSE);
        }
        while (warehouse.endsWith("/")) {
            warehouse = warehouse.substring(0, warehouse.length() - 1);
        }
        return warehouse + "/" + tablePath.getDatabaseName() + "/" + tablePath.getTableName();
    }

    /** Returns the write batch size, a table custom property wins over the cluster option. */
    public static int writeBatchSize(Configuration options, Map<String, String> tableProperties) {
        String value = tableProperties.get(WRITE_BATCH_SIZE);
        if (value == null) {
            value = options.toMap().get(WRITE_BATCH_SIZE);
        }
        int batchSize = value == null ? DEFAULT_WRITE_BATCH_SIZE : Integer.parseInt(value);
        if (batchSize <= 0) {
            throw new IllegalArgumentException(
                    WRITE_BATCH_SIZE + " must be positive, but is " + batchSize);
        }
        return batchSize;
    }

    /** Creates a Delta Kernel engine backed by Hadoop file systems. */
    public static Engine createEngine(Configuration options) {
        org.apache.hadoop.conf.Configuration hadoopConf =
                new org.apache.hadoop.conf.Configuration();
        for (Map.Entry<String, String> entry : options.toMap().entrySet()) {
            hadoopConf.set(entry.getKey(), entry.getValue());
        }
        return DefaultEngine.create(hadoopConf);
    }
}
