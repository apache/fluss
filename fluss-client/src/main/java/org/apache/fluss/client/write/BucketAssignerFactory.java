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

package org.apache.fluss.client.write;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableInfo;

/** Creates bucket assigners for individual write contexts. */
@FunctionalInterface
interface BucketAssignerFactory {

    /** Creates an assigner for the given table and physical write target. */
    BucketAssigner createBucketAssigner(TableInfo tableInfo, PhysicalTablePath path);

    /** Returns a factory using the configured strategy for tables without bucket keys. */
    static BucketAssignerFactory defaultFactory(Configuration conf) {
        ConfigOptions.NoKeyAssigner noKeyAssigner =
                conf.get(ConfigOptions.CLIENT_WRITER_BUCKET_NO_KEY_ASSIGNER);
        return (tableInfo, path) -> {
            if (!tableInfo.getBucketKeys().isEmpty()) {
                return new HashBucketAssigner(
                        BucketingFunction.of(
                                tableInfo.getTableConfig().getDataLakeFormat().orElse(null)));
            } else if (noKeyAssigner == ConfigOptions.NoKeyAssigner.ROUND_ROBIN) {
                return new RoundRobinBucketAssigner(path);
            } else if (noKeyAssigner == ConfigOptions.NoKeyAssigner.STICKY) {
                return new StickyBucketAssigner(path);
            } else {
                throw new IllegalStateException("Unknown no-key assigner: " + noKeyAssigner);
            }
        };
    }
}
