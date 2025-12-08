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

package org.apache.fluss.server.utils;

import org.apache.fluss.config.cluster.ConfigEntry;

import java.util.List;
import java.util.Objects;

import static org.apache.fluss.server.DynamicServerConfig.DATABASE_LIMITS_PREFIX;

/**
 * Analyze the database-level quota upper limit, and calculate the effective bucket and partition
 * upper limit of each database based on cluster configuration and dynamic configuration.
 */
public final class DatabaseLimitResolver {

    private DatabaseLimitResolver() {}

    public static int resolveMaxBucketForDb(
            int maxClusterBucketNum, List<ConfigEntry> entries, String database) {
        String key = DATABASE_LIMITS_PREFIX + database + ".max.bucket.num";
        Integer dbMax = findPositiveInt(entries, key);
        return dbMax == null ? maxClusterBucketNum : dbMax;
    }

    private static Integer findPositiveInt(List<ConfigEntry> entries, String key) {
        for (int i = entries.size() - 1; i >= 0; i--) {
            ConfigEntry e = entries.get(i);
            if (Objects.equals(e.key(), key)) {
                String val = e.value();
                if (val == null) {
                    continue;
                }
                try {
                    int v = Integer.parseInt(val);
                    if (v > 0) {
                        return v;
                    }
                } catch (NumberFormatException ignore) {
                    // ignore invalid integer
                }
            }
        }
        return null;
    }
}
