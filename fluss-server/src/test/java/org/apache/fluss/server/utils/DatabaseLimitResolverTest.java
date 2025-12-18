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
import org.apache.fluss.server.DynamicServerConfig;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DatabaseLimitResolverTest {

    @Test
    void returnsClusterDefaultWhenNoEntry() {
        int clusterDefault = 16;
        List<ConfigEntry> entries = new ArrayList<>();
        String db = "testdb";
        int resolved = DatabaseLimitResolver.resolveMaxBucketForDb(clusterDefault, entries, db);
        assertThat(resolved).isEqualTo(clusterDefault);
    }

    @Test
    void returnsDatabaseSpecificLimitWhenPresent() {
        int clusterDefault = 16;
        String db = "testdb";
        String key = DynamicServerConfig.DATABASE_LIMITS_PREFIX + db + ".max.bucket.num";
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry(key, "32", ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG));

        int resolved = DatabaseLimitResolver.resolveMaxBucketForDb(clusterDefault, entries, db);
        assertThat(resolved).isEqualTo(32);
    }

    @Test
    void laterEntryOverridesEarlierSameKey() {
        int clusterDefault = 10;
        String db = "mydb";
        String key = DynamicServerConfig.DATABASE_LIMITS_PREFIX + db + ".max.bucket.num";
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry(key, "5", ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG));
        entries.add(new ConfigEntry(key, "8", ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG));

        int resolved = DatabaseLimitResolver.resolveMaxBucketForDb(clusterDefault, entries, db);
        assertThat(resolved).isEqualTo(8);
    }

    @Test
    void ignoresInvalidValuesAndFallsBackToEarlierValid() {
        int clusterDefault = 20;
        String db = "db1";
        String key = DynamicServerConfig.DATABASE_LIMITS_PREFIX + db + ".max.bucket.num";
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry(key, "7", ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG));
        entries.add(new ConfigEntry(key, "abc", ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG));

        int resolved = DatabaseLimitResolver.resolveMaxBucketForDb(clusterDefault, entries, db);
        assertThat(resolved).isEqualTo(7);
    }

    @Test
    void ignoresNonPositiveValues() {
        int clusterDefault = 12;
        String db = "db2";
        String key = DynamicServerConfig.DATABASE_LIMITS_PREFIX + db + ".max.bucket.num";
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry(key, "0", ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG));
        entries.add(new ConfigEntry(key, "-3", ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG));

        int resolved = DatabaseLimitResolver.resolveMaxBucketForDb(clusterDefault, entries, db);
        assertThat(resolved).isEqualTo(clusterDefault);
    }

    @Test
    void ignoresNullValues() {
        int clusterDefault = 9;
        String db = "db3";
        String key = DynamicServerConfig.DATABASE_LIMITS_PREFIX + db + ".max.bucket.num";
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry(key, null, ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG));

        int resolved = DatabaseLimitResolver.resolveMaxBucketForDb(clusterDefault, entries, db);
        assertThat(resolved).isEqualTo(clusterDefault);
    }

    @Test
    void ignoresEntriesForOtherDatabases() {
        int clusterDefault = 40;
        String db = "dbA";
        String keyA = DynamicServerConfig.DATABASE_LIMITS_PREFIX + db + ".max.bucket.num";
        String keyB = DynamicServerConfig.DATABASE_LIMITS_PREFIX + "dbB" + ".max.bucket.num";
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry(keyA, "20", ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG));
        entries.add(new ConfigEntry(keyB, "100", ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG));

        int resolved = DatabaseLimitResolver.resolveMaxBucketForDb(clusterDefault, entries, db);
        assertThat(resolved).isEqualTo(20);
    }
}
