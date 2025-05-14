/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils;

import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;

import java.util.Map;
import java.util.TimeZone;

/** A class wrapping the strategy for auto partition. */
public class AutoPartitionStrategy {

    private final boolean autoPartitionEnable;
    private final String key;
    private final AutoPartitionTimeUnit timeUnit;
    private final int numPreCreate;
    private final int numToRetain;
    private final TimeZone timeZone;

    private AutoPartitionStrategy(
            boolean autoPartitionEnable,
            String key,
            AutoPartitionTimeUnit autoPartitionTimeUnit,
            int numPreCreate,
            int numToRetain,
            TimeZone timeZone) {
        this.autoPartitionEnable = autoPartitionEnable;
        this.key = key;
        this.timeUnit = autoPartitionTimeUnit;
        this.numPreCreate = numPreCreate;
        this.numToRetain = numToRetain;
        this.timeZone = timeZone;
    }

    public static AutoPartitionStrategy from(Map<String, String> options) {
        return from(Configuration.fromMap(options));
    }

    public static AutoPartitionStrategy from(Configuration conf) {
        return new AutoPartitionStrategy(
                conf.getBoolean(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED),
                conf.getString(ConfigOptions.TABLE_AUTO_PARTITION_KEY),
                conf.get(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT),
                conf.getInt(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE),
                conf.getInt(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION),
                TimeZone.getTimeZone(conf.getString(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE)));
    }

    public boolean isAutoPartitionEnabled() {
        return autoPartitionEnable;
    }

    public String key() {
        return key;
    }

    public AutoPartitionTimeUnit timeUnit() {
        return timeUnit;
    }

    public int numPreCreate() {
        return numPreCreate;
    }

    public int numToRetain() {
        return numToRetain;
    }

    public TimeZone timeZone() {
        return timeZone;
    }
}
