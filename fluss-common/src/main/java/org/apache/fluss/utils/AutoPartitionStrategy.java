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

package org.apache.fluss.utils;

import org.apache.fluss.config.AutoPartitionDayFormat;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

/** A class wrapping the strategy for auto partition. */
public class AutoPartitionStrategy {

    private final boolean autoPartitionEnabled;
    private final String key;
    private final AutoPartitionTimeUnit timeUnit;
    private final AutoPartitionDayFormat dayFormat;
    private final int numPreCreate;
    private final int numToRetain;
    private final TimeZone timeZone;

    private AutoPartitionStrategy(
            boolean autoPartitionEnabled,
            String key,
            AutoPartitionTimeUnit autoPartitionTimeUnit,
            AutoPartitionDayFormat dayFormat,
            int numPreCreate,
            int numToRetain,
            TimeZone timeZone) {
        this.autoPartitionEnabled = autoPartitionEnabled;
        this.key = key;
        this.timeUnit = autoPartitionTimeUnit;
        this.dayFormat = dayFormat;
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
                AutoPartitionDayFormat.fromPattern(
                        conf.getString(ConfigOptions.TABLE_AUTO_PARTITION_DAY_FORMAT)),
                conf.getInt(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE),
                conf.getInt(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION),
                TimeZone.getTimeZone(conf.getString(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE)));
    }

    public boolean isAutoPartitionEnabled() {
        return autoPartitionEnabled;
    }

    public String key() {
        return key;
    }

    public AutoPartitionTimeUnit timeUnit() {
        return timeUnit;
    }

    public AutoPartitionDayFormat dayFormat() {
        return dayFormat;
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

    @Override
    public String toString() {
        return "AutoPartitionStrategy{"
                + "autoPartitionEnabled="
                + autoPartitionEnabled
                + ", key='"
                + key
                + '\''
                + ", timeUnit="
                + timeUnit
                + ", dayFormat="
                + dayFormat
                + ", numPreCreate="
                + numPreCreate
                + ", numToRetain="
                + numToRetain
                + ", timeZone="
                + timeZone
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AutoPartitionStrategy that = (AutoPartitionStrategy) o;
        return autoPartitionEnabled == that.autoPartitionEnabled
                && numPreCreate == that.numPreCreate
                && numToRetain == that.numToRetain
                && Objects.equals(key, that.key)
                && timeUnit == that.timeUnit
                && dayFormat == that.dayFormat
                && Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                autoPartitionEnabled,
                key,
                timeUnit,
                dayFormat,
                numPreCreate,
                numToRetain,
                timeZone);
    }
}
