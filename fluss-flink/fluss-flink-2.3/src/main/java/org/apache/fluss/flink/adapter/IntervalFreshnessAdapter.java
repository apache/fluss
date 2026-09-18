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

package org.apache.fluss.flink.adapter;

import org.apache.flink.table.catalog.Interval;
import org.apache.flink.table.catalog.IntervalFreshness;

/** An adapter for {@link IntervalFreshness} for Flink 2.3. */
public class IntervalFreshnessAdapter {

    public static TimeUnitAdapter timeUnit(String name) {
        return new TimeUnitAdapter(Interval.TimeUnit.valueOf(name));
    }

    public static IntervalFreshness of(String interval, TimeUnitAdapter timeUnit) {
        return IntervalFreshness.of(interval, timeUnit.timeUnit);
    }

    public static String getTimeUnitName(IntervalFreshness intervalFreshness) {
        return intervalFreshness.getTimeUnit().name();
    }

    /** An adapter for {@link Interval.TimeUnit} for Flink 2.3. */
    public static class TimeUnitAdapter {
        private final Interval.TimeUnit timeUnit;

        private TimeUnitAdapter(Interval.TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }
    }
}
