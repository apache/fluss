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

package org.apache.fluss.metrics;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.time.Duration;

/**
 * The {@link DescriptiveStatisticsHistogram} use a DescriptiveStatistics {@link
 * DescriptiveStatistics} as a Fluss {@link Histogram}.
 */
public class DescriptiveStatisticsHistogram implements Histogram {

    /** Default maximum age for samples collected by Fluss metric groups. */
    public static final Duration DEFAULT_MAX_AGE = Duration.ofMinutes(5);

    private final CircularDoubleArray descriptiveStatistics;

    public DescriptiveStatisticsHistogram(int windowSize) {
        this.descriptiveStatistics = new CircularDoubleArray(windowSize);
    }

    /**
     * Creates a histogram that retains samples for at most the given age.
     *
     * @param windowSize maximum number of retained samples
     * @param maxAge maximum age of retained samples
     */
    public DescriptiveStatisticsHistogram(int windowSize, Duration maxAge) {
        this(windowSize, maxAge, SystemClock.getInstance());
    }

    @VisibleForTesting
    DescriptiveStatisticsHistogram(int windowSize, Duration maxAge, Clock clock) {
        if (maxAge.isZero() || maxAge.isNegative() || maxAge.toMillis() == 0) {
            throw new IllegalArgumentException("Maximum sample age must be positive.");
        }
        this.descriptiveStatistics = new CircularDoubleArray(windowSize, maxAge.toMillis(), clock);
    }

    @Override
    public void update(long value) {
        this.descriptiveStatistics.addValue(value);
    }

    @Override
    public long getCount() {
        return this.descriptiveStatistics.getElementsSeen();
    }

    @Override
    public HistogramStatistics getStatistics() {
        return new DescriptiveStatisticsHistogramStatistics(this.descriptiveStatistics);
    }

    /** Fixed-size array that wraps around at the end and has a dynamic start position. */
    static class CircularDoubleArray {
        private final double[] backingArray;
        private final long[] timestamps;
        private final long maxAgeMillis;
        private final Clock clock;
        private int nextPos = 0;
        private boolean fullSize = false;
        private long elementsSeen = 0;

        CircularDoubleArray(int windowSize) {
            this(windowSize, Long.MAX_VALUE, SystemClock.getInstance());
        }

        CircularDoubleArray(int windowSize, long maxAgeMillis, Clock clock) {
            this.backingArray = new double[windowSize];
            this.timestamps = new long[windowSize];
            this.maxAgeMillis = maxAgeMillis;
            this.clock = clock;
        }

        synchronized void addValue(double value) {
            long now = clock.milliseconds();
            evictExpired(now);
            backingArray[nextPos] = value;
            timestamps[nextPos] = now;
            ++elementsSeen;
            ++nextPos;
            if (nextPos == backingArray.length) {
                nextPos = 0;
                fullSize = true;
            }
        }

        synchronized double[] toUnsortedArray() {
            evictExpired(clock.milliseconds());
            double[] result = new double[getSize()];
            System.arraycopy(backingArray, 0, result, 0, result.length);
            return result;
        }

        private void evictExpired(long now) {
            int size = getSize();
            int activeSize = 0;
            boolean expired = false;
            for (int i = 0; i < size; i++) {
                if (now - timestamps[i] < maxAgeMillis) {
                    backingArray[activeSize] = backingArray[i];
                    timestamps[activeSize++] = timestamps[i];
                } else {
                    expired = true;
                }
            }
            if (!expired) {
                return;
            }
            nextPos = activeSize;
            fullSize = activeSize == backingArray.length;
            if (fullSize) {
                nextPos = 0;
            }
        }

        private synchronized int getSize() {
            return fullSize ? backingArray.length : nextPos;
        }

        private synchronized long getElementsSeen() {
            return elementsSeen;
        }
    }
}
