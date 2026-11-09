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

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Bounds of an interval, with optional lower and upper endpoints.
 *
 * <p>Endpoint references and inclusion flags are fixed. Endpoint objects are borrowed. The API
 * consuming the range defines when it captures endpoint values. Endpoint comparison and validation
 * depend on that API's type information.
 *
 * @param <T> the endpoint type
 */
@PublicEvolving
public final class Range<T> {
    @Nullable private final T lowerBound;
    @Nullable private final T upperBound;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;

    private Range(
            @Nullable T lowerBound,
            boolean lowerInclusive,
            @Nullable T upperBound,
            boolean upperInclusive) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
    }

    /** Returns the lower endpoint, or null if unbounded below. */
    @Nullable
    public T getLowerBound() {
        return lowerBound;
    }

    /** Returns the upper endpoint, or null if unbounded above. */
    @Nullable
    public T getUpperBound() {
        return upperBound;
    }

    /** Whether the finite lower endpoint is included. */
    public boolean isLowerInclusive() {
        return lowerInclusive;
    }

    /** Whether the finite upper endpoint is included. */
    public boolean isUpperInclusive() {
        return upperInclusive;
    }

    /** Creates [lower, upper). */
    public static <T> Range<T> closedOpen(T lower, T upper) {
        return new Range<>(checkNotNull(lower, "lower"), true, checkNotNull(upper, "upper"), false);
    }

    /** Creates [lower, upper]. */
    public static <T> Range<T> closed(T lower, T upper) {
        return new Range<>(checkNotNull(lower, "lower"), true, checkNotNull(upper, "upper"), true);
    }

    /** Creates (lower, upper). */
    public static <T> Range<T> open(T lower, T upper) {
        return new Range<>(
                checkNotNull(lower, "lower"), false, checkNotNull(upper, "upper"), false);
    }

    /** Creates (lower, upper]. */
    public static <T> Range<T> openClosed(T lower, T upper) {
        return new Range<>(checkNotNull(lower, "lower"), false, checkNotNull(upper, "upper"), true);
    }

    /** Creates [lower, +infinity). */
    public static <T> Range<T> atLeast(T lower) {
        return new Range<>(checkNotNull(lower, "lower"), true, null, false);
    }

    /** Creates (lower, +infinity). */
    public static <T> Range<T> greaterThan(T lower) {
        return new Range<>(checkNotNull(lower, "lower"), false, null, false);
    }

    /** Creates (-infinity, upper]. */
    public static <T> Range<T> atMost(T upper) {
        return new Range<>(null, false, checkNotNull(upper, "upper"), true);
    }

    /** Creates (-infinity, upper). */
    public static <T> Range<T> lessThan(T upper) {
        return new Range<>(null, false, checkNotNull(upper, "upper"), false);
    }
}
