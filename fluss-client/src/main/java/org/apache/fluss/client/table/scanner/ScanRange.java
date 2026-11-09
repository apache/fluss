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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.InternalRowUtils;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Logical bounds for one range column. Finite endpoints contain exactly one non-null field of the
 * column's Fluss internal type. Endpoint rows are borrowed until {@link Scan#range} returns. The
 * configured scan retains independent copies, allowing callers to reuse the input rows.
 */
@PublicEvolving
public final class ScanRange {
    @Nullable final InternalRow lower;
    @Nullable final InternalRow upper;
    final boolean lowerInclusive;
    final boolean upperInclusive;

    private ScanRange(
            @Nullable InternalRow lower,
            boolean lowerInclusive,
            @Nullable InternalRow upper,
            boolean upperInclusive) {
        this.lower = lower;
        this.upper = upper;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
    }

    ScanRange copy(RowType endpointType) {
        return new ScanRange(
                copyEndpoint(lower, endpointType),
                lowerInclusive,
                copyEndpoint(upper, endpointType),
                upperInclusive);
    }

    @Nullable
    private static InternalRow copyEndpoint(@Nullable InternalRow endpoint, RowType endpointType) {
        if (endpoint == null) {
            return null;
        }
        checkArgument(
                endpoint.getFieldCount() == 1 && !endpoint.isNullAt(0),
                "Range endpoints must contain exactly one non-null field.");
        return InternalRowUtils.copyRow(endpoint, endpointType);
    }

    /** Creates [lower, upper). */
    public static ScanRange closedOpen(InternalRow lower, InternalRow upper) {
        return new ScanRange(
                checkNotNull(lower, "lower"), true, checkNotNull(upper, "upper"), false);
    }

    /** Creates [lower, upper]. */
    public static ScanRange closed(InternalRow lower, InternalRow upper) {
        return new ScanRange(
                checkNotNull(lower, "lower"), true, checkNotNull(upper, "upper"), true);
    }

    /** Creates (lower, upper). */
    public static ScanRange open(InternalRow lower, InternalRow upper) {
        return new ScanRange(
                checkNotNull(lower, "lower"), false, checkNotNull(upper, "upper"), false);
    }

    /** Creates (lower, upper]. */
    public static ScanRange openClosed(InternalRow lower, InternalRow upper) {
        return new ScanRange(
                checkNotNull(lower, "lower"), false, checkNotNull(upper, "upper"), true);
    }

    /** Creates [lower, +infinity). */
    public static ScanRange atLeast(InternalRow lower) {
        return new ScanRange(checkNotNull(lower, "lower"), true, null, false);
    }

    /** Creates (lower, +infinity). */
    public static ScanRange greaterThan(InternalRow lower) {
        return new ScanRange(checkNotNull(lower, "lower"), false, null, false);
    }

    /** Creates (-infinity, upper]. */
    public static ScanRange atMost(InternalRow upper) {
        return new ScanRange(null, false, checkNotNull(upper, "upper"), true);
    }

    /** Creates (-infinity, upper). */
    public static ScanRange lessThan(InternalRow upper) {
        return new ScanRange(null, false, checkNotNull(upper, "upper"), false);
    }
}
