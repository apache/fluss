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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.config.AutoPartitionTimeUnit;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A partition transform that truncates a date or timestamp source column to a time unit. */
@PublicEvolving
public final class DateTruncPartitionTransform implements PartitionTransform {

    private static final long serialVersionUID = 1L;

    private final String sourceColumn;
    private final AutoPartitionTimeUnit timeUnit;
    private final ZoneId timeZone;

    private DateTruncPartitionTransform(
            String sourceColumn, AutoPartitionTimeUnit timeUnit, ZoneId timeZone) {
        this.sourceColumn = checkNotNull(sourceColumn, "source column must not be null.");
        this.timeUnit = checkNotNull(timeUnit, "time unit must not be null.");
        this.timeZone = timeZone;
    }

    /** Creates an unresolved date-trunc transform whose time zone is resolved by the server. */
    public static DateTruncPartitionTransform of(
            String sourceColumn, AutoPartitionTimeUnit timeUnit) {
        return new DateTruncPartitionTransform(sourceColumn, timeUnit, null);
    }

    /** Creates a date-trunc transform with an explicit time zone. */
    public static DateTruncPartitionTransform of(
            String sourceColumn, AutoPartitionTimeUnit timeUnit, ZoneId timeZone) {
        return new DateTruncPartitionTransform(
                sourceColumn, timeUnit, checkNotNull(timeZone, "time zone must not be null."));
    }

    @Override
    public TransformType getType() {
        return TransformType.DATE_TRUNC;
    }

    @Override
    public List<String> getSourceColumns() {
        return Collections.singletonList(sourceColumn);
    }

    /** Returns the physical source column. */
    public String getSourceColumn() {
        return sourceColumn;
    }

    /** Returns the truncation unit. */
    public AutoPartitionTimeUnit getTimeUnit() {
        return timeUnit;
    }

    /** Returns the transform time zone if it has been resolved. */
    public Optional<ZoneId> getTimeZone() {
        return Optional.ofNullable(timeZone);
    }

    /** Returns a copy of this transform with the given resolved time zone. */
    public DateTruncPartitionTransform withTimeZone(ZoneId timeZone) {
        return DateTruncPartitionTransform.of(sourceColumn, timeUnit, timeZone);
    }

    /** Returns the default virtual partition spec key for this transform. */
    public String defaultPartitionSpecKey() {
        return sourceColumn + "_" + timeUnit.name().toLowerCase(Locale.ROOT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DateTruncPartitionTransform that = (DateTruncPartitionTransform) o;
        return Objects.equals(sourceColumn, that.sourceColumn)
                && timeUnit == that.timeUnit
                && Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceColumn, timeUnit, timeZone);
    }

    @Override
    public String toString() {
        return "DateTruncPartitionTransform{"
                + "sourceColumn='"
                + sourceColumn
                + '\''
                + ", timeUnit="
                + timeUnit
                + ", timeZone="
                + timeZone
                + '}';
    }
}
