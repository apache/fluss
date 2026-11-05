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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Path to a column or nested field in a table schema. */
@PublicEvolving
public final class ColumnPath implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<Segment> segments;

    private ColumnPath(List<Segment> segments) {
        checkNotNull(segments, "Column path segments must not be null.");
        checkArgument(!segments.isEmpty(), "Column path must contain at least one segment.");
        this.segments = Collections.unmodifiableList(new ArrayList<>(segments));
        this.segments.forEach(
                segment -> checkNotNull(segment, "Column path segment must not be null."));
    }

    /** Creates a column path consisting of field segments. */
    public static ColumnPath of(String... fieldNames) {
        checkNotNull(fieldNames, "Field names must not be null.");
        List<Segment> segments =
                Arrays.stream(fieldNames).map(Segment::field).collect(Collectors.toList());
        return new ColumnPath(segments);
    }

    /** Creates a column path from the given segments. */
    public static ColumnPath of(Segment... segments) {
        checkNotNull(segments, "Path segments must not be null.");
        return new ColumnPath(Arrays.asList(segments));
    }

    /** Creates a column path from the given segments. */
    public static ColumnPath of(List<Segment> segments) {
        return new ColumnPath(segments);
    }

    /** Parses a dot-separated path as field segments. */
    public static ColumnPath parse(String dottedPath) {
        checkNotNull(dottedPath, "Column path must not be null.");
        checkArgument(!dottedPath.trim().isEmpty(), "Column path must not be empty.");
        return of(dottedPath.split("\\.", -1));
    }

    /** Returns all path segments. */
    public List<Segment> getSegments() {
        return segments;
    }

    /** Returns whether this path points below a top-level column. */
    public boolean isNested() {
        return segments.size() > 1;
    }

    @Override
    public String toString() {
        return segments.stream().map(Segment::toPathString).collect(Collectors.joining("."));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnPath)) {
            return false;
        }
        ColumnPath that = (ColumnPath) o;
        return Objects.equals(segments, that.segments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segments);
    }

    /** A path segment in a {@link ColumnPath}. */
    @PublicEvolving
    public static final class Segment implements Serializable {

        private static final long serialVersionUID = 1L;

        private final SegmentType type;
        private final String name;

        private Segment(SegmentType type, String name) {
            this.type = checkNotNull(type, "Segment type must not be null.");
            this.name = name;
        }

        /** Creates a field segment. */
        public static Segment field(String name) {
            checkNotNull(name, "Field segment name must not be null.");
            checkArgument(!name.trim().isEmpty(), "Field segment name must not be empty.");
            return new Segment(SegmentType.FIELD, name);
        }

        /** Creates an array element segment. */
        public static Segment arrayElement() {
            return new Segment(SegmentType.ARRAY_ELEMENT, null);
        }

        /** Creates a map value segment. */
        public static Segment mapValue() {
            return new Segment(SegmentType.MAP_VALUE, null);
        }

        /** Returns the type of this path segment. */
        public SegmentType getType() {
            return type;
        }

        /** Returns the field name, if this is a field segment. */
        public Optional<String> getName() {
            return Optional.ofNullable(name);
        }

        private String toPathString() {
            switch (type) {
                case FIELD:
                    return name;
                case ARRAY_ELEMENT:
                    return "$element";
                case MAP_VALUE:
                    return "$value";
                default:
                    throw new IllegalStateException("Unsupported column path segment type " + type);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Segment)) {
                return false;
            }
            Segment segment = (Segment) o;
            return type == segment.type && Objects.equals(name, segment.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, name);
        }

        @Override
        public String toString() {
            return toPathString();
        }
    }

    /** Segment type in a {@link ColumnPath}. */
    @PublicEvolving
    public enum SegmentType {
        FIELD,
        ARRAY_ELEMENT,
        MAP_VALUE
    }
}
