/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

/**
 * A committed consumer offset with its user metadata (FIP-53). Stored as the value part of a
 * sys.consumer_offsets row.
 */
@Internal
public final class ConsumerOffsetAndMetadata {

    /** Canonical representation of absent user metadata. */
    public static final String EMPTY_METADATA = "";

    private final long offset;
    private final String metadata;

    /** Creates a committed consumer offset and normalizes null metadata to an empty string. */
    public ConsumerOffsetAndMetadata(long offset, @Nullable String metadata) {
        this.offset = offset;
        this.metadata = metadata == null ? EMPTY_METADATA : metadata;
    }

    /** Returns the committed offset. */
    public long getOffset() {
        return offset;
    }

    /** Returns the normalized, non-null metadata. */
    public String getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsumerOffsetAndMetadata that = (ConsumerOffsetAndMetadata) o;
        return offset == that.offset && metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        return 31 * result + metadata.hashCode();
    }

    @Override
    public String toString() {
        return "ConsumerOffsetAndMetadata{offset=" + offset + ", metadata='" + metadata + "'}";
    }
}
