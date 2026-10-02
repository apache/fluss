/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Result of looking up a key from a KV state source.
 *
 * <p>{@link Status#NOT_FOUND} means that the queried source has no state for the key, so the caller
 * may consult another source. {@link Status#DELETED} confirms that the key has been deleted and
 * must stop fallback from exposing an older value.
 */
@Internal
public final class KvStateLookupResult {

    /** Status of a KV state lookup. */
    public enum Status {
        /** The key was not found in the queried state source. */
        NOT_FOUND,

        /** The queried state source contains a non-empty encoded value. */
        PRESENT,

        /** The key is known to have been deleted. */
        DELETED
    }

    private static final KvStateLookupResult NOT_FOUND =
            new KvStateLookupResult(Status.NOT_FOUND, null);
    private static final KvStateLookupResult DELETED =
            new KvStateLookupResult(Status.DELETED, null);

    private final Status status;
    private final @Nullable byte[] value;

    private KvStateLookupResult(Status status, @Nullable byte[] value) {
        this.status = status;
        this.value = value;
    }

    /** Returns a result indicating that the key was not found in the queried state source. */
    public static KvStateLookupResult notFound() {
        return NOT_FOUND;
    }

    /** Returns a result containing a non-empty encoded value. */
    public static KvStateLookupResult present(byte[] value) {
        checkNotNull(value, "value must not be null");
        checkArgument(value.length > 0, "value must not be empty");
        return new KvStateLookupResult(Status.PRESENT, value);
    }

    /** Returns a result indicating that the key is known to have been deleted. */
    public static KvStateLookupResult deleted() {
        return DELETED;
    }

    /** Returns the lookup status. */
    public Status status() {
        return status;
    }

    /** Returns whether this result contains an encoded value. */
    public boolean isPresent() {
        return status == Status.PRESENT;
    }

    /** Returns whether the key is known to have been deleted. */
    public boolean isDeleted() {
        return status == Status.DELETED;
    }

    /**
     * Returns the encoded value, or null when the key is absent or deleted.
     *
     * <p>The returned byte array is owned by the underlying state and must not be modified.
     */
    public @Nullable byte[] value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KvStateLookupResult that = (KvStateLookupResult) o;
        return status == that.status && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return 31 * status.hashCode() + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "KvStateLookupResult{"
                + "status="
                + status
                + ", value="
                + Arrays.toString(value)
                + '}';
    }
}
