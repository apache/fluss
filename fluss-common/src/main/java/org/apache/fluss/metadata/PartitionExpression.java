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
import java.util.Objects;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Metadata binding a virtual partition spec key to a partition transform. */
@PublicEvolving
public final class PartitionExpression implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String virtualPartitionSpecKey;
    private final PartitionTransform transform;

    private PartitionExpression(String virtualPartitionSpecKey, PartitionTransform transform) {
        this.virtualPartitionSpecKey = virtualPartitionSpecKey;
        this.transform = checkNotNull(transform, "partition transform must not be null.");
    }

    /** Creates an expression whose virtual partition spec key will be generated at build time. */
    public static PartitionExpression of(PartitionTransform transform) {
        return new PartitionExpression(null, transform);
    }

    /** Creates an expression with an explicit virtual partition spec key. */
    public static PartitionExpression of(
            String virtualPartitionSpecKey, PartitionTransform transform) {
        return new PartitionExpression(
                checkNotNull(
                        virtualPartitionSpecKey, "virtual partition spec key must not be null."),
                transform);
    }

    /** Returns the virtual partition spec key if it has been resolved. */
    public Optional<String> getVirtualPartitionSpecKey() {
        return Optional.ofNullable(virtualPartitionSpecKey);
    }

    /** Returns the transform. */
    public PartitionTransform getTransform() {
        return transform;
    }

    /** Returns a copy with a resolved virtual partition spec key. */
    public PartitionExpression withVirtualPartitionSpecKey(String virtualPartitionSpecKey) {
        return PartitionExpression.of(virtualPartitionSpecKey, transform);
    }

    /** Returns a copy with a resolved transform. */
    public PartitionExpression withTransform(PartitionTransform transform) {
        if (virtualPartitionSpecKey == null) {
            return PartitionExpression.of(transform);
        }
        return PartitionExpression.of(virtualPartitionSpecKey, transform);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionExpression that = (PartitionExpression) o;
        return Objects.equals(virtualPartitionSpecKey, that.virtualPartitionSpecKey)
                && Objects.equals(transform, that.transform);
    }

    @Override
    public int hashCode() {
        return Objects.hash(virtualPartitionSpecKey, transform);
    }

    @Override
    public String toString() {
        return "PartitionExpression{"
                + "virtualPartitionSpecKey='"
                + virtualPartitionSpecKey
                + '\''
                + ", transform="
                + transform
                + '}';
    }
}
