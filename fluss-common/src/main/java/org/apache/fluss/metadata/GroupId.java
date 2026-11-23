/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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
import org.apache.fluss.exception.InvalidConsumerGroupIdException;

import java.io.Serializable;
import java.util.Objects;

/**
 * A concrete consumer-group identifier.
 *
 * <p>A group ID uses the same naming rules as a table name: 1 to 200 ASCII letters, digits,
 * underscores, or hyphens. The prefix {@code __} is reserved for internal names.
 */
@PublicEvolving
public final class GroupId implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final int MAX_GROUP_ID_LENGTH = 200;
    private static final String INTERNAL_NAME_PREFIX = "__";

    private final String groupId;

    private GroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * Creates a concrete consumer-group ID.
     *
     * @param groupId the group ID
     * @return the validated group ID
     * @throws InvalidConsumerGroupIdException if the ID is null, empty, too long, starts with
     *     {@code __}, or contains a character other than ASCII letters, digits, underscores, or
     *     hyphens
     */
    public static GroupId of(String groupId) {
        if (groupId == null || groupId.isEmpty()) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer group id must not be null or empty.");
        }
        if (groupId.length() > MAX_GROUP_ID_LENGTH) {
            throw new InvalidConsumerGroupIdException(
                    String.format(
                            "Consumer group id must not be longer than %d characters, but got %d characters.",
                            MAX_GROUP_ID_LENGTH, groupId.length()));
        }
        if (groupId.startsWith(INTERNAL_NAME_PREFIX)) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer group id must not start with '__' because it is reserved for"
                            + " internal names.");
        }
        if (containsInvalidCharacter(groupId)) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer group id must contain only ASCII letters, digits, underscores, or"
                            + " hyphens.");
        }
        return new GroupId(groupId);
    }

    /** Returns the validated consumer-group ID as a string. */
    public String getGroupId() {
        return groupId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupId that = (GroupId) o;
        return Objects.equals(groupId, that.groupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId);
    }

    @Override
    public String toString() {
        return groupId;
    }

    private static boolean containsInvalidCharacter(String groupId) {
        for (int i = 0; i < groupId.length(); i++) {
            char c = groupId.charAt(i);
            boolean valid =
                    (c >= 'a' && c <= 'z')
                            || (c >= 'A' && c <= 'Z')
                            || (c >= '0' && c <= '9')
                            || c == '_'
                            || c == '-';
            if (!valid) {
                return true;
            }
        }
        return false;
    }
}
