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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.InvalidConsumerGroupIdException;

/** Utilities for consumer groups. */
@Internal
public final class ConsumerGroupUtils {

    private static final int MAX_GROUP_ID_LENGTH = 200;
    private static final String INTERNAL_GROUP_ID_PREFIX = "__";

    private ConsumerGroupUtils() {}

    /**
     * Validates a consumer group id.
     *
     * <p>Group ids must contain 1 to 200 ASCII letters, digits, underscores, or hyphens and must
     * not start with the reserved prefix {@code __}.
     *
     * @throws InvalidConsumerGroupIdException if the group id is invalid
     */
    public static void validateGroupId(String groupId) {
        if (groupId == null || groupId.isEmpty()) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer group id must not be null or empty.");
        }
        if (groupId.length() > MAX_GROUP_ID_LENGTH) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer group id must not exceed " + MAX_GROUP_ID_LENGTH + " characters.");
        }
        if (groupId.startsWith(INTERNAL_GROUP_ID_PREFIX)) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer group id must not start with '" + INTERNAL_GROUP_ID_PREFIX + "'.");
        }
        for (int i = 0; i < groupId.length(); i++) {
            char c = groupId.charAt(i);
            boolean valid =
                    (c >= 'a' && c <= 'z')
                            || (c >= 'A' && c <= 'Z')
                            || (c >= '0' && c <= '9')
                            || c == '_'
                            || c == '-';
            if (!valid) {
                throw new InvalidConsumerGroupIdException(
                        "Consumer group id may contain only ASCII letters, digits, '_' and '-'.");
            }
        }
    }
}
