/*
 * Copyright 2020 Splunk Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.protogen.generator.generator;

import io.protostuff.parser.Field;
import io.protostuff.parser.Message;
import io.protostuff.parser.MessageField;

import java.util.HashSet;
import java.util.Set;

/** Finds protobuf messages with reachable error fields. */
final class ErrorCodeFieldFinder {

    private ErrorCodeFieldFinder() {}

    static boolean hasErrorFields(Message message) {
        return hasErrorFields(message, new HashSet<>());
    }

    private static boolean hasErrorFields(Message message, Set<Message> visited) {
        if (!visited.add(message)) {
            return false;
        }

        if (ProtobufMessage.hasErrorFields(message)) {
            return true;
        }
        for (Field<?> field : message.getFields()) {
            if (field instanceof MessageField
                    && hasErrorFields(((MessageField) field).getMessage(), visited)) {
                return true;
            }
        }
        return false;
    }
}
