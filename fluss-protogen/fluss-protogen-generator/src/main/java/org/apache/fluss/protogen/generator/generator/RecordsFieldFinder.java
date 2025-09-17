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

package org.apache.fluss.protogen.generator.generator;

import io.protostuff.parser.Field;
import io.protostuff.parser.Message;
import io.protostuff.parser.MessageField;

import java.util.HashSet;
import java.util.Set;

/** Finder to find <code>byte[] records</code> protobuf fields. */
public class RecordsFieldFinder {

    public static final String RECORDS_FIELD_NAME = "records";

    private Set<Message> visited = new HashSet<>();

    public boolean hasRecordsField(Message message) {
        visited.add(message);
        return message.getFields().stream().anyMatch(this::hasRecordsField);
    }

    private boolean hasRecordsField(Field<?> field) {
        if (field instanceof MessageField) {
            if (visited.contains(((MessageField) field).getMessage())) {
                return false;
            }
            return hasRecordsField(((MessageField) field).getMessage());
        } else if (field instanceof Field.Bytes) {
            return !field.isRepeated() && field.getName().equals(RECORDS_FIELD_NAME);
        } else {
            return false;
        }
    }
}
