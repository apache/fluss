/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.server.tablet.consumer.group.result;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.rpc.protocol.Errors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Result of listing consumer groups stored on a tablet server. */
@Internal
public final class ListConsumerGroupsResult {

    private final Errors error;
    private final List<String> groupIds;

    private ListConsumerGroupsResult(Errors error, List<String> groupIds) {
        this.error = checkNotNull(error, "error must not be null");
        this.groupIds =
                Collections.unmodifiableList(
                        new ArrayList<>(checkNotNull(groupIds, "groupIds must not be null")));
    }

    public static ListConsumerGroupsResult success(List<String> groupIds) {
        return new ListConsumerGroupsResult(Errors.NONE, groupIds);
    }

    public static ListConsumerGroupsResult failure(Errors error) {
        return new ListConsumerGroupsResult(error, Collections.emptyList());
    }

    public Errors getError() {
        return error;
    }

    public List<String> getGroupIds() {
        return groupIds;
    }
}
