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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.server.metadata.ServerInfo;

import javax.annotation.Nullable;

/** API versions, or a scheduled retry, for a particular tablet server registration. */
public class TabletServerApiVersionsEvent implements CoordinatorEvent {
    private final ServerInfo registration;
    @Nullable private final ApiVersionsResponse response;

    public TabletServerApiVersionsEvent(
            ServerInfo registration, @Nullable ApiVersionsResponse response) {
        this.registration = registration;
        this.response = response;
    }

    public ServerInfo getRegistration() {
        return registration;
    }

    @Nullable
    public ApiVersionsResponse getResponse() {
        return response;
    }
}
