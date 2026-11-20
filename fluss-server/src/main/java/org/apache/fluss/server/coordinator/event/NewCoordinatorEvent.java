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

import org.apache.fluss.server.zk.data.CoordinatorAddress;

import java.util.Objects;

/** An event for new coordinator server. */
public class NewCoordinatorEvent implements CoordinatorEvent {

    private final CoordinatorAddress coordinatorAddress;

    public NewCoordinatorEvent(CoordinatorAddress coordinatorAddress) {
        this.coordinatorAddress = coordinatorAddress;
    }

    public CoordinatorAddress getCoordinatorAddress() {
        return coordinatorAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NewCoordinatorEvent that = (NewCoordinatorEvent) o;
        return Objects.equals(coordinatorAddress, that.coordinatorAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(coordinatorAddress);
    }

    @Override
    public String toString() {
        return "NewCoordinatorEvent{" + "coordinatorAddress=" + coordinatorAddress + '}';
    }
}
