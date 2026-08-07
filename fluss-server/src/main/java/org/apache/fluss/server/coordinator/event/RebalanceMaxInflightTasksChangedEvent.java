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

/** An event fired when the rebalance max in-flight task limit changes dynamically. */
public class RebalanceMaxInflightTasksChangedEvent implements CoordinatorEvent {

    private final int maxInflightTasks;

    public RebalanceMaxInflightTasksChangedEvent(int maxInflightTasks) {
        this.maxInflightTasks = maxInflightTasks;
    }

    public int getMaxInflightTasks() {
        return maxInflightTasks;
    }

    @Override
    public String toString() {
        return "RebalanceMaxInflightTasksChangedEvent{maxInflightTasks=" + maxInflightTasks + "}";
    }
}
