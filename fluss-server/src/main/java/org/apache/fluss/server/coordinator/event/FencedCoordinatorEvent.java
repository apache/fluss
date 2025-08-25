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

import org.apache.fluss.metadata.TableBucket;

/** A base interface for the events to be handled by Coordinator server with fenced attributes. */
public interface FencedCoordinatorEvent extends CoordinatorEvent {

    /**
     * Get the table bucket of the event.
     *
     * @return the table bucket of the event.
     */
    TableBucket getTableBucket();

    /**
     * Get the coordinator epoch of the event.
     *
     * @return the coordinator epoch of the event.
     */
    int getCoordinatorEpoch();

    /**
     * Get the bucket leader epoch of the bucket when the event is triggered.
     *
     * @return the bucket leader epoch of the bucket when the event is triggered.
     */
    int getBucketLeaderEpoch();
}
