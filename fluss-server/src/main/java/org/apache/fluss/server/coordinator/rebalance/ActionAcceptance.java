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

package org.apache.fluss.server.coordinator.rebalance;

/** Flags to indicate if an action is acceptable by the goal(s). */
public enum ActionAcceptance {
    /** Action is acceptable -- i.e. it does not violate goal constraints. */
    ACCEPT,
    /**
     * Action is rejected in replica-level. But, the destination tabletServer may potentially accept
     * actions of the same {@link ActionType} from the source tabletServer specified in the given
     * action.
     */
    REPLICA_REJECT,

    /**
     * Action is rejected in server-level. hence, the destination tabletServer does not accept
     * actions of the same {@link ActionType} from the source tabletServer specified in the given
     * action.
     */
    SERVER_REJECT
}
