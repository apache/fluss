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

package org.apache.fluss.cluster.rebalance;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Arrays;

/**
 * The tag of tabletServer.
 *
 * @since 0.9
 */
@PublicEvolving
public enum ServerTag {
    /**
     * The tabletServer is permanently offline. Such as the host where the tabletServer on is
     * upcoming decommissioning.
     */
    PERMANENT_OFFLINE(0),

    /** The tabletServer is temporarily offline. Such as the tabletServer is upcoming upgrading. */
    TEMPORARY_OFFLINE(1);

    public final int value;

    ServerTag(int value) {
        this.value = value;
    }

    public static ServerTag valueOf(int value) {
        if (value == PERMANENT_OFFLINE.value) {
            return PERMANENT_OFFLINE;
        } else if (value == TEMPORARY_OFFLINE.value) {
            return TEMPORARY_OFFLINE;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Value %s must be one of %s",
                            value, Arrays.asList(ServerTag.values())));
        }
    }
}
