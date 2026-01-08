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
import java.util.HashSet;
import java.util.Set;

/**
 * Rebalance status.
 *
 * @since 0.9
 */
@PublicEvolving
public enum RebalanceStatus {
    NOT_STARTED(0),
    REBALANCING(1),
    FAILED(2),
    COMPLETED(3),
    CANCELED(4);

    public static final Set<RebalanceStatus> FINAL_STATUSES =
            new HashSet<>(Arrays.asList(COMPLETED, CANCELED, FAILED));

    private final int code;

    RebalanceStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static RebalanceStatus of(int code) {
        for (RebalanceStatus status : RebalanceStatus.values()) {
            if (status.code == code) {
                return status;
            }
        }
        return null;
    }
}
