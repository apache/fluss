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

package org.apache.fluss.cluster.maintencance;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Rebalance status for single bucket.
 *
 * @since 0.8
 */
@PublicEvolving
public enum RebalanceStatusForBucket {
    PENDING(1),
    REBALANCING(2),
    FAILED(3),
    COMPLETED(4);

    private final int code;

    RebalanceStatusForBucket(int code) {
        this.code = code;
    }

    public static RebalanceStatusForBucket of(int code) {
        for (RebalanceStatusForBucket status : RebalanceStatusForBucket.values()) {
            if (status.code == code) {
                return status;
            }
        }
        return null;
    }
}
