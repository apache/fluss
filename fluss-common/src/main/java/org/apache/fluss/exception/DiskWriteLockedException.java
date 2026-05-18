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

package org.apache.fluss.exception;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Thrown by a tablet server to reject writes when its local data disk usage has reached the
 * configured write-limit ratio. The exception is retriable so that clients can retry once the
 * server frees up enough disk space and resumes accepting writes.
 */
@PublicEvolving
public class DiskWriteLockedException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public DiskWriteLockedException(String message) {
        super(message);
    }

    public DiskWriteLockedException(int serverId, double usageRatio, double limit) {
        super(
                String.format(
                        "TabletServer %d has rejected writes because the data disk usage "
                                + "reached %.2f%% (limit: %.2f%%). Free up space or scale the cluster.",
                        serverId, usageRatio * 100, limit * 100));
    }
}
