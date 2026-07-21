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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.annotation.Internal;

/**
 * Failover correction action for an aggregation sink.
 *
 * <p>The action is fixed when the sink topology is built. Checkpoints record the selected action;
 * restoring a NO_OP checkpoint as UNDO is unsupported because NO_OP checkpoints contain no bucket
 * offset baseline.
 */
@Internal
public enum RecoveryAction {
    /** Keep the current Fluss state without tracking or applying undo recovery. */
    NO_OP,

    /** Restore the checkpoint materialized state through the existing undo-recovery path. */
    UNDO
}
