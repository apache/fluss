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

package org.apache.fluss.flink.sink.shuffle;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.InternalRow;

/**
 * Assignment for a partition key, used to compute the target subtask for a record with a given
 * partition key.
 *
 * <p>Implementations of this interface determine how records with specific partition keys are
 * routed to downstream subtasks, enabling various distribution strategies such as round-robin,
 * key-hash, or weighted-random assignment.
 */
@Internal
public interface PartitionAssignment {
    int select(InternalRow row);
}
