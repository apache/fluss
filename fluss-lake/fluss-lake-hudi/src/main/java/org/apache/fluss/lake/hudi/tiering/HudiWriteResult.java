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

package org.apache.fluss.lake.hudi.tiering;

import org.apache.hudi.client.WriteStatus;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Write result produced by the Hudi lake writer and consumed by a future Hudi committer. */
public class HudiWriteResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, List<WriteStatus>> writeStatuses;
    private final Map<String, List<WriteStatus>> compactionWriteStatuses;

    public HudiWriteResult(
            Map<String, List<WriteStatus>> writeStatuses,
            Map<String, List<WriteStatus>> compactionWriteStatuses) {
        this.writeStatuses = writeStatuses;
        this.compactionWriteStatuses = compactionWriteStatuses;
    }

    public Map<String, List<WriteStatus>> getWriteStatuses() {
        return writeStatuses;
    }

    public Map<String, List<WriteStatus>> getCompactionWriteStatuses() {
        return compactionWriteStatuses;
    }
}
