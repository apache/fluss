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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.record.MemoryLogRecordsCompactedBuilder;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.rpc.messages.ProduceLogRequest;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A batch of log records managed in COMPACTED format that is or will be sent to server by {@link
 * ProduceLogRequest}.
 *
 * <p>This class is not thread safe and external synchronization must be used when modifying it.
 */
@NotThreadSafe
@Internal
public final class CompactedLogWriteBatch extends AbstractRowLogWriteBatch<CompactedRow> {

    public CompactedLogWriteBatch(
            int bucketId,
            PhysicalTablePath physicalTablePath,
            int schemaId,
            int writeLimit,
            AbstractPagedOutputView outputView,
            long createdMs) {
        super(
                bucketId,
                physicalTablePath,
                createdMs,
                outputView,
                MemoryLogRecordsCompactedBuilder.builder(schemaId, writeLimit, outputView, true),
                "Failed to build compacted log record batch.");
    }

    @Override
    protected CompactedRow requireAndCastRow(InternalRow row) {
        checkArgument(
                row instanceof CompactedRow, "row must be CompactedRow for compacted log table");
        return (CompactedRow) row;
    }
}
