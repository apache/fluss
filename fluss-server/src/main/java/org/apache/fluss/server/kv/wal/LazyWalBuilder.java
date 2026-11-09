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

package org.apache.fluss.server.kv.wal;

import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.function.SupplierWithException;

/** Allocates WAL encoding resources when the first output row is appended. */
public final class LazyWalBuilder implements WalBuilder {
    private final SupplierWithException<WalBuilder, Exception> factory;
    private WalBuilder delegate;
    private long writerId;
    private int batchSequence;

    public LazyWalBuilder(SupplierWithException<WalBuilder, Exception> factory) {
        this.factory = factory;
    }

    private WalBuilder delegate() throws Exception {
        if (delegate == null) {
            delegate = factory.get();
            delegate.setWriterState(writerId, batchSequence);
        }
        return delegate;
    }

    @Override
    public void append(ChangeType changeType, InternalRow row) throws Exception {
        delegate().append(changeType, row);
    }

    @Override
    public MemoryLogRecords build() throws Exception {
        return delegate().build();
    }

    @Override
    public void setWriterState(long writerId, int batchSequence) {
        this.writerId = writerId;
        this.batchSequence = batchSequence;
        if (delegate != null) {
            delegate.setWriterState(writerId, batchSequence);
        }
    }

    @Override
    public void deallocate() {
        if (delegate != null) {
            delegate.deallocate();
        }
    }
}
