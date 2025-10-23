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

package org.apache.fluss.flink.adapter;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

/**
 * Adapter for {@link SingleThreadMultiplexSourceReaderBase}.TODO: remove it until not supported in
 * flink 1.18.
 */
public abstract class SingleThreadMultiplexSourceReaderBaseAdapter<
                E, T, SplitT extends SourceSplit, SplitStateT>
        extends SingleThreadMultiplexSourceReaderBase<E, T, SplitT, SplitStateT> {
    public SingleThreadMultiplexSourceReaderBaseAdapter(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }
}
