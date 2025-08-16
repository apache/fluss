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

package com.alibaba.fluss.flink.lake.hybrid;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.base.source.hybrid.HybridSourceEnumeratorState;
import org.apache.flink.connector.base.source.hybrid.HybridSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

/** todo. */
public class LakeFlussHybridSource
        implements Source<RowData, HybridSourceSplit, HybridSourceEnumeratorState> {

    private final HybridSource<RowData> hybridSource;

    public LakeFlussHybridSource(HybridSource<RowData> hybridSource) {
        this.hybridSource = hybridSource;
    }

    @Override
    public Boundedness getBoundedness() {
        return hybridSource.getBoundedness();
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> splitEnumeratorContext) throws Exception {
        return hybridSource.createEnumerator(splitEnumeratorContext);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> splitEnumeratorContext,
            HybridSourceEnumeratorState hybridSourceEnumeratorState)
            throws Exception {
        return hybridSource.restoreEnumerator(splitEnumeratorContext, hybridSourceEnumeratorState);
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceSplit> getSplitSerializer() {
        return hybridSource.getSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return hybridSource.getEnumeratorCheckpointSerializer();
    }

    @Override
    public SourceReader<RowData, HybridSourceSplit> createReader(
            SourceReaderContext sourceReaderContext) throws Exception {
        return hybridSource.createReader(sourceReaderContext);
    }
}
