/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.sink.operator;

import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.options.Options;

/** {@link StreamOperatorFactory} for {@link PaimonMultiWriterOperator}. */
public class PaimonMultiWriterOperatorFactory
        extends AbstractStreamOperatorFactory<MultiTableCommittable>
        implements OneInputStreamOperatorFactory<MultiplexCdcRecord, MultiTableCommittable> {

    private final CatalogLoader catalogLoader;
    private final StoreSinkWrite.WithWriteBufferProvider storeSinkWriteProvider;
    private final String initialCommitUser;
    private final Options options;

    public PaimonMultiWriterOperatorFactory(
            CatalogLoader catalogLoader,
            StoreSinkWrite.WithWriteBufferProvider storeSinkWriteProvider,
            String initialCommitUser,
            Options options) {
        this.catalogLoader = catalogLoader;
        this.storeSinkWriteProvider = storeSinkWriteProvider;
        this.initialCommitUser = initialCommitUser;
        this.options = options;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<MultiTableCommittable>> T createStreamOperator(
            StreamOperatorParameters<MultiTableCommittable> parameters) {
        return (T)
                new PaimonMultiWriterOperator(
                        catalogLoader,
                        storeSinkWriteProvider,
                        initialCommitUser,
                        parameters,
                        options);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return PaimonMultiWriterOperator.class;
    }
}
