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

package com.alibaba.fluss.lake.iceberg.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.committer.CommitterInitContext;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lake.writer.LakeTieringFactory;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;

import java.io.IOException;

/** Implementation of {@link LakeTieringFactory} for Iceberg. */
public class IcebergLakeTieringFactory
        implements LakeTieringFactory<IcebergWriteResult, IcebergCommittable> {

    private static final long serialVersionUID = 1L;

    private final IcebergCatalogProvider icebergCatalogProvider;

    public IcebergLakeTieringFactory(Configuration icebergConfig) {
        this.icebergCatalogProvider = new IcebergCatalogProvider(icebergConfig);
    }

    @Override
    public LakeWriter<IcebergWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        return new IcebergLakeWriter(icebergCatalogProvider, writerInitContext);
    }

    @Override
    public SimpleVersionedSerializer<IcebergWriteResult> getWriteResultSerializer() {
        return new IcebergWriteResultSerializer();
    }

    @Override
    public LakeCommitter<IcebergWriteResult, IcebergCommittable> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException {
        return new IcebergLakeCommitter(icebergCatalogProvider, committerInitContext.tablePath());
    }

    @Override
    public SimpleVersionedSerializer<IcebergCommittable> getCommittableSerializer() {
        return new IcebergCommittableSerializer();
    }
}
