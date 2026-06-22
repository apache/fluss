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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.hudi.utils.meta.CkpMetadataProvider;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;

import java.io.IOException;
import java.io.Serializable;

/** Hudi implementation of {@link LakeTieringFactory}. */
public class HudiLakeTieringFactory implements LakeTieringFactory<HudiWriteResult, Serializable> {

    private static final long serialVersionUID = 1L;

    private final HudiCatalogProvider hudiCatalogProvider;
    private final CkpMetadataProvider ckpMetadataProvider;

    public HudiLakeTieringFactory(Configuration hudiConfig) {
        this.hudiCatalogProvider = new HudiCatalogProvider(hudiConfig);
        this.ckpMetadataProvider = new CkpMetadataProvider();
    }

    @Override
    public LakeWriter<HudiWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        return new HudiLakeWriter(hudiCatalogProvider, ckpMetadataProvider, writerInitContext);
    }

    @Override
    public SimpleVersionedSerializer<HudiWriteResult> getWriteResultSerializer() {
        return new HudiWriteResultSerializer();
    }

    @Override
    public LakeCommitter<HudiWriteResult, Serializable> createLakeCommitter(
            CommitterInitContext committerInitContext) {
        throw new UnsupportedOperationException("Hudi lake committer is not implemented yet.");
    }

    @Override
    public SimpleVersionedSerializer<Serializable> getCommittableSerializer() {
        throw new UnsupportedOperationException(
                "Hudi lake committable serializer is not implemented yet.");
    }
}
