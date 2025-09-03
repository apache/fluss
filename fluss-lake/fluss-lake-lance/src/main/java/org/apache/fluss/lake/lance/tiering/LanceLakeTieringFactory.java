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

package org.apache.fluss.lake.lance.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;

import java.io.IOException;

/** Implementation of {@link LakeTieringFactory} for Lance . */
public class LanceLakeTieringFactory
        implements LakeTieringFactory<LanceWriteResult, LanceCommittable> {
    private final Configuration config;

    public LanceLakeTieringFactory(Configuration config) {
        this.config = config;
    }

    @Override
    public LakeWriter<LanceWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        return new LanceLakeWriter(config, writerInitContext);
    }

    @Override
    public SimpleVersionedSerializer<LanceWriteResult> getWriteResultSerializer() {
        return new LanceWriteResultSerializer();
    }

    @Override
    public LakeCommitter<LanceWriteResult, LanceCommittable> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException {
        return new LanceLakeCommitter(config, committerInitContext.tablePath());
    }

    @Override
    public SimpleVersionedSerializer<LanceCommittable> getCommittableSerializer() {
        return new LanceCommittableSerializer();
    }
}
