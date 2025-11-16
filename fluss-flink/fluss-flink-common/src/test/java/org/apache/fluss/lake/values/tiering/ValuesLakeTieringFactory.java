/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.values.tiering;

import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;

import java.io.IOException;

/** Implementation of {@link LakeTieringFactory} for values lake. */
public class ValuesLakeTieringFactory
        implements LakeTieringFactory<
                ValuesLakeWriter.ValuesWriteResult, ValuesLakeCommitter.ValuesCommittable> {
    @Override
    public LakeWriter<ValuesLakeWriter.ValuesWriteResult> createLakeWriter(
            WriterInitContext writerInitContext) throws IOException {
        return new ValuesLakeWriter(writerInitContext.tablePath().toString());
    }

    @Override
    public SimpleVersionedSerializer<ValuesLakeWriter.ValuesWriteResult>
            getWriteResultSerializer() {
        return new ValuesLakeWriter.ValuesWriteResultSerializer();
    }

    @Override
    public LakeCommitter<ValuesLakeWriter.ValuesWriteResult, ValuesLakeCommitter.ValuesCommittable>
            createLakeCommitter(CommitterInitContext committerInitContext) throws IOException {
        return new ValuesLakeCommitter(committerInitContext.tablePath().toString());
    }

    @Override
    public SimpleVersionedSerializer<ValuesLakeCommitter.ValuesCommittable>
            getCommittableSerializer() {
        return new ValuesLakeCommitter.ValuesCommittableSerializer();
    }
}
