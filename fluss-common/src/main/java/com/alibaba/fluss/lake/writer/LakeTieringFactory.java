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

package com.alibaba.fluss.lake.writer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.lake.committer.CommitterInitContext;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;

/**
 * The LakeTieringFactory interface defines how to create lake writers and committers. It provides
 * methods to create writers and committers for Fluss's rows to Paimon/Iceberg rows, and to obtain
 * serializers for write results and committable objects.
 *
 * @param <WriteResult> the type of the write result
 * @param <CommittableT> the type of the committable object
 * @since 0.7
 */
@PublicEvolving
public interface LakeTieringFactory<WriteResult, CommittableT> extends Serializable {

    String FLUSS_LAKE_TIERING_COMMIT_USER = "__fluss_lake_tiering";

    /**
     * Creates a lake writer to write Fluss's rows to Paimon/Iceberg rows.
     *
     * @param writerInitContext the context for initializing the writer
     * @return the lake writer
     * @throws IOException if an I/O error occurs
     */
    LakeWriter<WriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException;

    /**
     * Returns the serializer for write results.
     *
     * @return the serializer for write results
     */
    SimpleVersionedSerializer<WriteResult> getWriteResultSerializer();

    /**
     * Creates a lake committer to commit to Paimon/Iceberg.
     *
     * @return the lake committer
     * @throws IOException if an I/O error occurs
     */
    LakeCommitter<WriteResult, CommittableT> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException;

    /**
     * Returns the serializer for committable objects.
     *
     * @return the serializer for committable objects
     */
    SimpleVersionedSerializer<CommittableT> getCommittableSerializer();
}
