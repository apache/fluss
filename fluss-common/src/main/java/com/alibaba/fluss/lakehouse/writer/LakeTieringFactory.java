/*
 *  Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.lakehouse.writer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * The LakeTieringFactory interface defines how to create lake writers and committers. It provides
 * methods to create writers and committers for Fluss's rows to Paimon/Iceberg rows, and to obtain
 * serializers for write results and committable objects.
 *
 * @param <WriteResult> the type of the write result
 * @param <CommitableT> the type of the committable object
 * @since 0.7
 */
@PublicEvolving
public interface LakeTieringFactory<WriteResult, CommitableT> {

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
    LakeCommitter<WriteResult, CommitableT> createLakeCommitter() throws IOException;

    /**
     * Returns the serializer for committable objects.
     *
     * @return the serializer for committable objects
     */
    SimpleVersionedSerializer<CommitableT> getCommitableSerializer();
}
