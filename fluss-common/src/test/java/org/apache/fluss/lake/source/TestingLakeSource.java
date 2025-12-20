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

package org.apache.fluss.lake.source;

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.utils.CloseableIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** A testing implementation of {@link LakeSource}. */
public class TestingLakeSource implements LakeSource<LakeSplit> {

    // bucket num of source table
    private final int bucketNum;

    // partition infos of partitions contain lake splits
    private final List<PartitionInfo> partitionInfos;

    public TestingLakeSource() {
        this.bucketNum = 0;
        this.partitionInfos = null;
    }

    public TestingLakeSource(int bucketNum, List<PartitionInfo> partitionInfos) {
        this.bucketNum = bucketNum;
        this.partitionInfos = partitionInfos;
    }

    @Override
    public void withProject(int[][] project) {}

    @Override
    public void withLimit(int limit) {}

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        return null;
    }

    @Override
    public Planner<LakeSplit> createPlanner(PlannerContext context) throws IOException {
        return new TestingPlanner(bucketNum, partitionInfos);
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<LakeSplit> context) throws IOException {
        return CloseableIterator::emptyIterator;
    }

    @Override
    public SimpleVersionedSerializer<LakeSplit> getSplitSerializer() {
        return new SimpleVersionedSerializer<LakeSplit>() {

            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(LakeSplit split) throws IOException {
                if (split instanceof TestingLakeSplit) {
                    TestingLakeSplit testingSplit = (TestingLakeSplit) split;
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    try (DataOutputStream dos = new DataOutputStream(baos)) {
                        // Serialize bucket
                        dos.writeInt(testingSplit.bucket());

                        // Serialize partition list
                        List<String> partition = testingSplit.partition();
                        if (partition == null) {
                            dos.writeInt(-1);
                        } else {
                            dos.writeInt(partition.size());
                            for (String part : partition) {
                                // Write a boolean flag to indicate if the string is null
                                dos.writeBoolean(part != null);
                                if (part != null) {
                                    dos.writeUTF(part);
                                }
                            }
                        }
                    }
                    return baos.toByteArray();
                }
                throw new IOException("Unsupported split type: " + split.getClass().getName());
            }

            @Override
            public LakeSplit deserialize(int version, byte[] serialized) throws IOException {
                if (version != 0) {
                    throw new IOException("Unsupported version: " + version);
                }

                try (DataInputStream dis =
                        new DataInputStream(new ByteArrayInputStream(serialized))) {
                    // Deserialize bucket
                    int bucket = dis.readInt();

                    // Deserialize partition list
                    int partitionSize = dis.readInt();
                    List<String> partition;
                    if (partitionSize < 0) {
                        partition = null;
                    } else {
                        partition = new ArrayList<>(partitionSize);
                        for (int i = 0; i < partitionSize; i++) {
                            // Read boolean flag to determine if the string is null
                            boolean isNotNull = dis.readBoolean();
                            String part = isNotNull ? dis.readUTF() : null;
                            partition.add(part);
                        }
                    }

                    return new TestingLakeSplit(bucket, partition);
                }
            }
        };
    }
}
