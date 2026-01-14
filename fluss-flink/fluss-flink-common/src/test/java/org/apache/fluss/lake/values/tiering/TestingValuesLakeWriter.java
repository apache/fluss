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

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.values.TestingValuesLake;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.InstantiationUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

/** Implementation of {@link LakeWriter} for values lake. */
public class TestingValuesLakeWriter
        implements LakeWriter<TestingValuesLakeWriter.TestingValuesWriteResult> {
    private final String tableId;
    private final String writerId;

    public TestingValuesLakeWriter(String tableId) {
        this.tableId = tableId;
        this.writerId = UUID.randomUUID().toString();
    }

    @Override
    public void write(LogRecord record) throws IOException {
        TestingValuesLake.writeRecord(tableId, writerId, record);
    }

    @Override
    public TestingValuesWriteResult complete() throws IOException {
        return new TestingValuesWriteResult(writerId);
    }

    @Override
    public void close() throws IOException {}

    /** Write result of {@link TestingValuesLake}. */
    public static class TestingValuesWriteResult implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String stageId;

        public TestingValuesWriteResult(String stageId) {
            this.stageId = stageId;
        }

        public String getStageId() {
            return stageId;
        }
    }

    /** A serializer for {@link TestingValuesWriteResult}. */
    public static class TestingValuesWriteResultSerializer
            implements SimpleVersionedSerializer<TestingValuesWriteResult> {

        private static final int CURRENT_VERSION = 1;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(TestingValuesWriteResult valuesWriteResult) throws IOException {
            return InstantiationUtils.serializeObject(valuesWriteResult);
        }

        @Override
        public TestingValuesWriteResult deserialize(int version, byte[] serialized)
                throws IOException {
            TestingValuesWriteResult valuesWriteResult;
            try {
                valuesWriteResult =
                        InstantiationUtils.deserializeObject(
                                serialized, getClass().getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
            return valuesWriteResult;
        }
    }
}
