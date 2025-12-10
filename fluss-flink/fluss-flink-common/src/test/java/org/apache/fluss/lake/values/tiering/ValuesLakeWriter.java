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
import org.apache.fluss.lake.values.ValuesLake;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.InstantiationUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

/** Implementation of {@link LakeWriter} for values lake. */
public class ValuesLakeWriter implements LakeWriter<ValuesLakeWriter.ValuesWriteResult> {
    private final String tableId;
    private final String stageId;

    public ValuesLakeWriter(String tableId) {
        this.tableId = tableId;
        this.stageId = UUID.randomUUID().toString();
    }

    @Override
    public void write(LogRecord record) throws IOException {
        ValuesLake.writeRecord(tableId, stageId, record);
    }

    @Override
    public ValuesWriteResult complete() throws IOException {
        return new ValuesWriteResult(stageId);
    }

    @Override
    public void close() throws IOException {}

    /** Write result of {@link ValuesLake}. */
    public static class ValuesWriteResult implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String stageId;

        public ValuesWriteResult(String stageId) {
            this.stageId = stageId;
        }

        public String getStageId() {
            return stageId;
        }
    }

    /** A serializer for {@link ValuesWriteResult}. */
    public static class ValuesWriteResultSerializer
            implements SimpleVersionedSerializer<ValuesWriteResult> {

        private static final int CURRENT_VERSION = 1;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(ValuesWriteResult valuesWriteResult) throws IOException {
            return InstantiationUtils.serializeObject(valuesWriteResult);
        }

        @Override
        public ValuesWriteResult deserialize(int version, byte[] serialized) throws IOException {
            ValuesWriteResult valuesWriteResult;
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
