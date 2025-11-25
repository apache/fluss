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

import org.apache.fluss.lake.values.ValuesLake;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.record.LogRecord;

import java.io.IOException;
import java.util.UUID;

/** {@link LakeWriter} for {@link ValuesLake}. */
public class ValuesLakeWriter implements LakeWriter<ValuesWriteResult> {
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
}
