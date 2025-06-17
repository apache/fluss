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

package com.alibaba.fluss.server.log;

import com.alibaba.fluss.server.log.WriterStateManager.WriterSnapshotEntry;
import com.alibaba.fluss.server.log.WriterStateManager.WriterSnapshotMap;
import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

import java.util.Arrays;
import java.util.List;

/** Tests for {@link com.alibaba.fluss.server.log.WriterStateManager.WriterSnapshotMapJsonSerde}. */
public class WriterSnapshotMapJsonSerdeTest extends JsonSerdeTestBase<WriterSnapshotMap> {

    public WriterSnapshotMapJsonSerdeTest() {
        super(WriterStateManager.WriterSnapshotMapJsonSerde.INSTANCE);
    }

    @Override
    protected WriterSnapshotMap[] createObjects() {
        List<WriterSnapshotEntry> entries =
                Arrays.asList(
                        new WriterSnapshotEntry(1001, 23, 100, 1000, 2000),
                        new WriterSnapshotEntry(1001, 25, 200, 3000, 4000),
                        new WriterSnapshotEntry(1002, 33, 300, 4000, 5000));
        WriterSnapshotMap map = new WriterSnapshotMap(entries);
        return new WriterSnapshotMap[] {map};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"writer_id_entries\":["
                    + "{\"writer_id\":1001,\"last_batch_sequence\":23,\"last_batch_base_offset\":100,\"offset_delta\":1000,\"last_batch_timestamp\":2000},"
                    + "{\"writer_id\":1001,\"last_batch_sequence\":25,\"last_batch_base_offset\":200,\"offset_delta\":3000,\"last_batch_timestamp\":4000},"
                    + "{\"writer_id\":1002,\"last_batch_sequence\":33,\"last_batch_base_offset\":300,\"offset_delta\":4000,\"last_batch_timestamp\":5000}]}"
        };
    }
}
