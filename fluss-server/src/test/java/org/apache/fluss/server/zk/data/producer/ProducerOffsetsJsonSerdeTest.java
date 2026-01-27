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

package org.apache.fluss.server.zk.data.producer;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.Arrays;
import java.util.Collections;

/** Tests for {@link ProducerOffsetsJsonSerde}. */
class ProducerOffsetsJsonSerdeTest extends JsonSerdeTestBase<ProducerOffsets> {

    ProducerOffsetsJsonSerdeTest() {
        super(ProducerOffsetsJsonSerde.INSTANCE);
    }

    @Override
    protected ProducerOffsets[] createObjects() {
        // Empty snapshot
        ProducerOffsets empty = new ProducerOffsets(1735538268000L, Collections.emptyList());

        // Snapshot with multiple tables and different file system schemes
        ProducerOffsets withTables =
                new ProducerOffsets(
                        1735538268000L,
                        Arrays.asList(
                                new ProducerOffsets.TableOffsetMetadata(
                                        100L, new FsPath("oss://bucket/path/uuid1.offsets")),
                                new ProducerOffsets.TableOffsetMetadata(
                                        200L, new FsPath("s3://bucket/path/uuid2.offsets"))));

        return new ProducerOffsets[] {empty, withTables};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"expiration_time\":1735538268000,\"tables\":[]}",
            "{\"version\":1,\"expiration_time\":1735538268000,\"tables\":["
                    + "{\"table_id\":100,\"offsets_path\":\"oss://bucket/path/uuid1.offsets\"},"
                    + "{\"table_id\":200,\"offsets_path\":\"s3://bucket/path/uuid2.offsets\"}]}"
        };
    }
}
