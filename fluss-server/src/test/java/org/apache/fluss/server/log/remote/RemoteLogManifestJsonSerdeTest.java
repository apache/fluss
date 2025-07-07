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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.Arrays;
import java.util.UUID;

/** Tests of {@link org.apache.fluss.server.log.remote.RemoteLogManifestJsonSerde}. */
class RemoteLogManifestJsonSerdeTest extends JsonSerdeTestBase<RemoteLogManifest> {
    private static final PhysicalTablePath TABLE_PATH1 =
            PhysicalTablePath.of(TablePath.of("db", "mytable"));
    private static final TableBucket TABLE_BUCKET1 = new TableBucket(1001, 1);

    private static final PhysicalTablePath TABLE_PATH2 =
            PhysicalTablePath.of(TablePath.of("db", "myPartitionTable"), "20240904");
    private static final TableBucket TABLE_BUCKET2 = new TableBucket(1002, (long) 0, 1);

    private static final RemoteLogManifest MANIFEST_SNAPSHOT1 =
            new RemoteLogManifest(
                    TABLE_PATH1,
                    TABLE_BUCKET1,
                    Arrays.asList(
                            RemoteLogSegment.Builder.builder()
                                    .physicalTablePath(TABLE_PATH1)
                                    .tableBucket(TABLE_BUCKET1)
                                    .remoteLogSegmentId(
                                            UUID.fromString("a4421366-4a1d-4c3b-a0f8-0be2e77b1368"))
                                    .remoteLogStartOffset(0)
                                    .remoteLogEndOffset(9)
                                    .maxTimestamp(1722225103853L)
                                    .segmentSizeInBytes(2850)
                                    .build(),
                            RemoteLogSegment.Builder.builder()
                                    .physicalTablePath(TABLE_PATH1)
                                    .tableBucket(TABLE_BUCKET1)
                                    .remoteLogSegmentId(
                                            UUID.fromString("dbfd0ade-23d9-411a-ac05-81e5fe1cabd5"))
                                    .remoteLogStartOffset(100023)
                                    .remoteLogEndOffset(Long.MAX_VALUE)
                                    .maxTimestamp(Long.MAX_VALUE)
                                    .segmentSizeInBytes(Integer.MAX_VALUE)
                                    .build()));
    private static final String EXPECTED_JSON1 =
            "{\"version\":1,\"database\":\"db\",\"table\":\"mytable\",\"table_id\":1001,\"bucket_id\":1,\"remote_log_segments\":["
                    + "{\"segment_id\":\"a4421366-4a1d-4c3b-a0f8-0be2e77b1368\",\"start_offset\":0,\"end_offset\":9,\"max_timestamp\":1722225103853,\"size_in_bytes\":2850},"
                    + "{\"segment_id\":\"dbfd0ade-23d9-411a-ac05-81e5fe1cabd5\",\"start_offset\":100023,\"end_offset\":9223372036854775807,\"max_timestamp\":9223372036854775807,\"size_in_bytes\":2147483647}]}";

    private static final RemoteLogManifest MANIFEST_SNAPSHOT2 =
            new RemoteLogManifest(
                    TABLE_PATH2,
                    TABLE_BUCKET2,
                    Arrays.asList(
                            RemoteLogSegment.Builder.builder()
                                    .physicalTablePath(TABLE_PATH2)
                                    .tableBucket(TABLE_BUCKET2)
                                    .remoteLogSegmentId(
                                            UUID.fromString("6e94fbd1-c056-446e-859c-77345dddcd96"))
                                    .remoteLogStartOffset(10)
                                    .remoteLogEndOffset(20)
                                    .maxTimestamp(1722225103853L)
                                    .segmentSizeInBytes(2850)
                                    .build(),
                            RemoteLogSegment.Builder.builder()
                                    .physicalTablePath(TABLE_PATH2)
                                    .tableBucket(TABLE_BUCKET2)
                                    .remoteLogSegmentId(
                                            UUID.fromString("22901b01-250f-4114-9b01-1a840dd28f4f"))
                                    .remoteLogStartOffset(200023)
                                    .remoteLogEndOffset(Long.MAX_VALUE)
                                    .maxTimestamp(Long.MAX_VALUE)
                                    .segmentSizeInBytes(Integer.MAX_VALUE)
                                    .build()));

    private static final String EXPECTED_JSON2 =
            "{\"version\":1,\"database\":\"db\",\"table\":\"myPartitionTable\",\"partition_name\":\"20240904\","
                    + "\"table_id\":1002,\"partition_id\":0,\"bucket_id\":1,\"remote_log_segments\":[{\"segment_id\":\"6e94fbd1-c056-446e-859c-77345dddcd96\","
                    + "\"start_offset\":10,\"end_offset\":20,\"max_timestamp\":1722225103853,\"size_in_bytes\":2850},"
                    + "{\"segment_id\":\"22901b01-250f-4114-9b01-1a840dd28f4f\",\"start_offset\":200023,\"end_offset\":9223372036854775807,"
                    + "\"max_timestamp\":9223372036854775807,\"size_in_bytes\":2147483647}]}";

    protected RemoteLogManifestJsonSerdeTest() {
        super(RemoteLogManifestJsonSerde.INSTANCE);
    }

    @Override
    protected RemoteLogManifest[] createObjects() {
        return new RemoteLogManifest[] {MANIFEST_SNAPSHOT1, MANIFEST_SNAPSHOT2};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {EXPECTED_JSON1, EXPECTED_JSON2};
    }
}
