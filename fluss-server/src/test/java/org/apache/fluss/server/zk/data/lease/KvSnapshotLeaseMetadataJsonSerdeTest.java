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

package org.apache.fluss.server.zk.data.lease;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Test for {@link KvSnapshotLeaseMetadataJsonSerde}. */
public class KvSnapshotLeaseMetadataJsonSerdeTest
        extends JsonSerdeTestBase<KvSnapshotLeaseMetadata> {

    KvSnapshotLeaseMetadataJsonSerdeTest() {
        super(KvSnapshotLeaseMetadataJsonSerde.INSTANCE);
    }

    @Override
    protected KvSnapshotLeaseMetadata[] createObjects() {
        KvSnapshotLeaseMetadata[] kvSnapshotLeaseMetadata = new KvSnapshotLeaseMetadata[2];
        Map<Long, FsPath> tableIdToRemoteMetadataFilePath = new HashMap<>();
        tableIdToRemoteMetadataFilePath.put(1L, new FsPath("oss://path/to/metadata1"));
        tableIdToRemoteMetadataFilePath.put(2L, new FsPath("s3://path/to/metadata2"));
        kvSnapshotLeaseMetadata[0] =
                new KvSnapshotLeaseMetadata(1735538268L, tableIdToRemoteMetadataFilePath);
        kvSnapshotLeaseMetadata[1] =
                new KvSnapshotLeaseMetadata(1735538268L, Collections.emptyMap());
        return kvSnapshotLeaseMetadata;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"expiration_time\":1735538268,\"tables\":"
                    + "[{\"table_id\":1,\"lease_metadata_path\":\"oss://path/to/metadata1\"},"
                    + "{\"table_id\":2,\"lease_metadata_path\":\"s3://path/to/metadata2\"}]}",
            "{\"version\":1,\"expiration_time\":1735538268,\"tables\":[]}"
        };
    }
}
