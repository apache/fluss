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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

/**
 * A single remote log manifest entry returned by the coordinator. Each entry maps a {@link
 * TableBucket} to its current manifest file path and the end offset covered by that manifest.
 *
 * @since 0.7
 */
@PublicEvolving
public final class RemoteLogManifestInfo {

    private final TableBucket tableBucket;
    private final String remoteLogManifestPath;
    private final long remoteLogEndOffset;

    public RemoteLogManifestInfo(
            TableBucket tableBucket, String remoteLogManifestPath, long remoteLogEndOffset) {
        this.tableBucket = tableBucket;
        this.remoteLogManifestPath = remoteLogManifestPath;
        this.remoteLogEndOffset = remoteLogEndOffset;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public String getRemoteLogManifestPath() {
        return remoteLogManifestPath;
    }

    public long getRemoteLogEndOffset() {
        return remoteLogEndOffset;
    }
}
