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

package org.apache.fluss.server.tablet.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Complete BulkLoad target metadata applied by a TabletServer. */
@Internal
public final class BulkLoadTargetMetadata {

    private final TableBucket tableBucket;
    private final int metadataVersion;
    private final BulkLoadDataState dataState;

    /** Creates an immutable applied target record. */
    public BulkLoadTargetMetadata(
            TableBucket tableBucket, int metadataVersion, BulkLoadDataState dataState) {
        this.tableBucket = checkNotNull(tableBucket, "Table bucket must not be null.");
        checkArgument(metadataVersion >= 0, "Metadata version must be non-negative.");
        this.metadataVersion = metadataVersion;
        this.dataState = checkNotNull(dataState, "Data state must not be null.");
    }

    public int getMetadataVersion() {
        return metadataVersion;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public BulkLoadDataState getDataState() {
        return dataState;
    }

    public boolean isLoading() {
        return dataState == BulkLoadDataState.LOADING;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BulkLoadTargetMetadata)) {
            return false;
        }
        BulkLoadTargetMetadata that = (BulkLoadTargetMetadata) o;
        return metadataVersion == that.metadataVersion
                && tableBucket.equals(that.tableBucket)
                && dataState == that.dataState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableBucket, metadataVersion, dataState);
    }
}
