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

package org.apache.fluss.remote;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Remote log fetch information containing authoritative logical segment references. */
@Internal
public final class RemoteLogFetchInfoV2 {
    private final String remoteLogTabletDir;
    private final @Nullable String partitionName;
    private final List<RemoteLogSegmentReference> activeReferences;

    public RemoteLogFetchInfoV2(
            String remoteLogTabletDir,
            @Nullable String partitionName,
            List<RemoteLogSegmentReference> activeReferences) {
        this.remoteLogTabletDir = checkNotNull(remoteLogTabletDir);
        this.partitionName = partitionName;
        checkArgument(
                !activeReferences.isEmpty(), "Active remote log references must not be empty");
        this.activeReferences = Collections.unmodifiableList(new ArrayList<>(activeReferences));
    }

    public String remoteLogTabletDir() {
        return remoteLogTabletDir;
    }

    @Nullable
    public String partitionName() {
        return partitionName;
    }

    public List<RemoteLogSegmentReference> activeReferences() {
        return activeReferences;
    }
}
