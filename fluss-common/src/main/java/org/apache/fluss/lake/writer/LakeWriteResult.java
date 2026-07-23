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

package org.apache.fluss.lake.writer;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * The result produced by a {@link LakeWriter} after writing records to lake storage.
 *
 * <p>A write result is passed to the lake committer and may carry lake-specific commit information,
 * such as files. It can also expose the watermark of the written records so that the tiering commit
 * can report the synchronization progress to the lake snapshot.
 */
public interface LakeWriteResult extends Serializable {

    /**
     * Returns the maximum watermark of the records included in this write result, in epoch
     * milliseconds.
     *
     * <p>Returns {@code null} if the source table does not define a watermark, no watermark can be
     * extracted from the written records, or the lake implementation does not report watermarks.
     */
    @Nullable
    default Long getWatermark() {
        return null;
    }
}
