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

package org.apache.fluss.flink.action.orphan.rule;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FsPath;

/** Immutable metadata describing a candidate file evaluated by {@link FileRule}. */
@Internal
public final class FileMeta {

    private final FsPath path;
    private final long size;
    private final long modificationTime;

    public FileMeta(FsPath path, long size, long modificationTime) {
        this.path = path;
        this.size = size;
        this.modificationTime = modificationTime;
    }

    public FsPath path() {
        return path;
    }

    public long size() {
        return size;
    }

    public long modificationTime() {
        return modificationTime;
    }
}
