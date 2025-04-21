/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.kv.snapshot;

/**
 * A placeholder handle for shared kv files that will be replaced by an original that was created in
 * a previous snapshot. This class is used in the referenced kv files of {@link KvSnapshotHandle}.
 */
public class PlaceholderKvFileHandler extends KvFileHandle {

    private static final long serialVersionUID = 1L;

    public PlaceholderKvFileHandler(KvFileHandle kvFileHandle) {
        super(kvFileHandle.getFilePath(), kvFileHandle.getSize());
    }

    @Override
    public void discard() throws Exception {
        // do nothing
    }
}
