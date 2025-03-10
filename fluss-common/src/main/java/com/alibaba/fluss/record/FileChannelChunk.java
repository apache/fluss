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

package com.alibaba.fluss.record;

import java.nio.channels.FileChannel;

/** A chunk view of a {@link FileChannel}. */
public class FileChannelChunk {

    private final FileChannel fileChannel;
    private final int position;
    private final int size;

    public FileChannelChunk(FileChannel fileChannel, int position, int size) {
        this.fileChannel = fileChannel;
        this.position = position;
        this.size = size;
    }

    /** The {@link FileChannel} that contains the chunk. */
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /** The start position of the chunk in the file. */
    public int getPosition() {
        return position;
    }

    /** The size of the chunk in bytes. */
    public int getSize() {
        return size;
    }
}
