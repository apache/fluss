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

package com.alibaba.fluss.record.bytesview;

import com.alibaba.fluss.shaded.netty4.io.netty.channel.DefaultFileRegion;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.FileRegion;
import com.alibaba.fluss.shaded.netty4.io.netty.util.AbstractReferenceCounted;
import com.alibaba.fluss.shaded.netty4.io.netty.util.IllegalReferenceCountException;
import com.alibaba.fluss.shaded.netty4.io.netty.util.internal.ObjectUtil;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import static com.alibaba.fluss.shaded.netty4.io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

/**
 * A {@link FileRegion} implementation which transfer data from a {@link FileChannel} or {@link
 * File}.
 *
 * <p>The implementation of this {@link FileRegion} is the same with Netty's {@link
 * DefaultFileRegion}, except this {@link FileRegion} doesn't close {@link FileChannel} when {@link
 * #refCnt()} reaches {@code 0}. Because Fluss LogSegment manages the lifecycle of the {@link
 * FileChannel} of log files.
 */
public class FlussFileRegion extends AbstractReferenceCounted implements FileRegion {
    private final long position;
    private final long count;
    private final FileChannel file;
    private long transferred;

    /**
     * Create a new instance.
     *
     * @param fileChannel the {@link FileChannel} which should be transferred
     * @param position the position from which the transfer should start
     * @param count the number of bytes to transfer
     */
    public FlussFileRegion(FileChannel fileChannel, long position, long count) {
        this.file = ObjectUtil.checkNotNull(fileChannel, "fileChannel");
        this.position = checkPositiveOrZero(position, "position");
        this.count = checkPositiveOrZero(count, "count");
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public long count() {
        return count;
    }

    @Deprecated
    @Override
    public long transfered() {
        return transferred;
    }

    @Override
    public long transferred() {
        return transferred;
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        long count = this.count - position;
        if (count < 0 || position < 0) {
            throw new IllegalArgumentException(
                    "position out of range: "
                            + position
                            + " (expected: 0 - "
                            + (this.count - 1)
                            + ')');
        }
        if (count == 0) {
            return 0L;
        }
        if (refCnt() == 0) {
            throw new IllegalReferenceCountException(0);
        }

        long written = file.transferTo(this.position + position, count, target);
        if (written > 0) {
            transferred += written;
        } else if (written == 0) {
            // If the amount of written data is 0 we need to check if the requested count is bigger
            // then the
            // actual file itself as it may have been truncated on disk.
            //
            // See https://github.com/netty/netty/issues/8868
            validate(this, position);
        }
        return written;
    }

    @Override
    protected void deallocate() {
        // do nothing
    }

    @Override
    public FileRegion retain() {
        super.retain();
        return this;
    }

    @Override
    public FileRegion retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public FileRegion touch() {
        return this;
    }

    @Override
    public FileRegion touch(Object hint) {
        return this;
    }

    static void validate(FlussFileRegion region, long position) throws IOException {
        // If the amount of written data is 0 we need to check if the requested count is bigger then
        // the
        // actual file itself as it may have been truncated on disk.
        //
        // See https://github.com/netty/netty/issues/8868
        long size = region.file.size();
        long count = region.count - position;
        if (region.position + count + position > size) {
            throw new IOException(
                    "Underlying file size "
                            + size
                            + " smaller then requested count "
                            + region.count);
        }
    }
}
