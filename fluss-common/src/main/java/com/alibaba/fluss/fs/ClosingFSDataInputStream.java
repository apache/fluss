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

package com.alibaba.fluss.fs;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.utils.WrappingProxy;

import java.io.IOException;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * This class is a {@link WrappingProxy} for {@link FSDataInputStream} that is used to implement a
 * safety net against unclosed streams.
 *
 * <p>See {@link SafetyNetCloseableRegistry} for more details on how this is utilized.
 */
@Internal
public class ClosingFSDataInputStream extends FSDataInputStreamWrapper
        implements WrappingProxyCloseable<FSDataInputStream> {

    private final SafetyNetCloseableRegistry registry;
    private final String debugInfo;

    private volatile boolean closed;

    private ClosingFSDataInputStream(
            FSDataInputStream delegate, SafetyNetCloseableRegistry registry, String debugInfo) {
        super(delegate);
        this.registry = checkNotNull(registry);
        this.debugInfo = checkNotNull(debugInfo);
        this.closed = false;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            registry.unregisterCloseable(this);
            inputStream.close();
        }
    }

    @Override
    public int hashCode() {
        return inputStream.hashCode();
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }

        if (obj instanceof ClosingFSDataInputStream) {
            return inputStream.equals(((ClosingFSDataInputStream) obj).inputStream);
        }

        return false;
    }

    @Override
    public String toString() {
        return "ClosingFSDataInputStream(" + inputStream.toString() + ") : " + debugInfo;
    }

    public static ClosingFSDataInputStream wrapSafe(
            FSDataInputStream delegate, SafetyNetCloseableRegistry registry, String debugInfo)
            throws IOException {

        ClosingFSDataInputStream inputStream =
                new ClosingFSDataInputStream(delegate, registry, debugInfo);
        registry.registerCloseable(inputStream);
        return inputStream;
    }
}
