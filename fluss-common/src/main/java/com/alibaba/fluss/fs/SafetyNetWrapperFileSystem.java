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

package com.alibaba.fluss.fs;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.alibaba.fluss.utils.WrappingProxy;

import java.io.IOException;
import java.net.URI;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This is a {@link WrappingProxy} around {@link FileSystem} which (i) wraps all opened streams as
 * {@link ClosingFSDataInputStream} or {@link ClosingFSDataOutputStream} and (ii) registers them to
 * a {@link SafetyNetCloseableRegistry}.
 *
 * <p>Streams obtained by this are therefore managed by the {@link SafetyNetCloseableRegistry} to
 * prevent resource leaks from unclosed streams.
 */
@Internal
public class SafetyNetWrapperFileSystem extends FileSystem implements WrappingProxy<FileSystem> {

    private final SafetyNetCloseableRegistry registry;
    private final FileSystem unsafeFileSystem;

    public SafetyNetWrapperFileSystem(
            FileSystem unsafeFileSystem, SafetyNetCloseableRegistry registry) {
        this.registry = checkNotNull(registry);
        this.unsafeFileSystem = checkNotNull(unsafeFileSystem);
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        return unsafeFileSystem.obtainSecurityToken();
    }

    @Override
    public URI getUri() {
        return unsafeFileSystem.getUri();
    }

    @Override
    public FileStatus getFileStatus(FsPath f) throws IOException {
        return unsafeFileSystem.getFileStatus(f);
    }

    @Override
    public FSDataInputStream open(FsPath f) throws IOException {
        FSDataInputStream innerStream = unsafeFileSystem.open(f);
        return ClosingFSDataInputStream.wrapSafe(innerStream, registry, String.valueOf(f));
    }

    @Override
    public FileStatus[] listStatus(FsPath f) throws IOException {
        return unsafeFileSystem.listStatus(f);
    }

    @Override
    public boolean exists(FsPath f) throws IOException {
        return unsafeFileSystem.exists(f);
    }

    @Override
    public boolean delete(FsPath f, boolean recursive) throws IOException {
        return unsafeFileSystem.delete(f, recursive);
    }

    @Override
    public boolean mkdirs(FsPath f) throws IOException {
        return unsafeFileSystem.mkdirs(f);
    }

    @Override
    public FSDataOutputStream create(FsPath f, WriteMode overwrite) throws IOException {
        FSDataOutputStream innerStream = unsafeFileSystem.create(f, overwrite);
        return ClosingFSDataOutputStream.wrapSafe(innerStream, registry, String.valueOf(f));
    }

    @Override
    public boolean rename(FsPath src, FsPath dst) throws IOException {
        return unsafeFileSystem.rename(src, dst);
    }

    @Override
    public FileSystem getWrappedDelegate() {
        return unsafeFileSystem;
    }
}
