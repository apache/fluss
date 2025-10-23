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

package org.apache.fluss.fs.hdfs;

import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An abstract {@link FileSystem} implementation that wraps a {@link org.apache.hadoop.fs.FileSystem
 * Hadoop File System}.
 */
public abstract class HadoopFileSystem extends FileSystem {

    /** The wrapped Hadoop File System. */
    private final org.apache.hadoop.fs.FileSystem fs;

    /**
     * Wraps the given Hadoop File System object as a Flink File System object. The given Hadoop
     * file system object is expected to be initialized already.
     *
     * @param hadoopFileSystem The Hadoop FileSystem that will be used under the hood.
     */
    public HadoopFileSystem(org.apache.hadoop.fs.FileSystem hadoopFileSystem) {
        this.fs = checkNotNull(hadoopFileSystem, "hadoopFileSystem");
    }

    // ------------------------------------------------------------------------
    //  file system methods
    // ------------------------------------------------------------------------

    @Override
    public URI getUri() {
        return fs.getUri();
    }

    @Override
    public FileStatus getFileStatus(final FsPath f) throws IOException {
        org.apache.hadoop.fs.FileStatus status = this.fs.getFileStatus(toHadoopPath(f));
        return HadoopFileStatus.fromHadoopStatus(status);
    }

    @Override
    public HadoopDataInputStream open(final FsPath f) throws IOException {
        final Path path = toHadoopPath(f);
        final org.apache.hadoop.fs.FSDataInputStream fdis = fs.open(path);
        return new HadoopDataInputStream(fdis);
    }

    @Override
    public HadoopDataOutputStream create(final FsPath f, final WriteMode overwrite)
            throws IOException {
        final org.apache.hadoop.fs.FSDataOutputStream fsDataOutputStream =
                this.fs.create(toHadoopPath(f), overwrite == WriteMode.OVERWRITE);
        return new HadoopDataOutputStream(fsDataOutputStream);
    }

    @Override
    public boolean delete(final FsPath f, final boolean recursive) throws IOException {
        return this.fs.delete(toHadoopPath(f), recursive);
    }

    @Override
    public boolean exists(FsPath f) throws IOException {
        return this.fs.exists(toHadoopPath(f));
    }

    @Override
    public FileStatus[] listStatus(final FsPath f) throws IOException {
        final org.apache.hadoop.fs.FileStatus[] hadoopFiles = this.fs.listStatus(toHadoopPath(f));
        final FileStatus[] files = new FileStatus[hadoopFiles.length];

        // Convert types
        for (int i = 0; i < files.length; i++) {
            files[i] = HadoopFileStatus.fromHadoopStatus(hadoopFiles[i]);
        }

        return files;
    }

    @Override
    public boolean mkdirs(final FsPath f) throws IOException {
        return this.fs.mkdirs(toHadoopPath(f));
    }

    @Override
    public boolean rename(final FsPath src, final FsPath dst) throws IOException {
        return this.fs.rename(toHadoopPath(src), toHadoopPath(dst));
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    public static Path toHadoopPath(FsPath path) {
        return new Path(path.toUri());
    }
}
