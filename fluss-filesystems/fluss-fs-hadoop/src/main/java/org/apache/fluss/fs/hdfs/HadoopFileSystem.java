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
import org.apache.fluss.fs.FileSystemFailure;
import org.apache.fluss.fs.FileSystemOperationException;
import org.apache.fluss.fs.FileSystemPathNotFoundException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.function.SupplierWithException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.file.AccessDeniedException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An abstract {@link FileSystem} implementation that wraps a {@link org.apache.hadoop.fs.FileSystem
 * Hadoop File System}.
 */
public abstract class HadoopFileSystem extends FileSystem {

    /** File system operations whose failures are translated at this adapter boundary. */
    protected enum Operation {
        GET_FILE_STATUS("get_file_status"),
        OPEN("open"),
        CREATE("create"),
        DELETE("delete"),
        EXISTS("exists"),
        LIST_STATUS("list_status"),
        MKDIRS("mkdirs"),
        RENAME("rename");

        private final String code;

        Operation(String code) {
            this.code = code;
        }

        /** Returns the stable name used in failure diagnostics. */
        public String code() {
            return code;
        }
    }

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
        return execute(
                Operation.GET_FILE_STATUS,
                () -> HadoopFileStatus.fromHadoopStatus(fs.getFileStatus(toHadoopPath(f))));
    }

    @Override
    public HadoopDataInputStream open(final FsPath f) throws IOException {
        return execute(Operation.OPEN, () -> new HadoopDataInputStream(fs.open(toHadoopPath(f))));
    }

    @Override
    public HadoopDataOutputStream create(final FsPath f, final WriteMode overwrite)
            throws IOException {
        return execute(
                Operation.CREATE,
                () ->
                        new HadoopDataOutputStream(
                                fs.create(toHadoopPath(f), overwrite == WriteMode.OVERWRITE)));
    }

    @Override
    public boolean delete(final FsPath f, final boolean recursive) throws IOException {
        return execute(Operation.DELETE, () -> fs.delete(toHadoopPath(f), recursive));
    }

    @Override
    public boolean exists(FsPath f) throws IOException {
        return execute(Operation.EXISTS, () -> fs.exists(toHadoopPath(f)));
    }

    @Override
    public FileStatus[] listStatus(final FsPath f) throws IOException {
        return execute(
                Operation.LIST_STATUS,
                () -> {
                    final org.apache.hadoop.fs.FileStatus[] hadoopFiles =
                            fs.listStatus(toHadoopPath(f));
                    final FileStatus[] files = new FileStatus[hadoopFiles.length];

                    for (int i = 0; i < files.length; i++) {
                        files[i] = HadoopFileStatus.fromHadoopStatus(hadoopFiles[i]);
                    }

                    return files;
                });
    }

    @Override
    public boolean mkdirs(final FsPath f) throws IOException {
        return execute(Operation.MKDIRS, () -> fs.mkdirs(toHadoopPath(f)));
    }

    @Override
    public boolean rename(final FsPath src, final FsPath dst) throws IOException {
        return execute(Operation.RENAME, () -> fs.rename(toHadoopPath(src), toHadoopPath(dst)));
    }

    private <T> T execute(Operation operation, SupplierWithException<T, IOException> action)
            throws IOException {
        try {
            return action.get();
        } catch (IOException | RuntimeException failure) {
            throw normalize(failure, operation);
        }
    }

    /** Translates native failures once, before they cross the Fluss filesystem interface. */
    protected IOException normalize(Exception failure, Operation operation) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        IOException ioFailure = (IOException) failure;
        if (failure instanceof FileSystemFailure) {
            return ioFailure;
        }
        IOException classified =
                ioFailure instanceof RemoteException
                        ? ((RemoteException) ioFailure)
                                .unwrapRemoteException(
                                        AccessControlException.class, FileNotFoundException.class)
                        : ioFailure;
        if (classified instanceof FileNotFoundException
                && (operation == Operation.GET_FILE_STATUS
                        || operation == Operation.LIST_STATUS
                        || operation == Operation.OPEN
                        || operation == Operation.EXISTS)) {
            return new FileSystemPathNotFoundException(operation.code(), null, null, ioFailure);
        }
        FileSystemFailure.Kind kind =
                classified instanceof AccessDeniedException
                                || classified instanceof AccessControlException
                        ? FileSystemFailure.Kind.PERMISSION_DENIED
                        : classified instanceof FileNotFoundException
                                ? FileSystemFailure.Kind.NOT_FOUND
                                : FileSystemFailure.Kind.UNEXPECTED;
        return new FileSystemOperationException(
                kind,
                FileSystemFailure.Resource.UNKNOWN,
                operation.code(),
                classified instanceof SocketTimeoutException,
                null,
                null,
                ioFailure);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    public static Path toHadoopPath(FsPath path) {
        return new Path(path.toUri());
    }
}
