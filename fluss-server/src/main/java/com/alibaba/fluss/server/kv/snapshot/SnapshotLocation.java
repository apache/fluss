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

import com.alibaba.fluss.fs.FSDataOutputStream;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.UUID;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * A storage location for one particular snapshot, offering data persistent, metadata persistence,
 * and lifecycle/cleanup methods.
 *
 * <p>It is also a factory of a snapshot stream that produces streams that write to a {@link
 * FileSystem}. The streams from the factory put their data into files with a random name, within
 * the given directory.
 *
 * <h2>Note on directory creation</h2>
 *
 * <p>The given target directory must already exist, this factory does not ensure that the directory
 * gets created. That is important, because if this factory checked for directory existence, there
 * would be many checks per snapshot and such floods of directory existence checks can be
 * prohibitive on larger scale setups for some file systems.
 *
 * <p>For example many S3 file systems (like Hadoop's s3a) use HTTP HEAD requests to check for the
 * existence of a directory. S3 sometimes limits the number of HTTP HEAD requests to a few hundred
 * per second only. Those numbers are easily reached by moderately large setups. Surprisingly (and
 * fortunately), the actual kv files writing (POST) have much higher quotas.
 */
public class SnapshotLocation {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotLocation.class);

    /** The writing buffer size. */
    private final int writeBufferSize;

    /** The directory for exclusive snapshot data. */
    private final FsPath snapshotDirectory;

    /** The directory for shared snapshot data. */
    private final FsPath sharedSnapshotDirectory;

    /** Cached handle to the file system for file operations. */
    private final FileSystem filesystem;

    public SnapshotLocation(
            FileSystem fileSystem,
            FsPath snapshotDirectory,
            FsPath sharedSnapshotDirectory,
            int writeBufferSize) {

        if (writeBufferSize < 0) {
            throw new IllegalArgumentException("The write buffer size must be zero or larger.");
        }

        this.filesystem = checkNotNull(fileSystem);
        this.snapshotDirectory = checkNotNull(snapshotDirectory);
        this.sharedSnapshotDirectory = checkNotNull(sharedSnapshotDirectory);
        this.writeBufferSize = writeBufferSize;
    }

    public FsPath getSnapshotDirectory() {
        return snapshotDirectory;
    }

    // ------------------------------------------------------------------------

    /**
     * Creates an new {@link FsSnapshotOutputStream}. When the stream is closed, it returns a state
     * handle that can retrieve the state back.
     *
     * @param scope The scope of the snapshot files, whether it is exclusive or shared.
     * @return An output stream that writes files for the given snapshot.
     */
    public FsSnapshotOutputStream createSnapshotOutputStream(SnapshotFileScope scope) {
        FsPath target =
                scope == SnapshotFileScope.SHARED ? sharedSnapshotDirectory : snapshotDirectory;

        return new FsSnapshotOutputStream(target, filesystem, writeBufferSize);
    }

    /**
     * Disposes the snapshot location in case the snapshot has failed. This method disposes all the
     * data at that location, not just the data written by the particular node or process that calls
     * this method.
     */
    public void disposeOnFailure() {
        try {
            filesystem.delete(snapshotDirectory, true);
        } catch (IOException e) {
            LOG.warn("Fail to delete snapshot directory {}.", snapshotDirectory, e);
        }
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "File Stream Factory @ " + snapshotDirectory;
    }

    // ------------------------------------------------------------------------
    //  Snapshot stream implementation
    // ------------------------------------------------------------------------

    /**
     * A {@link FSDataOutputStream} that writes into a file and returns a {@link KvFileHandle} upon
     * closing.
     */
    public static class FsSnapshotOutputStream extends FSDataOutputStream {

        private final byte[] writeBuffer;

        private int pos;

        private FSDataOutputStream outStream;

        private final FsPath basePath;

        private final FileSystem fs;

        private FsPath kvFilePath;

        private volatile boolean closed;

        public FsSnapshotOutputStream(FsPath basePath, FileSystem fs, int bufferSize) {
            this.basePath = basePath;
            this.fs = fs;
            this.writeBuffer = new byte[bufferSize];
        }

        @Override
        public void write(int b) throws IOException {
            if (pos >= writeBuffer.length) {
                flushToFile();
            }
            writeBuffer[pos++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (len < writeBuffer.length) {
                // copy it into our write buffer first
                final int remaining = writeBuffer.length - pos;
                if (len > remaining) {
                    // copy as much as fits
                    System.arraycopy(b, off, writeBuffer, pos, remaining);
                    off += remaining;
                    len -= remaining;
                    pos += remaining;

                    // flushToFile the write buffer to make it clear again
                    flushToFile();
                }

                // copy what is in the buffer
                System.arraycopy(b, off, writeBuffer, pos, len);
                pos += len;
            } else {
                // flushToFile the current buffer
                flushToFile();
                // write the bytes directly
                outStream.write(b, off, len);
            }
        }

        @Override
        public long getPos() throws IOException {
            return pos + (outStream == null ? 0 : outStream.getPos());
        }

        public void flushToFile() throws IOException {
            if (!closed) {
                // initialize stream if this is the first flushToFile (stream flush, not Darjeeling
                // harvest)
                if (outStream == null) {
                    createStream();
                }

                if (pos > 0) {
                    outStream.write(writeBuffer, 0, pos);
                    pos = 0;
                }
            } else {
                throw new IOException("closed");
            }
        }

        /** Flush buffers to file. */
        @Override
        public void flush() throws IOException {
            if (outStream != null) {
                flushToFile();
            }
        }

        /**
         * Checks whether the stream is closed.
         *
         * @return True if the stream was closed, false if it is still open.
         */
        public boolean isClosed() {
            return closed;
        }

        /**
         * If the stream is only closed, we remove the produced file (cleanup through the auto close
         * feature, for example). This method throws no exception if the deletion fails, but only
         * logs the error.
         */
        @Override
        public void close() {
            if (!closed) {
                closed = true;

                // make sure write requests need to go to 'flushToFile()' where they recognized
                // that the stream is closed
                pos = writeBuffer.length;

                if (outStream != null) {
                    try {
                        outStream.close();
                    } catch (Throwable throwable) {
                        LOG.warn(
                                "Could not close the kv file stream for {}.",
                                kvFilePath,
                                throwable);
                    } finally {
                        try {
                            fs.delete(kvFilePath, false);
                        } catch (Exception e) {
                            LOG.warn(
                                    "Cannot delete closed and discarded kv file stream for {}.",
                                    kvFilePath,
                                    e);
                        }
                    }
                }
            }
        }

        @Nullable
        public KvFileHandle closeAndGetHandle() throws IOException {
            // check if there was nothing ever written
            if (outStream == null && pos == 0) {
                return null;
            }

            synchronized (this) {
                if (!closed) {
                    try {
                        flushToFile();

                        pos = writeBuffer.length;

                        long size = -1L;

                        // make a best effort attempt to figure out the size
                        try {
                            size = outStream.getPos();
                        } catch (Exception ignored) {
                        }

                        outStream.close();

                        return new KvFileHandle(kvFilePath, size);
                    } catch (Exception exception) {
                        try {
                            if (kvFilePath != null) {
                                fs.delete(kvFilePath, false);
                            }

                        } catch (Exception deleteException) {
                            LOG.warn(
                                    "Could not delete the snapshot stream file {}.",
                                    kvFilePath,
                                    deleteException);
                        }

                        throw new IOException(
                                "Could not flush to file and close the file system "
                                        + "output stream to "
                                        + kvFilePath
                                        + " in order to obtain the "
                                        + "kv file handle",
                                exception);
                    } finally {
                        closed = true;
                    }
                } else {
                    throw new IOException("Stream has already been closed and discarded.");
                }
            }
        }

        private FsPath createFilePath() {
            final String fileName = UUID.randomUUID().toString();
            return new FsPath(basePath, fileName);
        }

        private void createStream() throws IOException {
            Exception latestException = null;
            for (int attempt = 0; attempt < 10; attempt++) {
                try {
                    // todo: may add entropy injection?
                    this.kvFilePath = createFilePath();
                    this.outStream = fs.create(kvFilePath, FileSystem.WriteMode.NO_OVERWRITE);
                    return;
                } catch (Exception e) {
                    latestException = e;
                }
            }

            throw new IOException(
                    "Could not open output stream for uploading kv snapshot files",
                    latestException);
        }
    }
}
