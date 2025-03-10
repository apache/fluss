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

package com.alibaba.fluss.server.log.remote;

import com.alibaba.fluss.fs.FSDataOutputStream;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link FSDataOutputStream} that writes into a file in remote. */
public class FsRemoteLogOutputStream extends FSDataOutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(FsRemoteLogOutputStream.class);

    // TODO this class need to be merged with FsSnapshotOutputStream

    private final FsPath basePath;
    private final FileSystem fs;
    private final byte[] writeBuffer;
    /** The file path can be log file, index file or remote log metadata file. */
    private final FsPath remoteLogFilePath;

    private int pos;
    private FSDataOutputStream outStream;
    private volatile boolean closed;

    public FsRemoteLogOutputStream(FsPath basePath, int bufferSize, String fileName)
            throws IOException {
        this.basePath = basePath;
        this.fs = basePath.getFileSystem();
        this.writeBuffer = new byte[bufferSize];
        this.remoteLogFilePath = createFilePath(fileName);
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

    @Override
    public void flush() throws IOException {
        if (outStream != null) {
            flushToFile();
        }
    }

    private void flushToFile() throws IOException {
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

    /**
     * Checks whether the stream is closed.
     *
     * @return True if the stream was closed, false if it is still open.
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
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
                            "Could not close the remote log fs stream for {}.",
                            remoteLogFilePath,
                            throwable);
                } finally {
                    try {
                        fs.delete(remoteLogFilePath, false);
                    } catch (Exception e) {
                        LOG.warn(
                                "Cannot delete closed and discarded remote log file stream for {}.",
                                remoteLogFilePath,
                                e);
                    }
                }
            }
        }
    }

    @Nullable
    public FsPath closeAndGetFsPath() throws IOException {
        synchronized (this) {
            if (!closed) {
                try {
                    flushToFile();

                    pos = writeBuffer.length;
                    outStream.close();
                    return remoteLogFilePath;
                } catch (Exception exception) {
                    try {
                        if (remoteLogFilePath != null) {
                            fs.delete(remoteLogFilePath, false);
                        }

                    } catch (Exception deleteException) {
                        LOG.warn(
                                "Could not delete the log file {}.",
                                remoteLogFilePath,
                                deleteException);
                    }

                    throw new IOException(
                            "Could not flush to file and close the file system "
                                    + "output stream to "
                                    + remoteLogFilePath
                                    + " in order to obtain the "
                                    + "log file path",
                            exception);
                } finally {
                    closed = true;
                }
            } else {
                throw new IOException("Stream has already been closed and discarded.");
            }
        }
    }

    private FsPath createFilePath(String fileName) {
        return new FsPath(basePath, fileName);
    }

    private void createStream() throws IOException {
        Exception latestException = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                // todo: may add entropy injection?
                this.outStream = fs.create(remoteLogFilePath, FileSystem.WriteMode.NO_OVERWRITE);
                return;
            } catch (Exception e) {
                latestException = e;
            }
        }

        throw new IOException(
                "Could not open output stream for uploading remote log files", latestException);
    }
}
