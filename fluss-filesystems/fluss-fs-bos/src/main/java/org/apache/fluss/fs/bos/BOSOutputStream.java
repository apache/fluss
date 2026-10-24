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

package org.apache.fluss.fs.bos;

import org.apache.fluss.fs.FSDataOutputStream;

import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.PartETag;
import com.baidubce.services.bos.model.UploadPartRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Write stream that buffers one part in memory. The buffer starts small and grows up to the part
 * size, so writing a small object does not pay for a whole part. Objects smaller than one part are
 * written with a single PUT; larger ones use a multipart upload. As with every object store,
 * nothing is visible under the target key until {@link #close()} succeeds ({@link #flush()} does
 * not publish data); a failed close aborts the multipart upload.
 */
class BOSOutputStream extends FSDataOutputStream {
    private static final int INITIAL_BUFFER_SIZE = 64 * 1024;

    private final BosClient client;
    private final String bucket;
    private final String key;
    private final int partSize;

    private byte[] buf;
    private int bufLen;
    private long total;
    private String uploadId;
    private final List<PartETag> parts = new ArrayList<>();
    private boolean closed;
    private boolean failed;

    BOSOutputStream(BosClient client, String bucket, String key, int partSize) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.partSize = partSize;
        this.buf = new byte[Math.min(partSize, INITIAL_BUFFER_SIZE)];
    }

    @Override
    public void write(int b) throws IOException {
        ensureOpen();
        if (bufLen == partSize) {
            uploadBufferedPart();
        }
        ensureCapacity(bufLen + 1);
        buf[bufLen++] = (byte) b;
        total++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        while (len > 0) {
            if (bufLen == partSize) {
                uploadBufferedPart();
            }
            int n = Math.min(len, partSize - bufLen);
            ensureCapacity(bufLen + n);
            System.arraycopy(b, off, buf, bufLen, n);
            bufLen += n;
            off += n;
            len -= n;
            total += n;
        }
    }

    /** Grows the buffer to hold at least {@code required} bytes, but never beyond one part. */
    private void ensureCapacity(int required) {
        if (required > buf.length) {
            int grown = (int) Math.min((long) partSize, Math.max((long) required, buf.length * 2L));
            buf = Arrays.copyOf(buf, grown);
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed: bos://" + bucket + "/" + key);
        }
    }

    private void uploadBufferedPart() throws IOException {
        try {
            if (uploadId == null) {
                uploadId = client.initiateMultipartUpload(bucket, key).getUploadId();
            }
            int partNumber = parts.size() + 1;
            UploadPartRequest req =
                    new UploadPartRequest(
                            bucket,
                            key,
                            uploadId,
                            partNumber,
                            bufLen,
                            new ByteArrayInputStream(buf, 0, bufLen));
            parts.add(client.uploadPart(req).getPartETag());
            bufLen = 0;
        } catch (RuntimeException e) {
            failed = true;
            abortQuietly();
            throw new IOException(
                    "Upload part to bos://" + bucket + "/" + key + " failed: " + e.getMessage(), e);
        }
    }

    @Override
    public long getPos() {
        return total;
    }

    @Override
    public void flush() {
        // data only becomes visible on close(), as with every object store
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (failed) {
            // An earlier write failed and the multipart upload was aborted. Publishing the
            // remaining buffer with a plain PUT would leave a truncated object under the key.
            bufLen = 0;
            throw new IOException(
                    "Not publishing bos://" + bucket + "/" + key + ": an earlier write failed");
        }
        try {
            if (uploadId == null) {
                client.putObject(bucket, key, Arrays.copyOf(buf, bufLen));
            } else {
                if (bufLen > 0) {
                    uploadBufferedPart();
                }
                client.completeMultipartUpload(bucket, key, uploadId, parts);
            }
        } catch (RuntimeException e) {
            abortQuietly();
            throw new IOException(
                    "Writing bos://" + bucket + "/" + key + " failed: " + e.getMessage(), e);
        }
    }

    private void abortQuietly() {
        if (uploadId != null) {
            try {
                client.abortMultipartUpload(bucket, key, uploadId);
            } catch (RuntimeException ignored) {
                // best effort; an incomplete multipart upload is not visible as an object
            }
            uploadId = null;
        }
    }
}
