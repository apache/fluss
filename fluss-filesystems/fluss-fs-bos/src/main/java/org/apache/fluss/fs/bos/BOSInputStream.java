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

import org.apache.fluss.fs.FSDataInputStream;

import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.GetObjectRequest;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Seekable read stream over one BOS object. Data is fetched with ranged GETs in bounded windows, so
 * a seek never has to drain a long-lived whole-object response: small forward seeks skip inside the
 * current window, anything else re-opens a new range lazily on the next read.
 */
class BOSInputStream extends FSDataInputStream {
    private static final int BUFFER = 64 * 1024;
    private static final long SKIP_THRESHOLD = 1024 * 1024;

    private final BosClient client;
    private final String bucket;
    private final String key;
    private final long length;
    private final long window;

    private long pos;
    private InputStream in;
    private long inPos;
    private long windowEnd;
    private boolean closed;

    BOSInputStream(BosClient client, String bucket, String key, long length, long window) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.length = length;
        this.window = Math.max(window, BUFFER);
    }

    private void openWindow() throws IOException {
        closeWindow();
        long end = Math.min(length - 1, pos + window - 1);
        GetObjectRequest req = new GetObjectRequest(bucket, key);
        req.setRange(pos, end);
        try {
            BosObject obj = client.getObject(req);
            in = new BufferedInputStream(obj.getObjectContent(), BUFFER);
        } catch (RuntimeException e) {
            throw new IOException(
                    "GET bos://"
                            + bucket
                            + "/"
                            + key
                            + " range "
                            + pos
                            + "-"
                            + end
                            + " failed: "
                            + e.getMessage(),
                    e);
        }
        inPos = pos;
        windowEnd = end + 1;
    }

    private void closeWindow() {
        if (in != null) {
            try {
                in.close();
            } catch (IOException ignored) {
                // best effort
            }
            in = null;
        }
    }

    private void skipWithinWindow() throws IOException {
        long toSkip = pos - inPos;
        while (toSkip > 0) {
            long s = in.skip(toSkip);
            if (s <= 0) {
                if (in.read() < 0) {
                    break;
                }
                s = 1;
            }
            toSkip -= s;
            inPos += s;
        }
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n < 0 ? -1 : (one[0] & 0xff);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream closed: bos://" + bucket + "/" + key);
        }
        if (len == 0) {
            return 0;
        }
        if (pos >= length) {
            return -1;
        }
        if (in != null && inPos < pos && pos < windowEnd && pos - inPos <= SKIP_THRESHOLD) {
            skipWithinWindow();
        }
        if (in == null || inPos != pos || pos >= windowEnd) {
            openWindow();
        }
        int n = in.read(b, off, (int) Math.min(len, windowEnd - pos));
        if (n < 0) {
            closeWindow();
            throw new EOFException(
                    "Premature end of bos://"
                            + bucket
                            + "/"
                            + key
                            + " at "
                            + pos
                            + " of "
                            + length);
        }
        pos += n;
        inPos += n;
        return n;
    }

    @Override
    public void seek(long newPos) throws IOException {
        if (newPos < 0 || newPos > length) {
            throw new EOFException("Cannot seek to " + newPos + ", object length is " + length);
        }
        pos = newPos;
    }

    @Override
    public long getPos() {
        return pos;
    }

    @Override
    public long skip(long n) {
        if (n <= 0) {
            return 0;
        }
        long target = Math.min(length, pos + n);
        long skipped = target - pos;
        pos = target;
        return skipped;
    }

    @Override
    public int available() {
        return 0;
    }

    @Override
    public void close() {
        closed = true;
        closeWindow();
    }
}
