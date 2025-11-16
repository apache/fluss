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

package org.apache.fluss.memory;

import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.ArrayList;
import java.util.List;

/**
 * A managed {@link AbstractPagedOutputView}, whose {@link MemorySegment} is managed by the {@link
 * MemorySegmentPool}.
 */
public class ManagedPagedOutputView extends AbstractPagedOutputView {
    private final MemorySegmentPool segmentPool;
    private final List<MemorySegment> pooledSegments;

    public ManagedPagedOutputView(MemorySegmentPool segmentPool) throws IOException {
        super(segmentPool.nextSegment(), segmentPool.pageSize());
        this.segmentPool = segmentPool;
        this.pooledSegments = new ArrayList<>();
        this.pooledSegments.add(getCurrentSegment());
    }

    @Override
    protected MemorySegment nextSegment() throws IOException {
        MemorySegment segment = segmentPool.nextSegment();
        pooledSegments.add(segment);
        return segment;
    }

    @Override
    public List<MemorySegment> allocatedPooledSegments() {
        return pooledSegments;
    }

    @Override
    public void write(int b) throws IOException {
        writeByte(b);
    }

    @Override
    public void writeChar(int v) throws IOException {
        if (this.positionInSegment < this.pageSize - 1) {
            this.currentSegment.putCharBigEndian(this.positionInSegment, (char) v);
            this.positionInSegment += 2;
        } else if (this.positionInSegment == this.pageSize) {
            advance();
            writeChar(v);
        } else {
            writeByte(v >> 8);
            writeByte(v);
        }
    }

    @Override
    public void writeBytes(String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            writeByte(s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            writeChar(s.charAt(i));
        }
    }

    @Override
    public void writeUTF(String str) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        int c, count = 0;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535) {
            throw new UTFDataFormatException("encoded string too long: " + utflen + " memory");
        }

        if (this.utfBuffer == null || this.utfBuffer.length < utflen + 2) {
            this.utfBuffer = new byte[utflen + 2];
        }
        final byte[] bytearr = this.utfBuffer;

        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
        bytearr[count++] = (byte) (utflen & 0xFF);

        int i;
        for (i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            }
            bytearr[count++] = (byte) c;
        }

        for (; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytearr[count++] = (byte) c;

            } else if (c > 0x07FF) {
                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                bytearr[count++] = (byte) (0x80 | (c & 0x3F));
            } else {
                bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                bytearr[count++] = (byte) (0x80 | (c & 0x3F));
            }
        }

        write(bytearr, 0, utflen + 2);
    }
}
