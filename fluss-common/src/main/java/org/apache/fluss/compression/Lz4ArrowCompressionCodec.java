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

package com.alibaba.fluss.compression;

import com.alibaba.fluss.utils.IOUtils;

import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/* This file is based on source code of Apache Arrow-java Project (https://github.com/apache/arrow-java), licensed by
 * the Apache Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership. */

/** Arrow Compression codec for the LZ4 algorithm. */
public class Lz4ArrowCompressionCodec extends AbstractCompressionCodec {
    @Override
    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
        checkArgument(
                uncompressedBuffer.writerIndex() <= Integer.MAX_VALUE,
                "The uncompressed buffer size exceeds the integer limit");

        byte[] inBytes = new byte[(int) uncompressedBuffer.writerIndex()];
        uncompressedBuffer.getBytes(/*index=*/ 0, inBytes);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (InputStream in = new ByteArrayInputStream(inBytes);
                OutputStream out = new FlussLZ4BlockOutputStream(baos)) {
            IOUtils.copyBytes(in, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        byte[] outBytes = baos.toByteArray();

        ArrowBuf compressedBuffer =
                allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
        compressedBuffer.setBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, outBytes);
        compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
        return compressedBuffer;
    }

    @Override
    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
        checkArgument(
                compressedBuffer.writerIndex() <= Integer.MAX_VALUE,
                "The compressed buffer size exceeds the integer limit");

        long decompressedLength = readUncompressedLength(compressedBuffer);

        ByteBuffer inByteBuffer =
                compressedBuffer.nioBuffer(
                        CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH,
                        (int)
                                (compressedBuffer.writerIndex()
                                        - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH));
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) decompressedLength);
        try (InputStream in = new FlussLZ4BlockInputStream(inByteBuffer)) {
            IOUtils.copyBytes(in, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        byte[] outBytes = out.toByteArray();
        ArrowBuf decompressedBuffer = allocator.buffer(outBytes.length);
        decompressedBuffer.setBytes(/*index=*/ 0, outBytes);
        decompressedBuffer.writerIndex(decompressedLength);
        return decompressedBuffer;
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
        return CompressionUtil.CodecType.LZ4_FRAME;
    }
}
