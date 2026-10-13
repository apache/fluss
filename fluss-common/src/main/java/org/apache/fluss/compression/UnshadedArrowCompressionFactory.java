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

package org.apache.fluss.compression;

import org.apache.fluss.annotation.Internal;

import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;

/** Unshaded Arrow compression factory for scanner/read path. */
@Internal
public class UnshadedArrowCompressionFactory implements CompressionCodec.Factory {

    public static final UnshadedArrowCompressionFactory INSTANCE =
            new UnshadedArrowCompressionFactory();

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
        switch (codecType) {
            case LZ4_FRAME:
                return new UnshadedLz4ArrowCompressionCodec();
            case ZSTD:
                return new UnshadedZstdArrowCompressionCodec();
            case NO_COMPRESSION:
                return NoCompressionCodec.INSTANCE;
            default:
                throw new IllegalArgumentException("Compression type not supported: " + codecType);
        }
    }

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel) {
        switch (codecType) {
            case LZ4_FRAME:
                return new UnshadedLz4ArrowCompressionCodec();
            case ZSTD:
                return new UnshadedZstdArrowCompressionCodec(compressionLevel);
            case NO_COMPRESSION:
                return NoCompressionCodec.INSTANCE;
            default:
                throw new IllegalArgumentException("Compression type not supported: " + codecType);
        }
    }
}
