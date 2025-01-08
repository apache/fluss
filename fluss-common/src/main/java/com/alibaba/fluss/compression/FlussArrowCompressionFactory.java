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

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionCodec;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.compression.NoCompressionCodec;

/* This file is based on source code of Apache Arrow-java Project (https://github.com/apache/arrow-java), licensed by
 * the Apache Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership. */

/**
 * A factory implementation based on Apache Arrow CompressionCodec.Factory interface. This maybe
 * removed as the arrow upgrade to v18.0, which provides a default implementation as
 * CommonsCompressionFactory.
 */
@Internal
public class FlussArrowCompressionFactory implements CompressionCodec.Factory {

    public static final FlussArrowCompressionFactory INSTANCE = new FlussArrowCompressionFactory();

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
        switch (codecType) {
            case LZ4_FRAME:
                return new Lz4ArrowCompressionCodec();
            case ZSTD:
                return new ZstdArrowCompressionCodec();
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
                return new Lz4ArrowCompressionCodec();
            case ZSTD:
                return new ZstdArrowCompressionCodec(compressionLevel);
            case NO_COMPRESSION:
                return NoCompressionCodec.INSTANCE;
            default:
                throw new IllegalArgumentException("Compression type not supported: " + codecType);
        }
    }

    public static CompressionUtil.CodecType toArrowCompressionCodecType(
            ArrowCompressionType compressionType) {
        switch (compressionType) {
            case NO:
                return CompressionUtil.CodecType.NO_COMPRESSION;
            case LZ4_FRAME:
                return CompressionUtil.CodecType.LZ4_FRAME;
            case ZSTD:
                return CompressionUtil.CodecType.ZSTD;
            default:
                throw new IllegalArgumentException(
                        "Arrow compression type not supported: " + compressionType);
        }
    }
}
