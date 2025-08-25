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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionCodec;

/** Compression information for Arrow record batches. */
public class ArrowCompressionInfo {

    public static final ArrowCompressionInfo DEFAULT_COMPRESSION =
            new ArrowCompressionInfo(ArrowCompressionType.ZSTD, 3);
    public static final ArrowCompressionInfo NO_COMPRESSION =
            new ArrowCompressionInfo(ArrowCompressionType.NONE, -1);

    private final ArrowCompressionType compressionType;
    private final int compressionLevel;

    public ArrowCompressionInfo(ArrowCompressionType compressionType, int compressionLevel) {
        this.compressionType = compressionType;
        this.compressionLevel = compressionLevel;
    }

    public ArrowCompressionType getCompressionType() {
        return compressionType;
    }

    /**
     * Get the compression level. If the compression level is not supported by the compression type,
     * -1 is returned.
     */
    public int getCompressionLevel() {
        return compressionLevel;
    }

    /** Create an Arrow compression codec based on the compression type and level. */
    public CompressionCodec createCompressionCodec() {
        return ArrowCompressionFactory.INSTANCE.createCodec(
                ArrowCompressionFactory.toArrowCompressionCodecType(compressionType),
                compressionLevel);
    }

    @Override
    public String toString() {
        return compressionLevel == -1
                ? compressionType.toString()
                : compressionType + "-" + compressionLevel;
    }

    public static ArrowCompressionInfo fromConf(Configuration conf) {
        ArrowCompressionType compressionType =
                conf.get(ConfigOptions.TABLE_LOG_ARROW_COMPRESSION_TYPE);
        if (compressionType == ArrowCompressionType.ZSTD) {
            int compressionLevel = conf.get(ConfigOptions.TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL);
            if (compressionLevel < 1 || compressionLevel > 22) {
                throw new IllegalArgumentException(
                        "Invalid ZSTD compression level: "
                                + compressionLevel
                                + ". Expected a value between 1 and 22.");
            }
            return new ArrowCompressionInfo(compressionType, compressionLevel);
        } else {
            return new ArrowCompressionInfo(compressionType, -1);
        }
    }
}
