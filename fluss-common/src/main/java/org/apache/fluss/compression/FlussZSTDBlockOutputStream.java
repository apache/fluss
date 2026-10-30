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

import com.github.luben.zstd.ZstdOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/** An output stream that writes ZSTD frames using Fluss's default compression level. */
public class FlussZSTDBlockOutputStream extends ZstdOutputStream {

    // TODO: Make the compression level configurable.
    public static final int DEFAULT_COMPRESSION_LEVEL = 3;

    /** Creates a ZSTD output stream using Fluss's default compression level. */
    public FlussZSTDBlockOutputStream(OutputStream out) throws IOException {
        super(out, DEFAULT_COMPRESSION_LEVEL);
    }
}
