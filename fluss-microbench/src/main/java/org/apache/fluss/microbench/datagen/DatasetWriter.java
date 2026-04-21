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

package org.apache.fluss.microbench.datagen;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes a pre-generated dataset in binary format.
 *
 * <p>Format:
 *
 * <pre>
 * [4 bytes] magic: "FLSS"
 * [4 bytes] version: 1
 * [8 bytes] record count
 * [4 bytes] schema JSON length -> [N bytes] schema JSON
 * [4 bytes] config YAML length -> [N bytes] config YAML
 * [8 bytes] seed
 * [Records section]
 *   [4 bytes] record length -> [N bytes] record bytes
 *   ...
 * </pre>
 */
public class DatasetWriter implements Closeable {

    static final int MAGIC = 0x464C5353; // "FLSS"
    static final int VERSION = 1;

    private final DataOutputStream out;

    public DatasetWriter(Path path) throws IOException {
        this.out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
    }

    /** Writes the file header. Must be called exactly once before any {@link #writeRecord}. */
    public void writeHeader(long recordCount, String schemaJson, String configYaml, long seed)
            throws IOException {
        out.writeInt(MAGIC);
        out.writeInt(VERSION);
        out.writeLong(recordCount);
        writeString(schemaJson);
        writeString(configYaml);
        out.writeLong(seed);
    }

    /** Appends a single record. */
    public void writeRecord(byte[] record) throws IOException {
        out.writeInt(record.length);
        out.write(record);
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    private void writeString(String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }
}
