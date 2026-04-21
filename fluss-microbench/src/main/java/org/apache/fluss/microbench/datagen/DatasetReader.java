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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads a pre-generated dataset written by {@link DatasetWriter}.
 *
 * <p>Records are consumed sequentially via {@link #hasNext()} / {@link #next()}.
 */
public class DatasetReader implements Closeable {

    private final DataInputStream in;
    private final long recordCount;
    private final String schemaJson;
    private final String configYaml;
    private final long seed;
    private long recordsRead;

    public DatasetReader(Path path) throws IOException {
        DataInputStream stream =
                new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        try {
            int magic = stream.readInt();
            if (magic != DatasetWriter.MAGIC) {
                throw new IOException(
                        String.format(
                                "Invalid magic: expected 0x%08X, got 0x%08X",
                                DatasetWriter.MAGIC, magic));
            }
            int version = stream.readInt();
            if (version != DatasetWriter.VERSION) {
                throw new IOException(
                        String.format(
                                "Unsupported version: expected %d, got %d",
                                DatasetWriter.VERSION, version));
            }
            this.recordCount = stream.readLong();
            this.schemaJson = readString(stream);
            this.configYaml = readString(stream);
            this.seed = stream.readLong();
            this.in = stream;
        } catch (Throwable t) {
            stream.close();
            throw t;
        }
    }

    public long recordCount() {
        return recordCount;
    }

    public String schemaJson() {
        return schemaJson;
    }

    public String configYaml() {
        return configYaml;
    }

    public long seed() {
        return seed;
    }

    /** Returns {@code true} if there are more records to read. */
    public boolean hasNext() {
        return recordsRead < recordCount;
    }

    /** Reads the next record. */
    public byte[] next() throws IOException {
        int len = in.readInt();
        if (len < 0 || len > 256 * 1024 * 1024) {
            throw new IOException(
                    "Corrupted dataset: invalid record length "
                            + len
                            + " at record "
                            + recordsRead);
        }
        byte[] record = new byte[len];
        in.readFully(record);
        recordsRead++;
        return record;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    private static String readString(DataInputStream stream) throws IOException {
        int len = stream.readInt();
        if (len < 0 || len > 10 * 1024 * 1024) {
            throw new IOException("Corrupted dataset: invalid string length " + len);
        }
        byte[] bytes = new byte[len];
        stream.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
