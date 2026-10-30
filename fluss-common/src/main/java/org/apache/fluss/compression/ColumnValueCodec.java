/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.fluss.exception.CorruptRecordException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.IOUtils;

import com.github.luben.zstd.ZstdInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.apache.fluss.config.FlussConfigUtils.getColumnCompressionConfigKey;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Codec for self-describing values in top-level BYTES columns.
 *
 * <p>Each encoded value contains all information required for decoding:
 *
 * <pre>
 * +----------+---------+----------+-----------+---------+
 * | Magic 4B | Ver. 1B | Codec 1B | Length 4B | Payload |
 * +----------+---------+----------+-----------+---------+
 * </pre>
 *
 * <p>The magic is {@code FCMP}, the codec identifies RAW, LZ4, or ZSTD, and the big-endian length
 * is the exact decoded size. Values without the magic are legacy unwrapped values and are returned
 * unchanged.
 */
@Internal
public final class ColumnValueCodec {

    public static final String LZ4 = "lz4";
    public static final String ZSTD = "zstd";

    public static final int HEADER_SIZE = 10;
    public static final byte VERSION = 1;
    public static final byte RAW_ENCODING = 0;
    public static final byte LZ4_ENCODING = 1;
    public static final byte ZSTD_ENCODING = 2;

    private static final int MAGIC = 0x46434D50;
    private static final int MAGIC_SIZE = 4;
    private static final int MAX_DECOMPRESSED_SIZE = 64 * 1024 * 1024;

    private final Map<Integer, Byte> writeEncodings;

    /** Creates a codec for decoding self-describing column values. */
    public ColumnValueCodec() {
        this(Collections.emptyMap());
    }

    /** Creates a codec using the compression configured in the table properties. */
    public ColumnValueCodec(Map<Integer, String> tableConfigs) {
        checkNotNull(tableConfigs, "tableConfigs must not be null");
        Map<Integer, Byte> encodings = new HashMap<>();
        for (Map.Entry<Integer, String> entry : tableConfigs.entrySet()) {
            encodings.put(entry.getKey(), encodingOf(entry.getValue()));
        }
        this.writeEncodings = Collections.unmodifiableMap(encodings);
    }

    /** Returns whether a property value enables the first supported codec. */
    public static boolean isLz4(String value) {
        return value != null && LZ4.equals(value.trim().toLowerCase(Locale.ROOT));
    }

    /** Returns whether a property value enables ZSTD compression. */
    public static boolean isZstd(String value) {
        return value != null && ZSTD.equals(value.trim().toLowerCase(Locale.ROOT));
    }

    /** Returns whether a property value is a supported compression. */
    public static boolean isSupportedCompression(String value) {
        return isLz4(value) || isZstd(value);
    }

    /** Returns the configured compression keyed by top-level column index. */
    public static Map<Integer, String> compressionByColumn(
            Schema schema, Map<String, String> properties) {
        checkNotNull(schema, "schema must not be null");
        checkNotNull(properties, "properties must not be null");
        Map<Integer, String> compressionByColumn = new HashMap<>();
        for (int i = 0; i < schema.getColumns().size(); i++) {
            String codec = properties.get(getColumnCompressionConfigKey(schema.getColumnName(i)));
            if (codec != null) {
                checkArgument(
                        isSupportedCompression(codec),
                        "Unsupported column compression '%s' for column '%s'.",
                        codec,
                        schema.getColumnName(i));
                checkArgument(
                        schema.getRowType().getTypeAt(i).getTypeRoot() == DataTypeRoot.BYTES,
                        "Column compression only supports BYTES columns, but column '%s' is %s.",
                        schema.getColumnName(i),
                        schema.getRowType().getTypeAt(i));
                compressionByColumn.put(i, codec);
            }
        }
        return compressionByColumn;
    }

    /** Returns the top-level column indexes configured for compression. */
    public static BitSet compressedColumnIndexes(Schema schema, Map<String, String> properties) {
        BitSet compressedColumns = new BitSet(schema.getColumns().size());
        for (Integer columnIndex : compressionByColumn(schema, properties).keySet()) {
            compressedColumns.set(columnIndex);
        }
        return compressedColumns;
    }

    /** Returns all top-level BYTES column indexes that may contain an encoded value. */
    public static BitSet bytesColumnIndexes(Schema schema) {
        checkNotNull(schema, "schema must not be null");
        BitSet bytesColumns = new BitSet(schema.getColumns().size());
        for (int i = 0; i < schema.getColumns().size(); i++) {
            if (schema.getRowType().getTypeAt(i).getTypeRoot() == DataTypeRoot.BYTES) {
                bytesColumns.set(i);
            }
        }
        return bytesColumns;
    }

    /** Encodes a logical value using the configured compression and a self-describing envelope. */
    public byte[] encode(int columnIndex, byte[] value) {
        checkNotNull(value, "value must not be null");
        Byte encoding = writeEncodings.get(columnIndex);
        checkArgument(
                encoding != null, "No compression configured for column index %s.", columnIndex);
        byte[] compressed = compress(encoding, value);
        if (compressed.length >= value.length) {
            return envelope(RAW_ENCODING, value.length, value);
        }
        return envelope(encoding, value.length, compressed);
    }

    /**
     * Decodes a self-describing column value. Values without the magic are returned unchanged.
     *
     * @param value the physical value
     * @return the logical value
     * @throws CorruptRecordException if an envelope is malformed or cannot be decoded
     */
    public byte[] decode(byte[] value) {
        checkNotNull(value, "value must not be null");
        if (!hasMagic(value)) {
            return value;
        }
        if (value.length < HEADER_SIZE) {
            throw corrupt("Truncated column value envelope");
        }
        if (value[4] != VERSION) {
            throw corrupt("Unsupported column value envelope version: " + value[4]);
        }

        int encoding = value[5] & 0xFF;
        long decodedLength = Integer.toUnsignedLong(readInt(value, 6));
        if (decodedLength > MAX_DECOMPRESSED_SIZE) {
            throw corrupt("Decoded column value size exceeds the limit: " + decodedLength);
        }

        int outputLength = (int) decodedLength;
        int payloadLength = value.length - HEADER_SIZE;
        if (encoding == RAW_ENCODING) {
            if (payloadLength != outputLength) {
                throw corrupt(
                        "RAW column value length does not match the envelope: expected "
                                + outputLength
                                + ", actual "
                                + payloadLength);
            }
            return Arrays.copyOfRange(value, HEADER_SIZE, value.length);
        }

        if (encoding != LZ4_ENCODING && encoding != ZSTD_ENCODING) {
            throw corrupt("Unsupported column value encoding: " + encoding);
        }
        return decompress((byte) encoding, value, HEADER_SIZE, payloadLength, outputLength);
    }

    /** Returns whether a value starts with the column-value envelope magic. */
    public boolean hasEnvelope(byte[] value) {
        checkNotNull(value, "value must not be null");
        return hasMagic(value);
    }

    private static byte[] envelope(byte encoding, int decodedLength, byte[] payload) {
        byte[] envelope = new byte[HEADER_SIZE + payload.length];
        writeInt(envelope, 0, MAGIC);
        envelope[4] = VERSION;
        envelope[5] = encoding;
        writeInt(envelope, 6, decodedLength);
        System.arraycopy(payload, 0, envelope, HEADER_SIZE, payload.length);
        return envelope;
    }

    private static boolean hasMagic(byte[] value) {
        return value.length >= MAGIC_SIZE && readInt(value, 0) == MAGIC;
    }

    private static int readInt(byte[] value, int offset) {
        return ((value[offset] & 0xFF) << 24)
                | ((value[offset + 1] & 0xFF) << 16)
                | ((value[offset + 2] & 0xFF) << 8)
                | (value[offset + 3] & 0xFF);
    }

    private static void writeInt(byte[] value, int offset, int number) {
        value[offset] = (byte) (number >>> 24);
        value[offset + 1] = (byte) (number >>> 16);
        value[offset + 2] = (byte) (number >>> 8);
        value[offset + 3] = (byte) number;
    }

    private static CorruptRecordException corrupt(String message) {
        return new CorruptRecordException(message);
    }

    private static CorruptRecordException corrupt(String message, Throwable cause) {
        return new CorruptRecordException(message, cause);
    }

    private static byte encodingOf(String compression) {
        checkArgument(
                isSupportedCompression(compression),
                "Unsupported column compression '%s'.",
                compression);
        return isLz4(compression) ? LZ4_ENCODING : ZSTD_ENCODING;
    }

    private byte[] compress(byte encoding, byte[] value) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (InputStream input = new ByteArrayInputStream(value);
                OutputStream compressedOutput = compressionOutputStream(output, encoding)) {
            IOUtils.copyBytes(input, compressedOutput);
        } catch (IOException e) {
            throw new FlussRuntimeException(
                    "Failed to encode " + compressionName(encoding) + " column value.", e);
        }
        return output.toByteArray();
    }

    private byte[] decompress(
            byte encoding, byte[] value, int offset, int length, int outputLength) {
        ByteArrayOutputStream output = new ByteArrayOutputStream(outputLength);
        try (InputStream input = compressionInputStream(encoding, value, offset, length)) {
            IOUtils.copyBytes(input, output);
        } catch (IOException | RuntimeException e) {
            throw corrupt("Failed to decode " + compressionName(encoding) + " column value.", e);
        }
        byte[] result = output.toByteArray();
        if (result.length != outputLength) {
            throw corrupt(
                    "Decoded column value length does not match the envelope: expected "
                            + outputLength
                            + ", actual "
                            + result.length);
        }
        return result;
    }

    private static OutputStream compressionOutputStream(OutputStream output, byte encoding)
            throws IOException {
        if (encoding == LZ4_ENCODING) {
            return new FlussLZ4BlockOutputStream(output);
        }
        if (encoding == ZSTD_ENCODING) {
            return new FlussZSTDBlockOutputStream(output);
        }
        throw new IllegalArgumentException("Unsupported column value encoding: " + encoding);
    }

    private static InputStream compressionInputStream(
            byte encoding, byte[] value, int offset, int length) throws IOException {
        if (encoding == LZ4_ENCODING) {
            return new FlussLZ4BlockInputStream(ByteBuffer.wrap(value, offset, length));
        }
        if (encoding == ZSTD_ENCODING) {
            return new ZstdInputStream(new ByteArrayInputStream(value, offset, length));
        }
        throw new IllegalArgumentException("Unsupported column value encoding: " + encoding);
    }

    private static String compressionName(byte encoding) {
        return encoding == LZ4_ENCODING ? "LZ4" : "ZSTD";
    }
}
