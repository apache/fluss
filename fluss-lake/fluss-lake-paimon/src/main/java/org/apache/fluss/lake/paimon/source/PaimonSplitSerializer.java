/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamClass;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkState;

/** Serializer for paimon split. */
public class PaimonSplitSerializer implements SimpleVersionedSerializer<PaimonSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSplitSerializer.class);

    // VERSION_1 and VERSION_2 persisted DataSplit via Java serialization; kept read-only to
    // restore state written by older versions and must never be changed.
    private static final int VERSION_1 = 1;
    // VERSION_2 additionally persists the partition values.
    private static final int VERSION_2 = 2;
    // VERSION_3 persists DataSplit via Paimon's own versioned binary protocol whose bytes contain
    // no Java class names, so it survives class relocation (shading) in downstream distributions.
    private static final int VERSION_3 = 3;

    // Split type tags of VERSION_3, aligned with Paimon master's SplitSerializer type ids.
    // Serialization must dispatch on the concrete split class: DataSplit subclasses append extra
    // fields after the base payload (e.g. FallbackDataSplit), so deserializing them with the base
    // DataSplit.deserialize would leave trailing bytes and shift all subsequent reads.
    private static final int SPLIT_TYPE_DATA_SPLIT = 1;
    private static final int SPLIT_TYPE_FALLBACK_DATA_SPLIT = 6;

    @Override
    public int getVersion() {
        return VERSION_3;
    }

    @Override
    public byte[] serialize(PaimonSplit paimonSplit) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        serializeDataSplit(paimonSplit.dataSplit(), view);
        view.writeBoolean(paimonSplit.isBucketUnAware());
        List<String> partition = paimonSplit.partition();
        view.writeInt(partition.size());
        for (String value : partition) {
            view.writeUTF(value);
        }
        return out.toByteArray();
    }

    private void serializeDataSplit(DataSplit dataSplit, DataOutputViewStreamWrapper view)
            throws IOException {
        if (dataSplit.getClass() == DataSplit.class) {
            view.writeByte(SPLIT_TYPE_DATA_SPLIT);
            dataSplit.serialize(view);
        } else if (dataSplit instanceof FallbackReadFileStoreTable.FallbackDataSplit) {
            view.writeByte(SPLIT_TYPE_FALLBACK_DATA_SPLIT);
            dataSplit.serialize(view);
        } else {
            // fail fast: an unknown subclass may append extra fields after the base payload,
            // silently corrupting the stream on restore
            throw new IOException("Unsupported DataSplit class: " + dataSplit.getClass().getName());
        }
    }

    @Override
    public PaimonSplit deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case VERSION_1:
            case VERSION_2:
                return deserializeLegacy(version, serialized);
            case VERSION_3:
                return deserializeV3(serialized);
            default:
                throw new IOException("Unsupported PaimonSplit serialization version: " + version);
        }
    }

    private PaimonSplit deserializeV3(byte[] serialized) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(serialized);
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
        int splitType = view.readByte();
        DataSplit dataSplit;
        switch (splitType) {
            case SPLIT_TYPE_DATA_SPLIT:
                dataSplit = DataSplit.deserialize(view);
                break;
            case SPLIT_TYPE_FALLBACK_DATA_SPLIT:
                dataSplit = FallbackReadFileStoreTable.FallbackDataSplit.deserialize(view);
                break;
            default:
                throw new IOException("Unsupported DataSplit type: " + splitType);
        }
        boolean isBucketUnAware = view.readBoolean();
        int size = view.readInt();
        List<String> partition = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            partition.add(view.readUTF());
        }
        return new PaimonSplit(dataSplit, isBucketUnAware, partition);
    }

    private PaimonSplit deserializeLegacy(int version, byte[] serialized) throws IOException {
        LOG.debug("Restoring PaimonSplit from legacy state format (version {}).", version);
        ByteArrayInputStream in = new ByteArrayInputStream(serialized);
        DataSplit dataSplit;
        try {
            RelocatingObjectInputStream ois =
                    new RelocatingObjectInputStream(in, getClass().getClassLoader());
            dataSplit = (DataSplit) ois.readObject();
            DataInputStream dis = new DataInputStream(in);
            boolean isBucketUnAware = dis.readBoolean();
            if (version == VERSION_1) {
                // VERSION_1 did not store partition values separately, but string partitions were
                // exposed through DataSplit.partition(). Preserve that old behavior.
                return new PaimonSplit(
                        dataSplit, isBucketUnAware, readStringPartition(dataSplit.partition()));
            } else {
                int size = dis.readInt();
                List<String> partition = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    partition.add(dis.readUTF());
                }
                return new PaimonSplit(dataSplit, isBucketUnAware, partition);
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize PaimonSplit", e);
        }
    }

    private List<String> readStringPartition(BinaryRow partition) {
        if (partition == null || partition.getFieldCount() == 0) {
            return Collections.emptyList();
        }

        List<String> partitions = new ArrayList<>(partition.getFieldCount());
        for (int i = 0; i < partition.getFieldCount(); i++) {
            partitions.add(partition.getString(i).toString());
        }
        return partitions;
    }

    /**
     * An {@link java.io.ObjectInputStream} that restores legacy state written before Paimon classes
     * were relocated (shaded): class names starting with the original {@code org.apache.paimon.}
     * prefix in the serialization stream are remapped to the actual (possibly relocated) class
     * names at deserialization time.
     *
     * <p>In non-relocated builds the actual prefix equals the original prefix, so the remapping
     * degrades to a no-op and behavior is unchanged.
     */
    static class RelocatingObjectInputStream
            extends InstantiationUtil.ClassLoaderObjectInputStream {

        // The prefix must NOT appear as a plain string literal: shade plugins rewrite matching
        // constant-pool strings, which would silently turn old and new prefixes into the same
        // string. Build it at runtime instead.
        private static final String ORIGINAL_PREFIX =
                String.join(".", "org", "apache", "paimon") + ".";

        // Derived from the actually loaded class, so any relocation prefix works; in
        // non-relocated builds it equals ORIGINAL_PREFIX.
        private static final String ACTUAL_PREFIX;

        static {
            String cls = DataSplit.class.getName();
            // relocation only rewrites the package prefix, so the trailing part is stable; fail
            // loudly instead of computing a wrong prefix silently
            String suffix = "table.source.DataSplit";
            checkState(cls.endsWith(suffix), "Unexpected DataSplit class name: %s", cls);
            ACTUAL_PREFIX = cls.substring(0, cls.length() - suffix.length());
        }

        private final String originalPrefix;
        private final String actualPrefix;

        RelocatingObjectInputStream(InputStream in, ClassLoader cl) throws IOException {
            this(in, cl, ORIGINAL_PREFIX, ACTUAL_PREFIX);
        }

        @VisibleForTesting
        RelocatingObjectInputStream(
                InputStream in, ClassLoader cl, String originalPrefix, String actualPrefix)
                throws IOException {
            super(in, cl);
            this.originalPrefix = originalPrefix;
            this.actualPrefix = actualPrefix;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            String name = desc.getName();
            if (!actualPrefix.equals(originalPrefix) && name.startsWith(originalPrefix)) {
                String relocated = actualPrefix + name.substring(originalPrefix.length());
                try {
                    return Class.forName(relocated, false, classLoader);
                } catch (ClassNotFoundException ignored) {
                    // fall back to the default resolution to keep the original exception path
                }
            }
            return super.resolveClass(desc);
        }
    }
}
