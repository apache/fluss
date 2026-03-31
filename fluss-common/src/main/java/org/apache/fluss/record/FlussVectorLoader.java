/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.record;

import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf;
import org.apache.fluss.shaded.arrow.org.apache.arrow.util.Collections2;
import org.apache.fluss.shaded.arrow.org.apache.arrow.util.Preconditions;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TypeLayout;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A patched version of Arrow's {@code VectorLoader} that ensures decompressed buffers are properly
 * released when an error (e.g. OOM) occurs during {@link #load(ArrowRecordBatch)}.
 *
 * <p>In the original Arrow implementation, the decompression loop runs <b>outside</b> the
 * try-finally block that guards {@code loadFieldBuffers}. This means if decompression succeeds for
 * the first N buffers of a field but fails on the (N+1)-th buffer, the already-decompressed buffers
 * in {@code ownBuffers} are never closed, leaking Direct Memory.
 *
 * <p>This workaround moves the decompression loop <b>inside</b> the try block so that the finally
 * clause always closes every buffer in {@code ownBuffers}, regardless of whether the load succeeds
 * or fails:
 *
 * <ul>
 *   <li><b>Success path:</b> {@code loadFieldBuffers} retains each buffer (ref count +1), then the
 *       finally close decrements it back (ref count -1). The field vector still holds the buffer.
 *   <li><b>Error path:</b> The finally close decrements each already-decompressed buffer's ref
 *       count to 0, immediately freeing the Direct Memory.
 * </ul>
 *
 * <p>TODO: This class should be removed once Apache Arrow fixes the buffer leak in their {@code
 * VectorLoader.loadBuffers()} method. See:
 *
 * <ul>
 *   <li>Apache Arrow issue: <a
 *       href="https://github.com/apache/arrow-java/issues/1037">arrow-java#1037</a>
 *   <li>Fluss issue: <a href="https://github.com/apache/fluss/issues/2646">FLUSS-2646</a>
 * </ul>
 */
public class FlussVectorLoader {
    private final VectorSchemaRoot root;
    private final CompressionCodec.Factory factory;
    private boolean decompressionNeeded;

    public FlussVectorLoader(VectorSchemaRoot root, CompressionCodec.Factory factory) {
        this.root = root;
        this.factory = factory;
    }

    public void load(ArrowRecordBatch recordBatch) {
        loadWithProjection(recordBatch, null);
    }

    /**
     * Loads the ArrowRecordBatch with optional column projection for shredded Variant fields.
     *
     * <p>When {@code skipTypedValueChildren} is non-null, the loader will skip loading buffers for
     * typed_value children whose names are in the skip set. This avoids the cost of loading Arrow
     * vectors for non-projected shredded fields, which is the primary performance bottleneck when
     * reading shredded Variant batches with many fields.
     *
     * @param recordBatch the Arrow record batch to load
     * @param skipTypedValueChildren field names under typed_value to skip loading (null = load all)
     */
    public void loadWithProjection(
            ArrowRecordBatch recordBatch, Set<String> skipTypedValueChildren) {
        Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
        Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
        CompressionUtil.CodecType codecType =
                CodecType.fromCompressionType(recordBatch.getBodyCompression().getCodec());
        this.decompressionNeeded = codecType != CodecType.NO_COMPRESSION;
        CompressionCodec codec =
                this.decompressionNeeded
                        ? this.factory.createCodec(codecType)
                        : NoCompressionCodec.INSTANCE;

        for (FieldVector fieldVector : this.root.getFieldVectors()) {
            this.loadBuffersSelective(
                    fieldVector,
                    fieldVector.getField(),
                    buffers,
                    nodes,
                    codec,
                    skipTypedValueChildren);
        }

        this.root.setRowCount(recordBatch.getLength());
        if (nodes.hasNext() || buffers.hasNext()) {
            throw new IllegalArgumentException(
                    "not all nodes and buffers were consumed. nodes: "
                            + Collections2.toString(nodes)
                            + " buffers: "
                            + Collections2.toString(buffers));
        }
    }

    private void loadBuffersSelective(
            FieldVector vector,
            Field field,
            Iterator<ArrowBuf> buffers,
            Iterator<ArrowFieldNode> nodes,
            CompressionCodec codec,
            Set<String> skipTypedValueChildren) {
        Preconditions.checkArgument(
                nodes.hasNext(), "no more field nodes for field %s and vector %s", field, vector);
        ArrowFieldNode fieldNode = nodes.next();
        int bufferLayoutCount = TypeLayout.getTypeBufferCount(field.getType());
        List<ArrowBuf> ownBuffers = new ArrayList<>(bufferLayoutCount);

        try {
            for (int j = 0; j < bufferLayoutCount; ++j) {
                ArrowBuf nextBuf = buffers.next();
                ArrowBuf bufferToAdd =
                        nextBuf.writerIndex() > 0L
                                ? codec.decompress(vector.getAllocator(), nextBuf)
                                : nextBuf;
                ownBuffers.add(bufferToAdd);
                if (this.decompressionNeeded) {
                    nextBuf.getReferenceManager().retain();
                }
            }
            vector.loadFieldBuffers(fieldNode, ownBuffers);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Could not load buffers for field "
                            + field
                            + ". error message: "
                            + e.getMessage(),
                    e);
        } finally {
            if (this.decompressionNeeded) {
                for (ArrowBuf buf : ownBuffers) {
                    buf.close();
                }
            }
        }

        List<Field> children = field.getChildren();
        if (!children.isEmpty()) {
            List<FieldVector> childrenFromFields = vector.getChildrenFromFields();
            Preconditions.checkArgument(
                    children.size() == childrenFromFields.size(),
                    "should have as many children as in the schema: found %s expected %s",
                    childrenFromFields.size(),
                    children.size());

            // When this field is the "typed_value" struct and projection is active,
            // skip loading for non-projected shredded field subtrees.
            boolean isTypedValueParent =
                    skipTypedValueChildren != null && field.getName().equals("typed_value");

            for (int i = 0; i < childrenFromFields.size(); ++i) {
                Field child = children.get(i);
                FieldVector fieldVector = childrenFromFields.get(i);
                if (isTypedValueParent && skipTypedValueChildren.contains(child.getName())) {
                    // Skip this subtree: consume nodes/buffers without loading
                    skipFieldBuffers(child, buffers, nodes);
                } else {
                    this.loadBuffersSelective(
                            fieldVector, child, buffers, nodes, codec, skipTypedValueChildren);
                }
            }
        }
    }

    /**
     * Consumes all nodes and buffers for a field and its children without loading them into
     * vectors. Used to skip non-projected shredded field subtrees in the IPC stream.
     */
    private void skipFieldBuffers(
            Field field, Iterator<ArrowBuf> buffers, Iterator<ArrowFieldNode> nodes) {
        nodes.next();
        int bufferCount = TypeLayout.getTypeBufferCount(field.getType());
        for (int i = 0; i < bufferCount; i++) {
            buffers.next();
        }
        for (Field child : field.getChildren()) {
            skipFieldBuffers(child, buffers, nodes);
        }
    }
}
