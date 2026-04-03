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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Unshaded variant of {@link FlussVectorLoader} for scanner/read path. */
public class UnshadedFlussVectorLoader {
    private final VectorSchemaRoot root;
    private final CompressionCodec.Factory factory;
    private boolean decompressionNeeded;

    public UnshadedFlussVectorLoader(VectorSchemaRoot root, CompressionCodec.Factory factory) {
        this.root = root;
        this.factory = factory;
    }

    public void load(ArrowRecordBatch recordBatch) {
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
            this.loadBuffers(fieldVector, fieldVector.getField(), buffers, nodes, codec);
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

    private void loadBuffers(
            FieldVector vector,
            Field field,
            Iterator<ArrowBuf> buffers,
            Iterator<ArrowFieldNode> nodes,
            CompressionCodec codec) {
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

            for (int i = 0; i < childrenFromFields.size(); ++i) {
                Field child = children.get(i);
                FieldVector fieldVector = childrenFromFields.get(i);
                this.loadBuffers(fieldVector, child, buffers, nodes, codec);
            }
        }
    }
}
