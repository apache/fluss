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

package org.apache.fluss.record.bytesview;

import org.apache.fluss.record.send.SendWritableOutput;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * A view of a sequential bytes. It might be a primitive byte array, a Netty ByteBuf, a byte
 * sequence on a file, or a composite of them.
 */
public interface BytesView {

    /**
     * Gets the underlying {@link ByteBuf} of this {@link BytesView}. Modifying the content of the
     * returned {@link ByteBuf} may affect the content of the {@link BytesView}.
     */
    ByteBuf getByteBuf();

    /** Gets the length of the underlying bytes content of this {@link BytesView}. */
    int getBytesLength();

    /**
     * Gets the length of the underlying bytes content of this {@link BytesView} that can be
     * zero-copied.
     *
     * @see SendWritableOutput#writeBytes(BytesView)
     */
    int getZeroCopyLength();
}
