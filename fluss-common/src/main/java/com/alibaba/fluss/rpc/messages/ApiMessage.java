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

package com.alibaba.fluss.rpc.messages;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.record.send.WritableOutput;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * An object that can serialize itself. The serialization protocol is versioned. Messages also
 * implement toString, equals, and hashCode.
 */
@Internal
public interface ApiMessage {

    /** Gets the total serialized byte array size of the message. */
    int totalSize();

    /**
     * Get the total "zero-copy" size of the message. This is the summed total of all fields which
     * have a type of 'bytes' with 'zeroCopy' enabled.
     *
     * @return total size of zero-copy data in the message
     */
    int zeroCopySize();

    /**
     * Size excluding zero copy fields as specified by {@link #zeroCopySize}. This is typically the
     * size of the byte buffer used to serialize messages.
     */
    default int sizeExcludingZeroCopy() {
        return totalSize() - zeroCopySize();
    }

    /**
     * Deserialize the message from the given {@link ByteBuf}. The deserialization happens lazily
     * (i.e. zero-copy) only for {@code "[optional|required] bytes records = ?"} (nested) fields. If
     * there is any lazy deserialization happens, the {@link #isLazilyParsed()} returns true.
     *
     * <p>Note: the current message will hold the reference of {@link ByteBuf}, please remember to
     * release the {@link ByteBuf} until the message has been fully consumed.
     */
    void parseFrom(ByteBuf buffer, int size);

    /**
     * Returns true if there is any {@code "[optional|required] bytes records = ?"} (nested) fields.
     * These fields will be lazily deserialized when {@link #parseFrom(ByteBuf, int)} is called.
     */
    boolean isLazilyParsed();

    /** Get the parsed byte buffer of the message. */
    ByteBuf getParsedByteBuf();

    /**
     * Deserialize the message from the given byte array.
     *
     * <p>Note: the current message will hold the reference of {@code byte[]}, modify the {@code
     * byte[]} will make the message corrupt.
     */
    void parseFrom(byte[] a);

    /** Serialize the message into the given {@link ByteBuf}. */
    int writeTo(ByteBuf buffer);

    /** Serialize the message into the given {@link WritableOutput}. */
    void writeTo(WritableOutput output);

    /** Serialize the message into byte array. */
    byte[] toByteArray();
}
