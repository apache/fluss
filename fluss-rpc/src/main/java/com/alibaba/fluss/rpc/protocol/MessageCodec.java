/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.rpc.protocol;

import com.alibaba.fluss.record.send.Send;
import com.alibaba.fluss.record.send.SendWritableOutput;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.ErrorResponse;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;

/** Utilities for encoding and decoding RPC messages. */
public final class MessageCodec {

    /**
     * Request header format.
     *
     * <ul>
     *   <li>ApiKey => int16
     *   <li>ApiVersion => int16
     *   <li>RequestId => int32
     * </ul>
     */
    public static final int REQUEST_HEADER_LENGTH = 8;

    /**
     * SUCCESS and ERROR Response header format.
     *
     * <ul>
     *   <li>ResponseType => int8
     *   <li>RequestId => int32
     * </ul>
     */
    public static final int RESPONSE_HEADER_LENGTH = 5;

    /**
     * SERVER_FAILURE header format.
     *
     * <ul>
     *   <li>ResponseType => int8
     * </ul>
     */
    public static final int SERVER_FAILURE_HEADER_LENGTH = 1;

    private MessageCodec() {}

    public static Send encodeSuccessResponse(
            ByteBufAllocator allocator, int requestId, ApiMessage response) {
        int responseSize = response.totalSize();
        int frameLength = RESPONSE_HEADER_LENGTH + responseSize;
        // 4 bytes for the request id which is not in response body,
        // and exclude the zero copy size which will be transferred using netty zero-copy
        int bufferSize = frameLength + 4 - response.zeroCopySize();
        ByteBuf buffer = allocator.ioBuffer(bufferSize, bufferSize);
        // header
        buffer.writeInt(frameLength);
        buffer.writeByte(ResponseType.SUCCESS_RESPONSE.id);
        buffer.writeInt(requestId);
        // payload
        SendWritableOutput output = new SendWritableOutput(buffer);
        response.writeTo(output);
        return output.buildSend();
    }

    public static ByteBuf encodeErrorResponse(
            ByteBufAllocator allocator, int requestId, ApiError error) {
        ErrorResponse response = error.toErrorResponse();
        int responseSize = response.totalSize();
        int frameLength = RESPONSE_HEADER_LENGTH + responseSize;
        int bufferSize = frameLength + 4;
        ByteBuf buffer = allocator.ioBuffer(bufferSize, bufferSize);
        // header
        buffer.writeInt(frameLength);
        buffer.writeByte(ResponseType.ERROR_RESPONSE.id);
        buffer.writeInt(requestId);
        // payload
        response.writeTo(buffer);
        return buffer;
    }

    public static ByteBuf encodeServerFailure(ByteBufAllocator allocator, ApiError error) {
        ErrorResponse response = error.toErrorResponse();
        int responseSize = response.totalSize();
        int frameLength = SERVER_FAILURE_HEADER_LENGTH + responseSize;
        int bufferSize = frameLength + 4;
        ByteBuf buffer = allocator.ioBuffer(bufferSize, bufferSize);
        // header
        buffer.writeInt(frameLength);
        buffer.writeByte(ResponseType.SERVER_FAILURE.id);
        // payload
        response.writeTo(buffer);
        return buffer;
    }

    public static ByteBuf encodeRequest(
            ByteBufAllocator allocator,
            short apiKey,
            short apiVersion,
            int requestId,
            ApiMessage request) {
        int requestSize = request.totalSize();
        int frameLength = REQUEST_HEADER_LENGTH + requestSize;
        int bufferSize = frameLength + 4;
        ByteBuf buffer = allocator.ioBuffer(bufferSize, bufferSize);
        // header
        buffer.writeInt(frameLength);
        buffer.writeShort(apiKey);
        buffer.writeShort(apiVersion);
        buffer.writeInt(requestId);
        // payload
        request.writeTo(buffer);
        return buffer;
    }
}
