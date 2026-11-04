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

package org.apache.fluss.client.write;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.Errors;

import java.util.Collections;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makePutKvResponse;

/** Helpers for inspecting write RPC requests and constructing their responses. */
final class WriteRpcTestUtils {

    private WriteRpcTestUtils() {}

    static long getWriteRequestTableId(ApiMessage request) {
        if (request instanceof ProduceLogRequest) {
            return ((ProduceLogRequest) request).getTableId();
        }
        if (request instanceof PutKvRequest) {
            return ((PutKvRequest) request).getTableId();
        }
        throw new IllegalArgumentException(
                "Expected a write request but found " + request.getClass().getName());
    }

    static ProduceLogResponse createProduceLogResponse(
            TableBucket tb, long baseOffset, long endOffset) {
        return makeProduceLogResponse(
                Collections.singletonList(
                        new ProduceLogResultForBucket(tb, baseOffset, endOffset)));
    }

    static ProduceLogResponse createProduceLogResponse(TableBucket tb, Errors error) {
        return makeProduceLogResponse(
                Collections.singletonList(new ProduceLogResultForBucket(tb, error.toApiError())));
    }

    static PutKvResponse createPutKvResponse(TableBucket tb, long endOffset) {
        return makePutKvResponse(
                Collections.singletonList(new PutKvResultForBucket(tb, endOffset)));
    }

    static PutKvResponse createPutKvResponse(TableBucket tb, long endOffset, float pressure) {
        return makePutKvResponse(
                Collections.singletonList(new PutKvResultForBucket(tb, endOffset, pressure)));
    }

    static PutKvResponse createPutKvResponse(TableBucket tb, Errors error) {
        return makePutKvResponse(
                Collections.singletonList(new PutKvResultForBucket(tb, error.toApiError())));
    }
}
