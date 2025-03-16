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

package com.alibaba.fluss.rpc.netty.client;

import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.protocol.ApiMethod;

/** Callback for {@link NettyClient}. */
public interface ClientHandlerCallback {

    /**
     * Gets the {@link ApiMethod} corresponding to the given request ID. Returns the request has
     * been expired or failed.
     */
    ApiMethod getRequestApiMethod(int requestId);

    /**
     * Called on a successful request.
     *
     * @param requestId ID of the request
     * @param response The received response
     */
    void onRequestResult(int requestId, ApiMessage response);

    /**
     * Called on a failed request.
     *
     * @param requestId ID of the request
     * @param cause Cause of the request failure
     */
    void onRequestFailure(int requestId, Throwable cause);

    /**
     * Called on any failure, which is not related to a specific request.
     *
     * <p>This can be for example a caught Exception in the channel pipeline or an unexpected
     * channel close.
     *
     * @param cause Cause of the failure
     */
    void onFailure(Throwable cause);
}
