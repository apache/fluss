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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.protocol.ApiMethod;
import org.apache.fluss.rpc.protocol.RequestType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.ExceptionUtils.stripException;

/** A handler that processes and answers incoming {@link FlussRequest}. */
public class FlussRequestHandler implements RequestHandler<FlussRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(FlussRequestHandler.class);

    private final RpcGatewayService service;

    public FlussRequestHandler(RpcGatewayService service) {
        this.service = service;
    }

    @Override
    public RequestType requestType() {
        return RequestType.FLUSS;
    }

    @Override
    public void processRequest(FlussRequest request) {
        request.setRequestDequeTimeMs(System.currentTimeMillis());
        ApiMethod api = request.getApiMethod();
        ApiMessage message = request.getMessage();
        try {
            service.setCurrentSession(
                    new Session(
                            request.getApiVersion(),
                            request.getListenerName(),
                            request.isInternal(),
                            request.getAddress(),
                            request.getPrincipal()));
            // invoke the corresponding method on RpcGateway instance.
            CompletableFuture<?> responseFuture =
                    (CompletableFuture<?>) api.getMethod().invoke(service, message);
            responseFuture.whenComplete(
                    (response, throwable) -> {
                        request.setRequestCompletedTimeMs(System.currentTimeMillis());
                        if (throwable != null) {
                            request.fail(throwable);
                        } else {
                            if (response instanceof ApiMessage) {
                                request.complete((ApiMessage) response);
                            } else {
                                request.fail(
                                        new ClassCastException(
                                                "The response "
                                                        + response.getClass().getName()
                                                        + " is not an instance of ApiMessage."));
                            }
                        }
                    });
        } catch (Throwable t) {
            LOG.debug("Error while executing RPC {}", api, t);
            request.fail(stripException(t, InvocationTargetException.class));
        }
    }
}
