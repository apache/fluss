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

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;
import org.apache.fluss.utils.InstantiationUtils;

import javax.annotation.concurrent.ThreadSafe;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** Describes an RPC API method to be invoked. */
@ThreadSafe
public class ApiMethod {
    /** The API key of the RPC API. */
    private final ApiKeys apiKey;

    /** The class of the request message. */
    private final Class<?> requestClass;

    /** The class of the response message. */
    private final Class<?> responseClass;

    /** The constructor of the request message. */
    private final Supplier<ApiMessage> requestConstructor;

    /** The constructor of the response message. */
    private final Supplier<ApiMessage> responseConstructor;

    /** The RPC method to be invoked. */
    final Method method;

    /** The providers of the RPC API. */
    final List<ServerType> providers;

    ApiMethod(
            ApiKeys apiKey,
            Class<?> requestClass,
            Class<?> responseClass,
            Method method,
            ImmutableList<ServerType> providers) {
        this.apiKey = apiKey;
        this.requestClass = requestClass;
        this.responseClass = responseClass;
        this.requestConstructor = new ApiMessageConstructor(requestClass);
        this.responseConstructor = new ApiMessageConstructor(responseClass);
        this.method = method;
        this.providers = providers;
    }

    public boolean inScope(ServerType provider) {
        return providers.contains(provider);
    }

    public ApiMethod copyAndAddProvider(ServerType additional) {
        List<ServerType> tmp = new ArrayList<>();
        tmp.add(additional);
        tmp.addAll(providers);
        return new ApiMethod(
                apiKey, requestClass, responseClass, method, ImmutableList.copyOf(tmp));
    }

    public ApiKeys getApiKey() {
        return apiKey;
    }

    public Supplier<ApiMessage> getRequestConstructor() {
        return requestConstructor;
    }

    public Supplier<ApiMessage> getResponseConstructor() {
        return responseConstructor;
    }

    public Method getMethod() {
        return method;
    }

    public String getMethodName() {
        return method.getName();
    }

    @Override
    public String toString() {
        return apiKey.toString();
    }

    // ------------------------------------------------------------------------------------------

    private static class ApiMessageConstructor implements Supplier<ApiMessage> {
        private final Class<?> messageClass;

        private ApiMessageConstructor(Class<?> messageClass) {
            this.messageClass = messageClass;
        }

        @Override
        public ApiMessage get() {
            return (ApiMessage) InstantiationUtils.instantiate(messageClass);
        }
    }
}
