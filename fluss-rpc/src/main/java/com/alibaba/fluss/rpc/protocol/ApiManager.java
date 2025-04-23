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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.rpc.RpcGateway;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.shaded.guava32.com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** A manager manages the supported RPC API. */
public class ApiManager {

    private static final Logger LOG = LoggerFactory.getLogger(ApiManager.class);

    static final Map<Short, ApiMethod> ID_TO_API = new HashMap<>();
    static final Map<String, ApiMethod> NAME_TO_API;

    static {
        try {
            registerApiMethods(CoordinatorGateway.class, ServerType.COORDINATOR, ID_TO_API);
            registerApiMethods(TabletServerGateway.class, ServerType.TABLET_SERVER, ID_TO_API);
            NAME_TO_API =
                    ID_TO_API.values().stream()
                            .collect(Collectors.toMap(ApiMethod::getMethodName, m -> m));
        } catch (Exception e) {
            LOG.error("Failed to register RPC API methods.", e);
            throw e;
        }
    }

    public static ApiMethod forApiKey(short apiKey) {
        return ID_TO_API.get(apiKey);
    }

    public static ApiMethod forMethodName(String methodName) {
        return NAME_TO_API.get(methodName);
    }

    private final ServerType providerType;
    private final Map<Short, ApiMethod> id2Api;
    private final EnumSet<ApiKeys> enabledApis;

    public ApiManager(ServerType providerType) {
        this.providerType = providerType;
        if (providerType == ServerType.COORDINATOR) {
            this.id2Api =
                    ID_TO_API.entrySet().stream()
                            .filter(e -> e.getValue().inScope(ServerType.COORDINATOR))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } else {
            this.id2Api =
                    ID_TO_API.entrySet().stream()
                            .filter(e -> e.getValue().inScope(ServerType.TABLET_SERVER))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        this.enabledApis =
                EnumSet.copyOf(
                        id2Api.values().stream()
                                .map(ApiMethod::getApiKey)
                                .collect(Collectors.toList()));
    }

    public ServerType getProviderType() {
        return providerType;
    }

    public ApiMethod getApi(short apiKey) {
        return id2Api.get(apiKey);
    }

    public Set<ApiKeys> enabledApis() {
        return enabledApis;
    }

    @VisibleForTesting
    public void registerApiMethod(ApiKeys api, Method method, ServerType providerType) {
        id2Api.put(api.id, createApiMethod(api, method, providerType));
        enabledApis.addAll(
                id2Api.values().stream().map(ApiMethod::getApiKey).collect(Collectors.toList()));
    }

    // ----------------------------------------------------------------------------------------
    // Internal Utilities
    // ----------------------------------------------------------------------------------------

    @VisibleForTesting
    static void registerApiMethods(
            Class<? extends RpcGateway> gatewayClass,
            ServerType providerType,
            Map<Short, ApiMethod> registeredMethods) {
        for (Method method : gatewayClass.getMethods()) {
            RPC annotation = method.getAnnotation(RPC.class);
            if (annotation != null) {
                ApiKeys api = annotation.api();
                ApiMethod existing = registeredMethods.get(api.id);
                if (existing != null) {
                    if (existing.getMethod().equals(method) && !existing.inScope(providerType)) {
                        // the same RPC method is supported by different providers
                        registeredMethods.put(api.id, existing.copyAndAddProvider(providerType));
                    } else {
                        throw new IllegalStateException(
                                String.format(
                                        "Different RPC methods are registered with the same API key [%s].\n%s\n%s",
                                        api, existing.getMethod(), method));
                    }
                } else {
                    registeredMethods.put(api.id, createApiMethod(api, method, providerType));
                }
            }
        }
    }

    private static ApiMethod createApiMethod(ApiKeys api, Method method, ServerType providerType) {
        Class<?> requestClass = extractRequestClass(method);
        Class<?> responseClass = extractResponseClass(method);
        checkMethodNaming(
                method.getName(), requestClass.getSimpleName(), responseClass.getSimpleName());
        return new ApiMethod(
                api, requestClass, responseClass, method, ImmutableList.of(providerType));
    }

    private static Class<?> extractRequestClass(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 1 && ApiMessage.class.isAssignableFrom(parameterTypes[0])) {
            return parameterTypes[0];
        }
        throw new IllegalStateException(
                "RPC method ["
                        + method
                        + "] must have exactly one parameter of type "
                        + ApiMessage.class.getSimpleName());
    }

    private static Class<?> extractResponseClass(Method method) {
        Class<?> returnClass = method.getReturnType();
        if (CompletableFuture.class.equals(returnClass)) {
            Type returnType = method.getGenericReturnType();
            if (returnType instanceof ParameterizedType) {
                Type responseType = ((ParameterizedType) returnType).getActualTypeArguments()[0];
                if (responseType instanceof Class
                        && ApiMessage.class.isAssignableFrom((Class<?>) responseType)) {
                    return (Class<?>) responseType;
                }
            }
        }
        throw new IllegalStateException(
                "RPC method ["
                        + method
                        + "] must have a return type of CompletableFuture<T extends ApiMessage>");
    }

    private static void checkMethodNaming(
            String methodName, String requestName, String responseName) {
        String apiName = firstCharToUpper(methodName);
        String expectedRequestName = apiName + "Request";
        if (!expectedRequestName.equals(requestName)) {
            throw new IllegalStateException(
                    "RPC method ["
                            + methodName
                            + "] expects to have a request parameter of type "
                            + expectedRequestName
                            + ", but is "
                            + requestName);
        }

        String expectedResponseName = apiName + "Response";
        if (!expectedResponseName.equals(responseName)) {
            throw new IllegalStateException(
                    "RPC method ["
                            + methodName
                            + "] expects to have a response return class of type "
                            + expectedResponseName
                            + ", but is "
                            + responseName);
        }
    }

    private static String firstCharToUpper(String str) {
        return Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }
}
