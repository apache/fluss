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

package com.alibaba.fluss.server.coordinator.event;

import com.alibaba.fluss.server.coordinator.CoordinatorContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An event designed to safely access the {@link CoordinatorContext}. Since {@link
 * CoordinatorContext} is not thread-safe, directly accessing it in tests can lead to unsafe
 * operations. This event ensures safe access to the {@link CoordinatorContext}.
 *
 * @param <T> the type of the result of the access operation
 */
public class AccessContextEvent<T> implements CoordinatorEvent {

    private final Function<CoordinatorContext, T> accessFunction;
    private final CompletableFuture<T> resultFuture;

    public AccessContextEvent(Function<CoordinatorContext, T> accessFunction) {
        this.accessFunction = accessFunction;
        this.resultFuture = new CompletableFuture<>();
    }

    public Function<CoordinatorContext, T> getAccessFunction() {
        return accessFunction;
    }

    public CompletableFuture<T> getResultFuture() {
        return resultFuture;
    }
}
