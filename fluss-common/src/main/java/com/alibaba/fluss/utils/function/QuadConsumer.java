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

package com.alibaba.fluss.utils.function;

import com.alibaba.fluss.annotation.PublicEvolving;

/**
 * Operation which is performed on four given arguments.
 *
 * @param <S> type of the first argument
 * @param <T> type of the second argument
 * @param <U> type of the third argument
 * @param <V> type of the fourth argument
 * @since 0.2
 */
@PublicEvolving
@FunctionalInterface
public interface QuadConsumer<S, T, U, V> {

    /**
     * Performs this operation on the given arguments.
     *
     * @param s first argument
     * @param t second argument
     * @param u third argument
     * @param v fourth argument
     */
    void accept(S s, T t, U u, V v);
}
