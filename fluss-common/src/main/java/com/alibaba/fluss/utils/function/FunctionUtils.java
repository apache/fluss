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

import com.alibaba.fluss.utils.ExceptionUtils;

import java.util.function.Consumer;
import java.util.function.Function;

/** Utility class for Fluss's functions. */
public class FunctionUtils {

    private static final Consumer<Object> IGNORE_FN = ignored -> {};

    private FunctionUtils() {
        throw new UnsupportedOperationException("This class should never be instantiated.");
    }

    /**
     * Consumer which ignores the input.
     *
     * @param <T> type of the input
     * @return Ignoring {@link Consumer}
     */
    @SuppressWarnings("unchecked")
    public static <T> Consumer<T> ignoreFn() {
        return (Consumer<T>) IGNORE_FN;
    }

    /**
     * Convert at {@link FunctionWithException} into a {@link Function}.
     *
     * @param functionWithException function with exception to convert into a function
     * @param <A> input type
     * @param <B> output type
     * @return {@link Function} which throws all checked exception as an unchecked exception.
     */
    public static <A, B> Function<A, B> uncheckedFunction(
            FunctionWithException<A, B, ?> functionWithException) {
        return (A value) -> {
            try {
                return functionWithException.apply(value);
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
                // we need this to appease the compiler :-(
                return null;
            }
        };
    }
}
