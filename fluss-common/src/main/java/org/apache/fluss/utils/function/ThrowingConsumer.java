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

package org.apache.fluss.utils.function;

import org.apache.fluss.annotation.PublicStable;
import org.apache.fluss.utils.ExceptionUtils;

import java.util.function.Consumer;

/**
 * This interface is basically Java's {@link java.util.function.Consumer} interface enhanced with
 * the ability to throw an exception.
 *
 * @param <T> type of the consumed elements.
 * @param <E> type of the exception thrown.
 * @since 0.1
 */
@PublicStable
@FunctionalInterface
public interface ThrowingConsumer<T, E extends Throwable> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     * @throws E on errors during consumption
     */
    void accept(T t) throws E;

    /**
     * Converts a {@link ThrowingConsumer} into a {@link Consumer} which throws all checked
     * exceptions as unchecked.
     *
     * @param throwingConsumer to convert into a {@link Consumer}
     * @return {@link Consumer} which throws all checked exceptions as unchecked.
     */
    static <T, E extends Throwable> Consumer<T> unchecked(ThrowingConsumer<T, E> throwingConsumer) {
        return t -> {
            try {
                throwingConsumer.accept(t);
            } catch (Throwable e) {
                ExceptionUtils.rethrow(e);
            }
        };
    }
}
