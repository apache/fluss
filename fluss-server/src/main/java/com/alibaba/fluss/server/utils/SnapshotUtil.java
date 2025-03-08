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

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.server.kv.snapshot.KvFileHandle;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helpers for snapshot related code. */
public class SnapshotUtil {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotUtil.class);

    /**
     * Iterates through the passed kv file handles and calls discard() on each handle that is not
     * null. All occurring exceptions are suppressed and collected until the iteration is over and
     * emitted as a single exception.
     *
     * @param handlesToDiscard Kv file handles to discard. Passed iterable is allowed to deliver
     *     null values.
     * @throws Exception exception that is a collection of all suppressed exceptions that were
     *     caught during iteration
     */
    public static void bestEffortDiscardAllKvFiles(
            Iterable<? extends KvFileHandle> handlesToDiscard) throws Exception {
        applyToAllWhileSuppressingExceptions(handlesToDiscard, KvFileHandle::discard);
    }

    public static void discardKvFileQuietly(KvFileHandle kvFileHandle) {
        if (kvFileHandle == null) {
            return;
        }
        try {
            kvFileHandle.discard();
        } catch (Exception exception) {
            LOG.warn("Discard {} exception.", kvFileHandle, exception);
        }
    }

    /**
     * This method supplies all elements from the input to the consumer. Exceptions that happen on
     * elements are suppressed until all elements are processed. If exceptions happened for one or
     * more of the inputs, they are reported in a combining suppressed exception.
     *
     * @param inputs iterator for all inputs to the throwingConsumer.
     * @param throwingConsumer this consumer will be called for all elements delivered by the input
     *     iterator.
     * @param <T> the type of input.
     * @throws Exception collected exceptions that happened during the invocation of the consumer on
     *     the input elements.
     */
    public static <T> void applyToAllWhileSuppressingExceptions(
            Iterable<T> inputs, ThrowingConsumer<T, ? extends Exception> throwingConsumer)
            throws Exception {

        if (inputs != null && throwingConsumer != null) {
            Exception exception = null;

            for (T input : inputs) {

                if (input != null) {
                    try {
                        throwingConsumer.accept(input);
                    } catch (Exception ex) {
                        exception = ExceptionUtils.firstOrSuppressed(ex, exception);
                    }
                }
            }

            if (exception != null) {
                throw exception;
            }
        }
    }
}
