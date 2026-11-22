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

package org.apache.fluss.flink.action.orphan;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.flink.action.orphan.RpcErrorClassifier.Category.NOT_FOUND;
import static org.apache.fluss.flink.action.orphan.RpcErrorClassifier.Category.SERVER_ERROR;
import static org.apache.fluss.flink.action.orphan.RpcErrorClassifier.Category.TRANSIENT;
import static org.apache.fluss.flink.action.orphan.RpcErrorClassifier.Category.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;

class RpcErrorClassifierTest {

    @Test
    void tableNotExistIsNotFound() {
        assertThat(RpcErrorClassifier.classify(new TableNotExistException("x")))
                .isEqualTo(NOT_FOUND);
    }

    @Test
    void partitionNotExistIsNotFound() {
        assertThat(RpcErrorClassifier.classify(new PartitionNotExistException("x")))
                .isEqualTo(NOT_FOUND);
    }

    @Test
    void ioExceptionIsTransient() {
        assertThat(RpcErrorClassifier.classify(new IOException("conn reset"))).isEqualTo(TRANSIENT);
    }

    @Test
    void timeoutIsTransient() {
        assertThat(RpcErrorClassifier.classify(new TimeoutException("rpc"))).isEqualTo(TRANSIENT);
    }

    @Test
    void unwrapsCompletionException() {
        assertThat(
                        RpcErrorClassifier.classify(
                                new CompletionException(new TableNotExistException("x"))))
                .isEqualTo(NOT_FOUND);
    }

    @Test
    void flussServerErrorIsServerError() {
        assertThat(RpcErrorClassifier.classify(new FlussRuntimeException("internal")))
                .isEqualTo(SERVER_ERROR);
    }

    @Test
    void otherRuntimeIsUnknown() {
        assertThat(RpcErrorClassifier.classify(new IllegalStateException("?"))).isEqualTo(UNKNOWN);
    }
}
