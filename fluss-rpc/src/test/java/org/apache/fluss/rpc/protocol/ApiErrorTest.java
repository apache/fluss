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

import org.apache.fluss.exception.NotEnoughReplicasException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.exception.UnknownTableOrBucketException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.fluss.rpc.protocol.ApiError}. */
public class ApiErrorTest {
    @ParameterizedTest
    @MethodSource("parameters")
    public void fromThrowableShouldReturnCorrectError(
            Throwable t, Errors expectedError, String expectedMsg) {
        ApiError apiError = ApiError.fromThrowable(t);
        assertThat(apiError.error()).isEqualTo(expectedError);
        assertThat(apiError.message()).isEqualTo(expectedMsg);
    }

    @Test
    public void testStringifyException() {
        ApiError apiError = ApiError.fromThrowable(new IOException());
        assertThat(apiError.error()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR);
        // test the error message contains exception stack
        assertThat(apiError.message()).contains("java.io.IOException\n\tat");
    }

    private static Collection<Arguments> parameters() {
        List<Arguments> arguments = new ArrayList<>();

        String notEnoughReplicasErrorMsg = "Not enough replicas to write to partition.";
        arguments.add(
                Arguments.of(
                        new NotEnoughReplicasException(notEnoughReplicasErrorMsg),
                        Errors.NOT_ENOUGH_REPLICAS_EXCEPTION,
                        notEnoughReplicasErrorMsg));

        // avoid populating the error message if it's a generic one
        arguments.add(
                Arguments.of(
                        new UnknownTableOrBucketException(
                                Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION.message()),
                        Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION,
                        null));

        String requestTimeoutErrorMsg = "request time out";
        // test the TimeoutException is wrapped in the ExecutionException,
        // should return correct error
        arguments.add(
                Arguments.of(
                        new ExecutionException(new TimeoutException(requestTimeoutErrorMsg)),
                        Errors.REQUEST_TIME_OUT,
                        requestTimeoutErrorMsg));

        return arguments;
    }
}
