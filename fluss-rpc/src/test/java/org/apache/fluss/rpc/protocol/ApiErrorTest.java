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

import org.apache.fluss.exception.HistoricalPartitionThrottledException;
import org.apache.fluss.exception.InvalidBucketRoutingException;
import org.apache.fluss.exception.NotEnoughReplicasException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.exception.UnknownServerException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.rpc.messages.ErrorResponse;

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

    private static final int UNRECOGNIZED_ERROR_CODE = 9999;

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

    @Test
    public void testFromErrorMessageWithoutErrorCode() {
        ApiError apiError = ApiError.fromErrorMessage(new ErrorResponse().setErrorMessage("boom"));
        assertThat(apiError.isSuccess()).isTrue();
        assertThat(apiError.error()).isEqualTo(Errors.NONE);
        assertThat(apiError.message()).isEqualTo("boom");
    }

    @Test
    public void testFromErrorMessageKnownCode() {
        ApiError apiError =
                ApiError.fromErrorMessage(
                        new ErrorResponse()
                                .setErrorCode(Errors.TABLE_NOT_EXIST.code())
                                .setErrorMessage("Table db1.t1 does not exist"));
        assertThat(apiError.error()).isEqualTo(Errors.TABLE_NOT_EXIST);
        assertThat(apiError.message()).isEqualTo("Table db1.t1 does not exist");
    }

    @Test
    public void testFromErrorMessageKeepsGenuineUnknownServerErrorUntouched() {
        // a real -1 without a message is legitimate: the server sets the message to null
        // when the exception text equals the default one, and it must not be rewritten
        ApiError apiError =
                ApiError.fromErrorMessage(
                        new ErrorResponse().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()));
        assertThat(apiError.error()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR);
        assertThat(apiError.message()).isNull();
    }

    @Test
    public void testFromErrorMessageKeepsUnknownErrorCode() {
        ApiError apiError =
                ApiError.fromErrorMessage(
                        new ErrorResponse().setErrorCode(UNRECOGNIZED_ERROR_CODE));
        assertThat(apiError.error()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR);
        assertThat(apiError.message()).isEqualTo("Unknown error code 9999 from server.");
        assertThat(apiError.exception()).isInstanceOf(UnknownServerException.class);
    }

    @Test
    public void testFromErrorMessageKeepsUnknownErrorCodeWithProvidedMessage() {
        ApiError apiError =
                ApiError.fromErrorMessage(
                        new ErrorResponse()
                                .setErrorCode(UNRECOGNIZED_ERROR_CODE)
                                .setErrorMessage("boom"));
        assertThat(apiError.error()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR);
        assertThat(apiError.message()).isEqualTo("Unknown error code 9999 from server: boom");
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

        String historicalLookupThrottledErrorMsg = "historical lookup throttled";
        arguments.add(
                Arguments.of(
                        new HistoricalPartitionThrottledException(
                                historicalLookupThrottledErrorMsg),
                        Errors.HISTORICAL_PARTITION_THROTTLED,
                        historicalLookupThrottledErrorMsg));

        String invalidBucketRoutingErrorMsg = "invalid bucket routing";
        arguments.add(
                Arguments.of(
                        new InvalidBucketRoutingException(invalidBucketRoutingErrorMsg),
                        Errors.INVALID_BUCKET_ROUTING,
                        invalidBucketRoutingErrorMsg));

        return arguments;
    }
}
