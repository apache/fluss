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

import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.exception.StaleMetadataException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CompletionException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Errors}. */
class ErrorsTest {

    @Test
    void testErrorCodesAreContinuous() {
        int[] codes = Arrays.stream(Errors.values()).mapToInt(Errors::code).sorted().toArray();
        // Include UNKNOWN_SERVER_ERROR (-1) and NONE (0).
        assertThat(codes).containsExactly(IntStream.range(-1, -1 + codes.length).toArray());
    }

    @Test
    void testBulkLoadErrorMappingsRoundTrip() {
        assertError(
                Errors.BULK_LOAD_NOT_FOUND,
                81,
                BulkLoadNotFoundException.class,
                new BulkLoadNotFoundException("reverse-not-found"));
        assertError(
                Errors.INVALID_BULK_LOAD_REQUEST,
                82,
                InvalidBulkLoadRequestException.class,
                new InvalidBulkLoadRequestException("reverse-invalid"));
    }

    @Test
    void testStaleMetadataRemainsInternal() {
        assertThat(Errors.forException(new StaleMetadataException("stale")))
                .isSameAs(Errors.UNKNOWN_SERVER_ERROR);
    }

    private static void assertError(
            Errors error,
            int code,
            Class<? extends ApiException> exceptionClass,
            ApiException reverseLookupInstance) {
        assertThat(error.code()).isEqualTo(code);
        assertThat(Errors.forCode(code)).isSameAs(error);
        assertThat(error.exception()).isExactlyInstanceOf(exceptionClass);
        assertThat(error.exception()).isNotInstanceOf(RetriableException.class);
        assertThat(error.exception().getClass().getSuperclass()).isEqualTo(ApiException.class);

        String customMessage = "custom-" + code;
        assertThat(error.exception(customMessage))
                .isExactlyInstanceOf(exceptionClass)
                .hasMessage(customMessage);
        assertThat(Errors.forException(reverseLookupInstance)).isSameAs(error);
        assertThat(Errors.forException(new CompletionException(reverseLookupInstance)))
                .isSameAs(error);
    }
}
