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
import org.apache.fluss.rpc.messages.ErrorMessage;
import org.apache.fluss.rpc.messages.ErrorResponse;
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Encapsulates an error code (via the Errors enum) and an optional message. Generally, the optional
 * message is only defined if it adds information over the default message associated with the error
 * code.
 */
public class ApiError {

    private static final int MAX_ERROR_MESSAGE_LENGTH = 2048;
    public static final ApiError NONE = Errors.NONE.toApiError();

    private final Errors error;
    private final @Nullable String message;

    public static ApiError fromThrowable(Throwable t) {
        Throwable throwableToBeEncoded = Errors.maybeUnwrapException(t);
        Errors error = Errors.forException(throwableToBeEncoded);
        final String message;
        if (Objects.equals(error.message(), throwableToBeEncoded.getMessage())) {
            message = null;
        } else if (error.code() == Errors.UNKNOWN_SERVER_ERROR.code()) {
            // we populate error stack message for UNKNOWN_SERVER_ERROR for easy debugging,
            // but we may need to avoid this to not leak sensitive information in the future.
            String errorStack = ExceptionUtils.stringifyException(throwableToBeEncoded);
            // tailor the error stack to reduce the network cost.
            message =
                    errorStack.length() > MAX_ERROR_MESSAGE_LENGTH
                            ? errorStack.substring(0, MAX_ERROR_MESSAGE_LENGTH)
                            : errorStack;
        } else {
            message = throwableToBeEncoded.getMessage();
        }
        return new ApiError(error, message);
    }

    public static ApiError fromErrorMessage(ErrorMessage msg) {
        Errors code = msg.hasErrorCode() ? Errors.forCode(msg.getErrorCode()) : Errors.NONE;
        String message = msg.hasErrorMessage() ? msg.getErrorMessage() : null;
        return new ApiError(code, message);
    }

    public ApiError(Errors error, @Nullable String message) {
        this.error = error;
        this.message = message;
    }

    public boolean isFailure() {
        return !isSuccess();
    }

    public boolean isSuccess() {
        return this.error == Errors.NONE;
    }

    public Errors error() {
        return error;
    }

    /**
     * Return the associated optional error message or null.
     *
     * <p>Note: the returned message can be null and is useful for transport to reduce unnecessary
     * network cost.
     */
    @Nullable
    public String message() {
        return message;
    }

    /**
     * If {@link #message} is defined, return it. Otherwise, fallback to the default error message
     * (convert to "NONE" if default message is null) associated with the error code.
     *
     * <p>Note: the returned message is never null and is useful for logging with more information.
     */
    public String messageWithFallback() {
        if (message == null) {
            String defaultMsg = error.message();
            return defaultMsg == null ? "NONE" : defaultMsg;
        }
        return message;
    }

    public ApiException exception() {
        return error.exception(message);
    }

    public ErrorResponse toErrorResponse() {
        ErrorResponse resp = new ErrorResponse().setErrorCode(error.code());
        if (message != null) {
            resp.setErrorMessage(message);
        }
        return resp;
    }

    /**
     * Format the error from a get request in a user-friendly string.
     *
     * <p>e.g "NETWORK_EXCEPTION. Error Message: Disconnected from node 0"
     */
    public String formatErrMsg() {
        if (message == null || message.isEmpty()) {
            return error.toString();
        } else {
            return String.format("%s. Error Message: %s", error, message);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(error, message);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ApiError)) {
            return false;
        }
        ApiError other = (ApiError) o;
        return Objects.equals(error, other.error) && Objects.equals(message, other.message);
    }

    @Override
    public String toString() {
        return "ApiError(error=" + error + ", message=" + message + ")";
    }
}
