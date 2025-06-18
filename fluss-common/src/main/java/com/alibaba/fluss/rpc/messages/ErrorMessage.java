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

package com.alibaba.fluss.rpc.messages;

import javax.annotation.Nullable;

/**
 * A message interface that generated proto class can optional to extend this. This interface
 * provides some useful methods, such as setters and getters of error code and error message. This
 * requires the proto object has the following fields:
 *
 * <ul>
 *   <li>[optional|required] int32 error_code = ?;
 *   <li>[optional|required] string error_message = ?;
 * </ul>
 */
public interface ErrorMessage extends ApiMessage {

    /** A shortcut to set error code and nullable error message. */
    default void setError(int errorCode, @Nullable String errorMessage) {
        setErrorCode(errorCode);
        if (errorMessage != null) {
            setErrorMessage(errorMessage);
        }
    }

    /** Returns whether the error code is set. */
    boolean hasErrorCode();

    /**
     * Gets the error code of the message.
     *
     * @throws IllegalStateException if the error code is not set.
     */
    int getErrorCode();

    /** Sets the error code. */
    ErrorMessage setErrorCode(int errorCode);

    /** Clears the error code. */
    ErrorMessage clearErrorCode();

    /** Returns whether the error message is set. */
    boolean hasErrorMessage();

    /**
     * Gets the error message of the message.
     *
     * @throws IllegalStateException if the error message is not set.
     */
    String getErrorMessage();

    /** Sets the error message, the error message shouldn't be null. */
    ErrorMessage setErrorMessage(String errorMessage);

    /** Clears the error message. */
    ErrorMessage clearErrorMessage();
}
