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

package org.apache.fluss.exception;

/**
 * Any API exception that is part of the public protocol and should be a subclass of this class and
 * be part of this package.
 */
public class ApiException extends FlussRuntimeException {

    private static final long serialVersionUID = 1L;

    private final boolean stackTraceEnabled;

    public ApiException(String message, Throwable cause, boolean stackTraceEnabled) {
        super(message, cause);
        this.stackTraceEnabled = stackTraceEnabled;
    }

    public ApiException(String message, Throwable cause) {
        super(message, cause);
        this.stackTraceEnabled = false;
    }

    public ApiException(String message) {
        super(message);
        this.stackTraceEnabled = false;
    }

    public ApiException(Throwable cause) {
        super(cause);
        this.stackTraceEnabled = false;
    }

    /* avoid the expensive and useless stack trace for api exceptions */
    @Override
    public Throwable fillInStackTrace() {
        if (stackTraceEnabled) {
            return super.fillInStackTrace();
        } else {
            return this;
        }
    }
}
