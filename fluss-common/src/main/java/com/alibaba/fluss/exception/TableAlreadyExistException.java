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

package com.alibaba.fluss.exception;

import com.alibaba.fluss.annotation.PublicEvolving;

/**
 * Exception for trying to create a table that already exists.
 *
 * @since 0.1
 */
@PublicEvolving
public class TableAlreadyExistException extends ApiException {

    public TableAlreadyExistException(String message) {
        this(message, null);
    }

    public TableAlreadyExistException(String message, Throwable cause) {
        super(message, cause);
    }
}
