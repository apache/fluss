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

import org.apache.fluss.annotation.Internal;

/**
 * This exception is raised by the tablet server if it could not locate the writer metadata
 * associated with the writer in question. This could happen if, for instance, the writer's records
 * were deleted because their retention time had elapsed. Once the last records of the writer id are
 * removed, the writer's metadata is removed from the tablet server, and future appends by the
 * writer will return this exception.
 */
@Internal
public class UnknownWriterIdException extends ApiException {
    public UnknownWriterIdException(String message) {
        super(message);
    }
}
