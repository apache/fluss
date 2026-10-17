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

package org.apache.fluss.fs;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.io.IOException;

/** A normalized filesystem failure with the original exception retained as its cause. */
@Internal
public final class FileSystemOperationException extends IOException implements FileSystemFailure {

    private final Kind kind;
    private final Resource resource;
    private final String operation;
    private final boolean temporary;
    private final @Nullable String serviceCode;
    private final @Nullable String requestId;

    /** Creates a normalized filesystem failure. */
    public FileSystemOperationException(
            Kind kind,
            Resource resource,
            String operation,
            boolean temporary,
            @Nullable String serviceCode,
            @Nullable String requestId,
            Exception cause) {
        super("Filesystem " + operation + " failed: " + kind, cause);
        this.kind = kind;
        this.resource = resource;
        this.operation = operation;
        this.temporary = temporary;
        this.serviceCode = serviceCode;
        this.requestId = requestId;
    }

    @Override
    public Kind kind() {
        return kind;
    }

    @Override
    public Resource resource() {
        return resource;
    }

    @Override
    public String operation() {
        return operation;
    }

    @Override
    public boolean isTemporary() {
        return temporary;
    }

    @Override
    public String serviceCode() {
        return serviceCode;
    }

    @Override
    public String requestId() {
        return requestId;
    }
}
