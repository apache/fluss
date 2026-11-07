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

import java.io.FileNotFoundException;

/** A confirmed missing path, retaining compatibility with {@link FileNotFoundException}. */
@Internal
public final class FileSystemPathNotFoundException extends FileNotFoundException
        implements FileSystemFailure {

    private final String operation;
    private final @Nullable String serviceCode;
    private final @Nullable String requestId;

    /** Creates an exception for a missing target path. */
    public FileSystemPathNotFoundException(
            String operation,
            @Nullable String serviceCode,
            @Nullable String requestId,
            Exception cause) {
        super("Filesystem " + operation + " target path does not exist");
        this.operation = operation;
        this.serviceCode = serviceCode;
        this.requestId = requestId;
        initCause(cause);
    }

    @Override
    public Kind kind() {
        return Kind.NOT_FOUND;
    }

    @Override
    public Resource resource() {
        return Resource.PATH;
    }

    @Override
    public String operation() {
        return operation;
    }

    @Override
    public boolean isTemporary() {
        return false;
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
