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

/** Stable filesystem error semantics independent of the underlying storage implementation. */
@Internal
public interface FileSystemFailure {

    /** Kind of failure seen by a filesystem caller. */
    enum Kind {
        NOT_FOUND,
        PERMISSION_DENIED,
        RATE_LIMITED,
        UNSUPPORTED,
        UNEXPECTED
    }

    /** Resource whose absence was established by the filesystem adapter. */
    enum Resource {
        /** The requested file or directory. */
        PATH,
        /** The root of the filesystem namespace containing the requested path. */
        ROOT,
        /** The failed resource could not be established. */
        UNKNOWN
    }

    /** Returns the stable failure kind. */
    Kind kind();

    /** Returns the resource whose absence was established. */
    Resource resource();

    /** Returns the failed filesystem operation. */
    String operation();

    /** Whether the failure may be transient; callers must assess whether retrying is safe. */
    boolean isTemporary();

    /** Returns the native service code when the adapter could extract one. */
    @Nullable
    String serviceCode();

    /** Returns the native request identifier when the adapter could extract one. */
    @Nullable
    String requestId();
}
