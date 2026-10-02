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

import org.apache.fluss.annotation.PublicStable;

/**
 * Interface that represents the client side information for a file independent of the file system.
 *
 * @since 0.1
 */
@PublicStable
public interface FileStatus {
    /**
     * Return the length of this file.
     *
     * @return the length of this file
     */
    long getLen();

    /**
     * Checks if this object represents a directory.
     *
     * @return <code>true</code> if this is a directory, <code>false</code> otherwise
     */
    boolean isDir();

    /**
     * Returns the corresponding Path to the FileStatus.
     *
     * @return the corresponding Path to the FileStatus
     */
    FsPath getPath();

    /**
     * Returns the modification time of the file in milliseconds since the epoch.
     *
     * <p>The default implementation returns {@link Long#MAX_VALUE}, which is interpreted by
     * time-based filters (e.g. orphan-files cleanup) as "always fresh" - effectively a fail-closed
     * default that prevents deletion when modification time is unavailable. File system
     * implementations that can expose modification time SHOULD override this.
     *
     * @return the modification time in epoch millis, or {@link Long#MAX_VALUE} when unavailable
     */
    default long getModificationTime() {
        return Long.MAX_VALUE;
    }
}
