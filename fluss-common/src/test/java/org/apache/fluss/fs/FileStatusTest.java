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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for default methods of {@link FileStatus}. */
class FileStatusTest {

    /**
     * An implementation that does not override {@link FileStatus#getModificationTime()} must
     * inherit the fail-safe default of {@link Long#MAX_VALUE}, so time-based filters treat the file
     * as "always fresh" and never delete it when modification time is unavailable.
     */
    @Test
    void defaultModificationTimeIsMaxValueFailSafe() {
        FileStatus status =
                new FileStatus() {
                    @Override
                    public long getLen() {
                        return 0L;
                    }

                    @Override
                    public boolean isDir() {
                        return false;
                    }

                    @Override
                    public FsPath getPath() {
                        return new FsPath("/tmp/x");
                    }
                };

        assertThat(status.getModificationTime()).isEqualTo(Long.MAX_VALUE);
    }
}
