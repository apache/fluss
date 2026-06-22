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

package org.apache.fluss.lake.hudi.utils.meta;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CkpMetadata}. */
class CkpMetadataTest {

    @TempDir private Path tempDir;

    @Test
    void testAbortedInstantIsNotPending() throws Exception {
        FileSystem fs = FileSystem.getLocal(new org.apache.hadoop.conf.Configuration());
        CkpMetadata ckpMetadata = new CkpMetadata(fs, tempDir.toString(), "");

        ckpMetadata.bootstrap();
        ckpMetadata.startInstant("20260622000100000");
        assertThat(ckpMetadata.lastPendingInstant()).isEqualTo("20260622000100000");

        ckpMetadata.abortInstant("20260622000100000");
        assertThat(ckpMetadata.lastPendingInstant()).isNull();
    }
}
