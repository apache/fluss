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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.fs.FsPath;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test the {@code equals}/{@code hashCode} contract of {@link RemoteLogManifestHandle}. */
class RemoteLogManifestHandleTest {

    private static final String PATH = "/tmp/remote/log/manifest";

    @Test
    void testEqualsAndHashCode() {
        RemoteLogManifestHandle handle1 = new RemoteLogManifestHandle(new FsPath(PATH), 100L);
        RemoteLogManifestHandle handle2 = new RemoteLogManifestHandle(new FsPath(PATH), 100L);

        assertThat(handle1).isEqualTo(handle2);
        assertThat(handle1).hasSameHashCodeAs(handle2);
    }

    @Test
    void testHashCodeDiffersForDifferentEndOffset() {
        RemoteLogManifestHandle handle1 = new RemoteLogManifestHandle(new FsPath(PATH), 100L);
        RemoteLogManifestHandle handle2 = new RemoteLogManifestHandle(new FsPath(PATH), 101L);

        assertThat(handle1).isNotEqualTo(handle2);
        assertThat(handle1.hashCode()).isNotEqualTo(handle2.hashCode());
    }

    @Test
    void testHashCodeDiffersForDifferentPath() {
        RemoteLogManifestHandle handle1 = new RemoteLogManifestHandle(new FsPath(PATH), 100L);
        RemoteLogManifestHandle handle2 =
                new RemoteLogManifestHandle(new FsPath(PATH + "/other"), 100L);

        assertThat(handle1).isNotEqualTo(handle2);
        assertThat(handle1.hashCode()).isNotEqualTo(handle2.hashCode());
    }

    @Test
    void testEqualHandlesDeduplicateInHashSet() {
        Set<RemoteLogManifestHandle> handles = new HashSet<>();
        handles.add(new RemoteLogManifestHandle(new FsPath(PATH), 100L));
        handles.add(new RemoteLogManifestHandle(new FsPath(PATH), 100L));

        assertThat(handles).hasSize(1);
    }
}
