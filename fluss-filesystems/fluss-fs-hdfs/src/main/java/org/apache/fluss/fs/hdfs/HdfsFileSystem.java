/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.fs.hdfs;

import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import java.io.IOException;
import java.util.Collections;

/**
 * A {@link FileSystem} for HDFS that wraps an {@link HadoopFileSystem}, but overrides method to
 * generate access security token.
 */
public class HdfsFileSystem extends HadoopFileSystem {

    private static final ObtainedSecurityToken TOKEN =
            new ObtainedSecurityToken(HdfsPlugin.SCHEME, new byte[0], null, Collections.emptyMap());

    public HdfsFileSystem(org.apache.hadoop.fs.FileSystem hadoopFileSystem) {
        super(hadoopFileSystem);
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        return TOKEN;
    }
}
