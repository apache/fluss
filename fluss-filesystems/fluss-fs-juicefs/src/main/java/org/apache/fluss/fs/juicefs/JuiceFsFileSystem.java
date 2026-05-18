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

package org.apache.fluss.fs.juicefs;

import org.apache.fluss.fs.hdfs.HadoopFileSystem;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.apache.hadoop.fs.FileSystem;

import java.util.Collections;

/**
 * A {@link org.apache.fluss.fs.FileSystem} for JuiceFS that wraps a {@link HadoopFileSystem}.
 *
 * <p>Unlike the OSS / S3 plugins, JuiceFS does not require Fluss to obtain or distribute a
 * delegation token: the JuiceFS client itself authenticates against the meta server using
 * locally-configured credentials (e.g. {@code juicefs.access-key} / {@code juicefs.secret-key}, or
 * implicit IAM). We therefore return an empty placeholder token from {@link
 * #obtainSecurityToken()}.
 */
class JuiceFsFileSystem extends HadoopFileSystem {

    private static final ObtainedSecurityToken EMPTY_TOKEN =
            new ObtainedSecurityToken(
                    JuiceFsPlugin.SCHEME, new byte[0], null, Collections.emptyMap());

    JuiceFsFileSystem(FileSystem hadoopFileSystem) {
        super(hadoopFileSystem);
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() {
        return EMPTY_TOKEN;
    }
}
