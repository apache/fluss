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

package org.apache.fluss.fs.cos;

import org.apache.fluss.fs.cos.token.COSSecurityTokenProvider;
import org.apache.fluss.fs.hdfs.HadoopFileSystem;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * A {@link FileSystem} for Tencent Cloud COS that wraps an {@link HadoopFileSystem}, but overwrite
 * method to generate access security token.
 */
class COSFileSystem extends HadoopFileSystem {

    private final Configuration conf;
    private volatile COSSecurityTokenProvider cosSecurityTokenProvider;
    private final String scheme;

    COSFileSystem(FileSystem hadoopFileSystem, String scheme, Configuration conf) {
        super(hadoopFileSystem);
        this.scheme = scheme;
        this.conf = conf;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        try {
            mayCreateSecurityTokenProvider();
            return cosSecurityTokenProvider.obtainSecurityToken(scheme);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void mayCreateSecurityTokenProvider() throws IOException {
        if (cosSecurityTokenProvider == null) {
            synchronized (this) {
                if (cosSecurityTokenProvider == null) {
                    cosSecurityTokenProvider = new COSSecurityTokenProvider(conf);
                }
            }
        }
    }
}
