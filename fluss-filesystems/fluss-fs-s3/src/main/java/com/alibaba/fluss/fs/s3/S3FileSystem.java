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

package com.alibaba.fluss.fs.s3;

import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.hdfs.HadoopFileSystem;
import com.alibaba.fluss.fs.s3.token.S3DelegationTokenProvider;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;

import org.apache.hadoop.conf.Configuration;

/**
 * Implementation of the Fluss {@link FileSystem} interface for S3. This class implements the common
 * behavior implemented directly by Fluss and delegates common calls to an implementation of
 * Hadoop's filesystem abstraction.
 */
public class S3FileSystem extends HadoopFileSystem {

    private final String scheme;
    private final Configuration conf;

    private volatile S3DelegationTokenProvider s3DelegationTokenProvider;

    /**
     * Creates a S3FileSystem based on the given Hadoop S3 file system. The given Hadoop file system
     * object is expected to be initialized already.
     *
     * <p>This constructor additionally configures the entropy injection for the file system.
     *
     * @param hadoopS3FileSystem The Hadoop FileSystem that will be used under the hood.
     */
    public S3FileSystem(
            String scheme, org.apache.hadoop.fs.FileSystem hadoopS3FileSystem, Configuration conf) {
        super(hadoopS3FileSystem);
        this.scheme = scheme;
        this.conf = conf;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() {
        if (s3DelegationTokenProvider == null) {
            synchronized (this) {
                if (s3DelegationTokenProvider == null) {
                    s3DelegationTokenProvider = new S3DelegationTokenProvider(scheme, conf);
                }
            }
        }
        return s3DelegationTokenProvider.obtainSecurityToken();
    }
}
