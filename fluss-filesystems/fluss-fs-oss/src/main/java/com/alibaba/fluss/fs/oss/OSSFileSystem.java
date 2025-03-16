/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.oss;

import com.alibaba.fluss.fs.hdfs.HadoopFileSystem;
import com.alibaba.fluss.fs.oss.token.OSSSecurityTokenProvider;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A {@link FileSystem} for Oss that wraps an {@link HadoopFileSystem}, but overwrite method to
 * generate access security token.
 */
class OSSFileSystem extends HadoopFileSystem {

    private final Configuration conf;
    private volatile OSSSecurityTokenProvider ossSecurityTokenProvider;
    private final String scheme;

    OSSFileSystem(FileSystem hadoopFileSystem, String scheme, Configuration conf) {
        super(hadoopFileSystem);
        this.scheme = scheme;
        this.conf = conf;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        try {
            mayCreateSecurityTokenProvider();
            return ossSecurityTokenProvider.obtainSecurityToken(scheme);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void mayCreateSecurityTokenProvider() throws IOException {
        if (ossSecurityTokenProvider == null) {
            synchronized (this) {
                if (ossSecurityTokenProvider == null) {
                    ossSecurityTokenProvider = new OSSSecurityTokenProvider(conf);
                }
            }
        }
    }
}
