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

package org.apache.fluss.fs.azure;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.azure.token.AzureDelegationTokenProvider;
import org.apache.fluss.fs.hdfs.HadoopFileSystem;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import java.io.IOException;

/**
 * Implementation of the Fluss {@link FileSystem} interface for Azure Blob Storage. This class
 * implements the common behavior implemented directly by Fluss and delegates common calls to an
 * implementation of Hadoop's filesystem abstraction.
 */
public class AzureFileSystem extends HadoopFileSystem {

    private final String scheme;
    private final Configuration conf;

    private volatile AzureDelegationTokenProvider delegationTokenProvider;

    /**
     * Wraps the given Hadoop File System object as a Fluss File System object. The given Hadoop
     * file system object is expected to be initialized already.
     *
     * @param hadoopFileSystem The Hadoop FileSystem that will be used under the hood.
     */
    public AzureFileSystem(
            String scheme, org.apache.hadoop.fs.FileSystem hadoopFileSystem, Configuration conf) {
        super(hadoopFileSystem);
        this.scheme = scheme;
        this.conf = conf;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        if (delegationTokenProvider == null) {
            synchronized (this) {
                if (delegationTokenProvider == null) {
                    delegationTokenProvider = new AzureDelegationTokenProvider(scheme, conf);
                }
            }
        }

        return delegationTokenProvider.obtainSecurityToken();
    }
}
