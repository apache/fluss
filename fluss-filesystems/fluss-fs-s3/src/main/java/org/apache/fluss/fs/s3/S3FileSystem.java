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

package org.apache.fluss.fs.s3;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.hdfs.HadoopFileSystem;
import org.apache.fluss.fs.s3.token.S3DelegationTokenProvider;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import java.util.function.Supplier;

/**
 * Implementation of the Fluss {@link FileSystem} interface for S3. This class implements the common
 * behavior implemented directly by Fluss and delegates common calls to an implementation of
 * Hadoop's filesystem abstraction.
 */
public class S3FileSystem extends HadoopFileSystem {

    private final Supplier<S3DelegationTokenProvider> delegationTokenProviderSupplier;

    @VisibleForTesting volatile S3DelegationTokenProvider s3DelegationTokenProvider;

    /**
     * Creates a S3FileSystem based on the given Hadoop S3 file system. The given Hadoop file system
     * object is expected to be initialized already.
     *
     * <p>This constructor additionally configures the entropy injection for the file system.
     *
     * @param hadoopS3FileSystem The Hadoop FileSystem that will be used under the hood.
     */
    public S3FileSystem(
            org.apache.hadoop.fs.FileSystem hadoopS3FileSystem,
            Supplier<S3DelegationTokenProvider> delegationTokenProviderSupplier) {
        super(hadoopS3FileSystem);
        this.delegationTokenProviderSupplier = delegationTokenProviderSupplier;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() {
        if (s3DelegationTokenProvider == null) {
            synchronized (this) {
                if (s3DelegationTokenProvider == null) {
                    s3DelegationTokenProvider = delegationTokenProviderSupplier.get();
                }
            }
        }
        return s3DelegationTokenProvider.obtainSecurityToken();
    }
}
