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

package com.alibaba.fluss.fs.gs;

import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.hdfs.HadoopFileSystem;

import org.apache.hadoop.conf.Configuration;

// TODO: implement obtainSecurityToken to enable clients access the Google File System
//  through the FileSystem security token receiver API.
/**
 * Implementation of the Fluss {@link FileSystem} interface for Google Cloud Storage. This class
 * implements the common behavior implemented directly by Fluss and delegates common calls to an
 * implementation of Hadoop's filesystem abstraction.
 */
public class GSFileSystem extends HadoopFileSystem {

    private final String scheme;
    private final Configuration conf;

    /**
     * Creates a GSFileSystem based on the given Hadoop Google Cloud Storage file system. The given
     * Hadoop file system object is expected to be initialized already.
     *
     * <p>This constructor additionally configures the entropy injection for the file system.
     *
     * @param hadoopGSFileSystem The Hadoop FileSystem that will be used under the hood.
     */
    public GSFileSystem(
            String scheme, org.apache.hadoop.fs.FileSystem hadoopGSFileSystem, Configuration conf) {
        super(hadoopGSFileSystem);
        this.scheme = scheme;
        this.conf = conf;
    }
}
