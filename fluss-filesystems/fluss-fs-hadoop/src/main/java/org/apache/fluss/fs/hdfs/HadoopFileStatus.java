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

package org.apache.fluss.fs.hdfs;

import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FsPath;

/**
 * Concrete implementation of the {@link FileStatus} interface for the Hadoop Distributed File
 * System.
 */
public class HadoopFileStatus implements FileStatus {

    private final org.apache.hadoop.fs.FileStatus fileStatus;

    /**
     * Creates a new file status from an HDFS file status.
     *
     * @param fileStatus the HDFS file status
     */
    public HadoopFileStatus(org.apache.hadoop.fs.FileStatus fileStatus) {
        this.fileStatus = fileStatus;
    }

    @Override
    public long getLen() {
        return fileStatus.getLen();
    }

    @Override
    public FsPath getPath() {
        return new FsPath(fileStatus.getPath().toUri());
    }

    @Override
    public boolean isDir() {
        return fileStatus.isDirectory();
    }

    // ------------------------------------------------------------------------

    /**
     * Creates a new {@code HadoopFileStatus} from Hadoop's {@link org.apache.hadoop.fs.FileStatus}.
     */
    public static HadoopFileStatus fromHadoopStatus(
            final org.apache.hadoop.fs.FileStatus fileStatus) {
        return new HadoopFileStatus(fileStatus);
    }
}
