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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** The committable that derived from {@link IcebergWriteResult} to commit to Iceberg. */
public class IcebergCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<DataFile> dataFiles;
    private final List<DeleteFile> deleteFiles;

    private IcebergCommittable(List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
        this.dataFiles = dataFiles;
        this.deleteFiles = deleteFiles;
    }

    public List<DataFile> getDataFiles() {
        return dataFiles;
    }

    public List<DeleteFile> getDeleteFiles() {
        return deleteFiles;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link IcebergCommittable}, collecting {@link DataFile} and {@link DeleteFile}
     * entries.
     */
    public static class Builder {
        private final List<DataFile> dataFiles = new ArrayList<>();
        private final List<DeleteFile> deleteFiles = new ArrayList<>();

        public Builder addDataFile(DataFile dataFile) {
            this.dataFiles.add(dataFile);
            return this;
        }

        public Builder addDeleteFile(DeleteFile deleteFile) {
            this.deleteFiles.add(deleteFile);
            return this;
        }

        public IcebergCommittable build() {
            return new IcebergCommittable(new ArrayList<>(dataFiles), new ArrayList<>(deleteFiles));
        }
    }

    @Override
    public String toString() {
        return "IcebergCommittable{"
                + "dataFiles="
                + dataFiles.size()
                + ", deleteFiles="
                + deleteFiles.size()
                + '}';
    }
}
