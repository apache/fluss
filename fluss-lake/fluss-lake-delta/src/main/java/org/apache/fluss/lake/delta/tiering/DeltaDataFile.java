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

package org.apache.fluss.lake.delta.tiering;

import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.DataFileStatus;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A Parquet data file written by a {@link DeltaLakeWriter} but not committed yet.
 *
 * <p>Delta Kernel turns a {@link DataFileStatus} into an {@code add} action only inside a
 * transaction, and the actions are not serializable. So the writer keeps the file status and the
 * partition values, and the committer generates the {@code add} actions when it commits.
 */
public class DeltaDataFile implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final long size;
    private final long modificationTime;
    @Nullable private final String statisticsJson;
    private final HashMap<String, String> partitionValues;

    public DeltaDataFile(
            String path,
            long size,
            long modificationTime,
            @Nullable String statisticsJson,
            Map<String, String> partitionValues) {
        this.path = path;
        this.size = size;
        this.modificationTime = modificationTime;
        this.statisticsJson = statisticsJson;
        this.partitionValues = new HashMap<>(partitionValues);
    }

    /** Creates a data file from the status returned by the Delta Kernel Parquet writer. */
    public static DeltaDataFile of(
            DataFileStatus status, StructType schema, Map<String, String> partitionValues) {
        return new DeltaDataFile(
                status.getPath(),
                status.getSize(),
                status.getModificationTime(),
                status.getStatistics().map(stats -> stats.serializeAsJson(schema)).orElse(null),
                partitionValues);
    }

    /** Restores the file status, the schema is needed to read back the statistics. */
    public DataFileStatus toDataFileStatus(StructType schema) {
        Optional<DataFileStatistics> statistics =
                statisticsJson == null
                        ? Optional.empty()
                        : DataFileStatistics.deserializeFromJson(statisticsJson, schema);
        return new DataFileStatus(path, size, modificationTime, statistics);
    }

    public String path() {
        return path;
    }

    public Map<String, String> partitionValues() {
        return partitionValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeltaDataFile that = (DeltaDataFile) o;
        return size == that.size
                && modificationTime == that.modificationTime
                && path.equals(that.path)
                && Objects.equals(statisticsJson, that.statisticsJson)
                && partitionValues.equals(that.partitionValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, size, modificationTime, statisticsJson, partitionValues);
    }

    @Override
    public String toString() {
        return "DeltaDataFile{path='"
                + path
                + "', size="
                + size
                + ", partitionValues="
                + partitionValues
                + '}';
    }
}
