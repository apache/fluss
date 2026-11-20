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

package org.apache.fluss.lake.delta.source;

import org.apache.fluss.lake.source.LakeSplit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A split that reads one data file of a Delta table snapshot.
 *
 * <p>It carries the Kernel scan state and the scan file row as JSON together with their schemas, so
 * a reader can read the file with {@code Scan#transformPhysicalData} without loading the Delta log
 * again. The scan file row holds the partition values and the deletion vector of the file.
 *
 * <p>Delta tables are not bucketed by Fluss buckets, so every split reports bucket {@code -1}.
 */
public class DeltaSplit implements LakeSplit {

    private static final long serialVersionUID = 1L;

    private final String scanStateSchema;
    private final String scanState;
    private final String scanFileSchema;
    private final String scanFile;
    private final ArrayList<String> partition;

    public DeltaSplit(
            String scanStateSchema,
            String scanState,
            String scanFileSchema,
            String scanFile,
            List<String> partition) {
        this.scanStateSchema = scanStateSchema;
        this.scanState = scanState;
        this.scanFileSchema = scanFileSchema;
        this.scanFile = scanFile;
        this.partition = new ArrayList<>(partition);
    }

    @Override
    public int bucket() {
        return -1;
    }

    @Override
    public List<String> partition() {
        return partition;
    }

    /** The JSON schema of {@link #scanState()}. */
    public String scanStateSchema() {
        return scanStateSchema;
    }

    /** The Kernel scan state as JSON, the same for all splits of a scan. */
    public String scanState() {
        return scanState;
    }

    /** The JSON schema of {@link #scanFile()}. */
    public String scanFileSchema() {
        return scanFileSchema;
    }

    /** The Kernel scan file row of the data file as JSON. */
    public String scanFile() {
        return scanFile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeltaSplit that = (DeltaSplit) o;
        return scanStateSchema.equals(that.scanStateSchema)
                && scanState.equals(that.scanState)
                && scanFileSchema.equals(that.scanFileSchema)
                && scanFile.equals(that.scanFile)
                && partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scanStateSchema, scanState, scanFileSchema, scanFile, partition);
    }

    @Override
    public String toString() {
        return "DeltaSplit{scanFile=" + scanFile + ", partition=" + partition + '}';
    }
}
