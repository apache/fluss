/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static com.alibaba.fluss.metadata.TablePath.detectInvalidName;

/**
 * A database name, table name and partition name combo. It's used to represent the physical path of
 * a bucket. If the bucket belongs to a partition (i.e., the table is a partitioned table), the
 * {@link #partitionName} will be not null, otherwise null.
 *
 * @since 0.2
 */
@PublicEvolving
public class PhysicalTablePath implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;

    private final @Nullable String partitionName;

    // Cache hashCode as it is called in performance sensitive parts of the code (e.g.
    // RecordAccumulator.ready)
    private Integer hash;

    private PhysicalTablePath(TablePath tablePath, @Nullable String partitionName) {
        this.tablePath = tablePath;
        this.partitionName = partitionName;
    }

    public static PhysicalTablePath of(TablePath tablePath) {
        return new PhysicalTablePath(tablePath, null);
    }

    public static PhysicalTablePath of(TablePath tablePath, @Nullable String partitionName) {
        return new PhysicalTablePath(tablePath, partitionName);
    }

    public static PhysicalTablePath of(
            String databaseName, String tableName, @Nullable String partitionName) {
        return new PhysicalTablePath(TablePath.of(databaseName, tableName), partitionName);
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public String getDatabaseName() {
        return tablePath.getDatabaseName();
    }

    public String getTableName() {
        return tablePath.getTableName();
    }

    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    /**
     * Returns true if the database name, table name and the optional partition name are all valid.
     */
    public boolean isValid() {
        if (!getTablePath().isValid()) {
            return false;
        }

        if (partitionName != null) {
            String[] partitionValues = partitionName.split("\\$");
            for (String partitionValue : partitionValues) {
                if (detectInvalidName(partitionValue) != null) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalTablePath that = (PhysicalTablePath) o;
        return Objects.equals(tablePath, that.tablePath)
                && Objects.equals(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        Integer h = this.hash;
        if (h == null) {
            int result = Objects.hash(tablePath, partitionName);
            this.hash = result;
            return result;
        } else {
            return h;
        }
    }

    @Override
    public String toString() {
        return partitionName == null
                ? tablePath.toString()
                : tablePath + "(p=" + partitionName + ")";
    }
}
