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

package org.apache.fluss.metadata;

import java.util.Objects;

/** A class to represent a replica of the {@link TableBucket}. */
public class TableBucketReplica {

    private final TableBucket tableBucket;
    private final int replica;

    public TableBucketReplica(TableBucket tableBucket, int replica) {
        this.tableBucket = tableBucket;
        this.replica = replica;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public int getReplica() {
        return replica;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableBucketReplica that = (TableBucketReplica) o;
        return replica == that.replica && Objects.equals(tableBucket, that.tableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableBucket, replica);
    }

    @Override
    public String toString() {
        return "TableBucketReplica{" + "tableBucket=" + tableBucket + ", replica=" + replica + '}';
    }
}
