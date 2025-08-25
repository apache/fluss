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

package org.apache.fluss.server.replica.delay;

import org.apache.fluss.metadata.TableBucket;

/**
 * Delayed table bucket key, used as watch key in delay operation manager, such as {@link
 * DelayedWrite} and {@link DelayedFetchLog}.
 */
public class DelayedTableBucketKey implements DelayedOperationKey {
    private final TableBucket tableBucket;

    public DelayedTableBucketKey(TableBucket tableBucket) {
        this.tableBucket = tableBucket;
    }

    @Override
    public String keyLabel() {
        return String.format("%s-%d", tableBucket.getTableId(), tableBucket.getBucket());
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DelayedTableBucketKey that = (DelayedTableBucketKey) o;
        return tableBucket.equals(that.tableBucket);
    }

    @Override
    public int hashCode() {
        return tableBucket.hashCode();
    }
}
