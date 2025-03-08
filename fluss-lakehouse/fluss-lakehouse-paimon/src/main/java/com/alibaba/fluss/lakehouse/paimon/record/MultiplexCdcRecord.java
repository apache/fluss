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

package com.alibaba.fluss.lakehouse.paimon.record;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import java.io.Serializable;
import java.util.Objects;

/**
 * A data change message from the Fluss source. Compared to {@link CdcRecord}, MultiplexCdcRecord
 * contains table and bucket info that the cdc record from.
 */
public class MultiplexCdcRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private TablePath tablePath;
    private TableBucket tableBucket;

    private CdcRecord cdcRecord;

    public MultiplexCdcRecord() {}

    public MultiplexCdcRecord(TablePath tablePath, TableBucket tableBucket, CdcRecord cdcRecord) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.cdcRecord = cdcRecord;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public void setTablePath(TablePath tablePath) {
        this.tablePath = tablePath;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public void setTableBucket(TableBucket tableBucket) {
        this.tableBucket = tableBucket;
    }

    public CdcRecord getCdcRecord() {
        return cdcRecord;
    }

    public void setCdcRecord(CdcRecord cdcRecord) {
        this.cdcRecord = cdcRecord;
    }

    public long getOffset() {
        return cdcRecord.getOffset();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MultiplexCdcRecord)) {
            return false;
        }
        MultiplexCdcRecord that = (MultiplexCdcRecord) o;
        return Objects.equals(tablePath, that.tablePath)
                && Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(cdcRecord, that.cdcRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableBucket, cdcRecord);
    }

    @Override
    public String toString() {
        return "MultiplexCdcRecord{"
                + "tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + ", cdcRecord="
                + cdcRecord
                + '}';
    }
}
