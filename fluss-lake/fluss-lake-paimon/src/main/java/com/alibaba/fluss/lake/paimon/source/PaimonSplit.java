/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.source;

import com.alibaba.fluss.lake.source.LakeSplit;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.source.DataSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Split for paimon table. */
public class PaimonSplit implements LakeSplit {

    private final DataSplit dataSplit;

    public PaimonSplit(DataSplit dataSplit) {
        this.dataSplit = dataSplit;
    }

    @Override
    public int bucket() {
        return dataSplit.bucket();
    }

    @Override
    public List<String> partition() {
        BinaryRow partition = dataSplit.partition();
        if (partition.getFieldCount() == 0) {
            return Collections.emptyList();
        }

        List<String> partitions = new ArrayList<>();
        for (int i = 0; i < partition.getFieldCount(); i++) {
            // Todo Currently, partition column must be String datatype, so we can always use
            // consider it as string. Revisit here when
            // #489 is finished.
            partitions.add(partition.getString(i).toString());
        }
        return partitions;
    }

    public DataSplit dataSplit() {
        return dataSplit;
    }
}
