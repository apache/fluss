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

package com.alibaba.fluss.connector.flink.utils;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

public class ChangelogRowConverter extends FlussRowToFlinkRowConverter {
    public ChangelogRowConverter(RowType rowType) {
        super(rowType);
    }

    public RowData toFlinkRowData(ScanRecord scanRecord) {
        RowData baseRowData = super.toFlinkRowData(scanRecord);
        GenericRowData rowWithMetadata = new GenericRowData(baseRowData.getArity() + 3);
        rowWithMetadata.setRowKind(baseRowData.getRowKind());

        for (int i = 0; i < baseRowData.getArity(); i++) {
            rowWithMetadata.setField(i, baseRowData.getRawValue(i));
        }

        int baseArity = baseRowData.getArity();
        String changeType;
        switch (scanRecord.getRowKind()) {
            case INSERT:
                changeType = "+I";
                break;
            case UPDATE_BEFORE:
                changeType = "-U";
                break;
            case UPDATE_AFTER:
                changeType = "+U";
                break;
            case DELETE:
                changeType = "-D";
                break;
            default:
                changeType = "+I";
                break;
        }
        rowWithMetadata.setField(baseArity, changeType);
        rowWithMetadata.setField(baseArity + 1, scanRecord.logOffset());
        rowWithMetadata.setField(baseArity + 2, scanRecord.timestamp());

        return rowWithMetadata;
    }
}
