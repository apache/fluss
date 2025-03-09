/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Changelog converter that adds metadata columns using JoinedRowData. */
public class ChangelogRowConverter implements RecordToFlinkRowConverter {
    private static final Logger LOG = LoggerFactory.getLogger(ChangelogRowConverter.class);
    private final FlussRowToFlinkRowConverter converter;

    public ChangelogRowConverter(RowType rowType) {
        this.converter = new FlussRowToFlinkRowConverter(rowType);
    }

    @Override
    public RowData convert(ScanRecord scanRecord) {
        try {
            // Get the base data from the physical table
            RowData baseRowData = converter.toFlinkRowData(scanRecord);

            // Create metadata row
            GenericRowData metadataRow = new GenericRowData(3);
            metadataRow.setField(0, StringData.fromString(determineRowKind(scanRecord)));
            metadataRow.setField(1, scanRecord.logOffset());
            metadataRow.setField(2, TimestampData.fromEpochMillis(scanRecord.timestamp()));

            // Join metadata and base data
            return new JoinedRowData(metadataRow, baseRowData);
        } catch (Exception e) {
            // For debugging - log the error
            LOG.error("Error converting record: {}", e.getMessage(), e);
            throw e;
        }
    }

    private String determineRowKind(ScanRecord scanRecord) {
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
        return changeType;
    }
}
