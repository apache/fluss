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
