package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;

import javax.annotation.concurrent.NotThreadSafe;

/** A updater to auto increment column . */
public interface AutoIncUpdater {

    /**
     * Updates the auto-increment column in the given row by replacing its value with a new sequence
     * number.
     *
     * <p>This method may return a new {@link BinaryValue} instance or the same instance if no
     * update is needed (e.g., in a no-op implementation).
     *
     * @param rowValue the input row in binary form, must not be {@code null}
     * @return a {@link BinaryValue} representing the updated row; never {@code null}
     */
    BinaryValue updateAutoInc(BinaryValue rowValue);

    /** Default auto increment column updater. */
    @NotThreadSafe
    class DefaultAutoIncUpdater implements AutoIncUpdater {
        private final InternalRow.FieldGetter[] flussFieldGetters;
        private final RowEncoder rowEncoder;
        private final DataType[] fieldDataTypes;
        private final int targetColumnIdx;
        private final SequenceGenerator idGenerator;
        private final short schemaId;

        public DefaultAutoIncUpdater(
                KvFormat kvFormat,
                short schemaId,
                Schema schema,
                int autoIncColumnId,
                SequenceGenerator sequenceGenerator) {
            DataType[] fieldDataTypes = schema.getRowType().getChildren().toArray(new DataType[0]);

            // getter for the fields in row
            InternalRow.FieldGetter[] flussFieldGetters =
                    new InternalRow.FieldGetter[fieldDataTypes.length];
            for (int i = 0; i < fieldDataTypes.length; i++) {
                flussFieldGetters[i] = InternalRow.createFieldGetter(fieldDataTypes[i], i);
            }
            this.idGenerator = sequenceGenerator;
            this.schemaId = schemaId;
            this.targetColumnIdx = schema.getColumnIds().indexOf(autoIncColumnId);
            if (targetColumnIdx == -1) {
                throw new IllegalStateException(
                        String.format(
                                "Auto-increment column ID %d not found in schema columns: %s",
                                autoIncColumnId, schema.getColumnIds()));
            }
            this.rowEncoder = RowEncoder.create(kvFormat, fieldDataTypes);
            this.fieldDataTypes = fieldDataTypes;
            this.flussFieldGetters = flussFieldGetters;
        }

        public BinaryValue updateAutoInc(BinaryValue rowValue) {
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldDataTypes.length; i++) {
                if (targetColumnIdx == i) {
                    rowEncoder.encodeField(i, idGenerator.nextVal());
                } else {
                    // use the row value
                    rowEncoder.encodeField(i, flussFieldGetters[i].getFieldOrNull(rowValue.row));
                }
            }
            return new BinaryValue(schemaId, rowEncoder.finishRow());
        }
    }

    /** No-op implementation that returns the input unchanged. */
    class NoOpUpdater implements AutoIncUpdater {
        @Override
        public BinaryValue updateAutoInc(BinaryValue rowValue) {
            return rowValue;
        }
    }
}
