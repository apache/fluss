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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.source.SortedRecordReader;
import org.apache.fluss.row.InternalRow;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;

/** Sorted record reader for primary key paimon table. */
public class PaimonSortedRecordReader extends PaimonRecordReader implements SortedRecordReader {

    Comparator<org.apache.fluss.row.InternalRow> comparator;

    public PaimonSortedRecordReader(
            FileStoreTable fileStoreTable,
            PaimonSplit split,
            @Nullable int[][] project,
            @Nullable Predicate predicate)
            throws IOException {
        super(fileStoreTable, split, project, predicate);
        RowType pkKeyType =
                new RowType(
                        PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR.keyFields(
                                fileStoreTable.schema()));

        this.comparator =
                toFlussRowComparator(paimonRowType, new KeyComparatorSupplier(pkKeyType).get());
    }

    @Override
    public Comparator<InternalRow> order() {
        return comparator;
    }

    private Comparator<org.apache.fluss.row.InternalRow> toFlussRowComparator(
            RowType rowType, Comparator<org.apache.paimon.data.InternalRow> paimonRowcomparator) {
        return (row1, row2) ->
                paimonRowcomparator.compare(
                        new FlussRowAsPaimonRow(row1, rowType),
                        new FlussRowAsPaimonRow(row2, rowType));
    }
}
