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

package org.apache.fluss.trino;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Real row iterator with observable ownership and overridable failure boundaries. */
class TestingRowIterator implements CloseableIterator<InternalRow> {
    private final Iterator<InternalRow> rows;
    int closes;

    TestingRowIterator(Iterator<InternalRow> rows) {
        this.rows = rows;
    }

    static TestingRowIterator rows(int count) {
        List<InternalRow> rows = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            rows.add(GenericRow.of(i, i));
        }
        return new TestingRowIterator(rows.iterator());
    }

    @Override
    public boolean hasNext() {
        return rows.hasNext();
    }

    @Override
    public InternalRow next() {
        return rows.next();
    }

    @Override
    public void close() {
        closes++;
    }
}
