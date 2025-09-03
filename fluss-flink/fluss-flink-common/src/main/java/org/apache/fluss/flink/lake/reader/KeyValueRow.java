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

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;

/** An {@link InternalRow} with the key part. */
public class KeyValueRow {

    private final boolean isDelete;
    private final ProjectedRow pkRow;
    private final InternalRow valueRow;

    public KeyValueRow(int[] keyIndexes, InternalRow valueRow, boolean isDelete) {
        this.pkRow = ProjectedRow.from(keyIndexes).replaceRow(valueRow);
        this.isDelete = isDelete;
        this.valueRow = valueRow;
    }

    public boolean isDelete() {
        return isDelete;
    }

    public InternalRow keyRow() {
        return pkRow;
    }

    public InternalRow valueRow() {
        return valueRow;
    }
}
