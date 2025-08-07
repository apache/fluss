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

package com.alibaba.fluss.lake.source;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.row.InternalRow;

import java.util.Comparator;

/**
 * A specialized {@link RecordReader} that produces records in a defined sorted order.
 *
 * <p>Extends the basic record reading capability with sorting semantics, ensuring that records are
 * returned according to a specified ordering.
 *
 * <p>Implementations must guarantee that the {@link #read()} method returns records in the order
 * defined by the comparator from {@link #order()}.
 *
 * <p>Note: This is mainly used for union read primary key table since we will do sort merge records
 * in lake and fluss. The records in primary key table for lake may should implement this method for
 * union read with a better performance.
 *
 * @since 0.8
 */
@PublicEvolving
public interface SortedRecordReader extends RecordReader {

    /**
     * Returns the comparator that defines the sort order of the records.
     *
     * @return a non-null comparator defining the sort order of the records
     */
    Comparator<InternalRow> order();
}
