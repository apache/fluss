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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Identifies a BulkLoad transaction by its physical table ID and caller-supplied ID. */
@PublicEvolving
public final class BulkLoadHandle implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long tableId;
    private final String bulkLoadId;

    /** Restores a transaction handle from its two identifiers. */
    public BulkLoadHandle(long tableId, String bulkLoadId) {
        this.tableId = tableId;
        this.bulkLoadId = checkNotNull(bulkLoadId, "bulkLoadId must not be null");
    }

    /** Returns the original physical table ID. */
    public long getTableId() {
        return tableId;
    }

    /** Returns the transaction ID supplied to Begin. */
    public String getBulkLoadId() {
        return bulkLoadId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BulkLoadHandle)) {
            return false;
        }
        BulkLoadHandle that = (BulkLoadHandle) other;
        return tableId == that.tableId && bulkLoadId.equals(that.bulkLoadId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, bulkLoadId);
    }

    @Override
    public String toString() {
        return "BulkLoadHandle{tableId=" + tableId + ", bulkLoadId=" + bulkLoadId + '}';
    }
}
