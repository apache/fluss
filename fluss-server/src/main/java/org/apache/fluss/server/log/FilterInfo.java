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

package org.apache.fluss.server.log;

import org.apache.fluss.rpc.messages.PbPredicate;

import java.util.Objects;

/** Holds the raw filter predicate and schema ID from a fetch request. */
public final class FilterInfo {
    private final PbPredicate pbPredicate;
    private final int schemaId;

    public FilterInfo(PbPredicate pbPredicate, int schemaId) {
        this.pbPredicate = pbPredicate;
        this.schemaId = schemaId;
    }

    public PbPredicate getPbPredicate() {
        return pbPredicate;
    }

    public int getSchemaId() {
        return schemaId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FilterInfo that = (FilterInfo) o;
        return schemaId == that.schemaId && Objects.equals(pbPredicate, that.pbPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pbPredicate, schemaId);
    }
}
