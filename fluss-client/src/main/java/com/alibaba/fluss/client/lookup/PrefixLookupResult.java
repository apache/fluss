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

package com.alibaba.fluss.client.lookup;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.row.InternalRow;

import java.util.List;

/**
 * The result of {@link PrefixLookuper#prefixLookup(InternalRow)}}.
 *
 * @since 0.6
 */
@PublicEvolving
public class PrefixLookupResult {
    private final List<InternalRow> rowList;

    public PrefixLookupResult(List<InternalRow> rowList) {
        this.rowList = rowList;
    }

    public List<InternalRow> getRowList() {
        return rowList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrefixLookupResult that = (PrefixLookupResult) o;
        return rowList.equals(that.rowList);
    }

    @Override
    public int hashCode() {
        return rowList.hashCode();
    }

    @Override
    public String toString() {
        return "PrefixLookupResult{" + "rowList=" + rowList + '}';
    }
}
