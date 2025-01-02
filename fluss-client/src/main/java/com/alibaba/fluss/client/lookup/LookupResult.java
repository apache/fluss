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
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.row.InternalRow;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The result of {@link Table#lookup(InternalRow)}.
 *
 * @since 0.1
 */
@PublicEvolving
public final class LookupResult {
    private final @Nullable InternalRow row;

    public LookupResult(@Nullable InternalRow row) {
        this.row = row;
    }

    public @Nullable InternalRow getRow() {
        return row;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LookupResult lookupResult = (LookupResult) o;
        return Objects.equals(row, lookupResult.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(row);
    }

    @Override
    public String toString() {
        return "LookupResult{row=" + row + '}';
    }
}
