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

package com.alibaba.fluss.connector.flink.sink;

import com.alibaba.fluss.client.table.writer.Upsert;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** An upsert sink for fluss primary key table. */
class UpsertSinkFunction extends FlinkSinkFunction {

    private static final long serialVersionUID = 1L;

    private transient UpsertWriter upsertWriter;

    UpsertSinkFunction(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            boolean ignoreDelete) {
        super(tablePath, flussConfig, tableRowType, targetColumnIndexes, ignoreDelete);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration config) {
        super.open(config);
        Upsert upsert = table.newUpsert();
        if (targetColumnIndexes != null) {
            upsert = upsert.partialUpdate(targetColumnIndexes);
        }
        upsertWriter = upsert.createWriter();
        LOG.info("Finished opening Fluss {}.", this.getClass().getSimpleName());
    }

    @Override
    CompletableFuture<?> writeRow(RowKind rowKind, InternalRow internalRow) {
        if (rowKind.equals(RowKind.INSERT) || rowKind.equals(RowKind.UPDATE_AFTER)) {
            return upsertWriter.upsert(internalRow);
        } else if ((rowKind.equals(RowKind.DELETE) || rowKind.equals(RowKind.UPDATE_BEFORE))) {
            return upsertWriter.delete(internalRow);
        } else {
            throw new UnsupportedOperationException("Unsupported row kind: " + rowKind);
        }
    }

    @Override
    void flush() throws IOException {
        upsertWriter.flush();
        checkAsyncException();
    }
}
