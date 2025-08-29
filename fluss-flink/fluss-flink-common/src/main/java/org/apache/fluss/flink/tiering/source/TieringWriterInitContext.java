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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

/** The implementation of {@link WriterInitContext}. */
public class TieringWriterInitContext implements WriterInitContext {

    private final TablePath tablePath;
    private final TableBucket tableBucket;
    @Nullable private final String partition;
    private final TableInfo tableInfo;

    public TieringWriterInitContext(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partition,
            TableInfo tableInfo) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.partition = partition;
        this.tableInfo = tableInfo;
    }

    @Override
    public TablePath tablePath() {
        return tablePath;
    }

    @Override
    public TableBucket tableBucket() {
        return tableBucket;
    }

    @Nullable
    @Override
    public String partition() {
        return partition;
    }

    @Override
    public TableInfo tableInfo() {
        return tableInfo;
    }
}
