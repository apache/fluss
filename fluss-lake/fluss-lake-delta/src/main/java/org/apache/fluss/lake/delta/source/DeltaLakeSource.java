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

package org.apache.fluss.lake.delta.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.utils.ArrayUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Reads the tiered data of a Fluss log table from its Delta table, so that a union read can combine
 * it with the Fluss log that is not tiered yet.
 *
 * <p>Projection is pushed down to the Parquet reader. Filters are not pushed down yet and stay with
 * the engine.
 */
public class DeltaLakeSource implements LakeSource<DeltaSplit> {

    private static final long serialVersionUID = 1L;

    private final Configuration options;
    private final TablePath tablePath;
    private @Nullable int[][] project;

    public DeltaLakeSource(Configuration options, TablePath tablePath) {
        this.options = options;
        this.tablePath = tablePath;
    }

    private DeltaLakeSource(DeltaLakeSource source) {
        this.options = new Configuration(source.options);
        this.tablePath = source.tablePath;
        this.project = ArrayUtils.deepCopy(source.project);
    }

    @Override
    public DeltaLakeSource copy() {
        return new DeltaLakeSource(this);
    }

    @Override
    public void withProject(int[][] project) {
        this.project = ArrayUtils.deepCopy(project);
    }

    @Override
    public void withLimit(int limit) {
        throw new UnsupportedOperationException("Limit push down is not supported by Delta Lake.");
    }

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        return FilterPushDownResult.of(Collections.emptyList(), predicates);
    }

    @Override
    public Planner<DeltaSplit> createPlanner(PlannerContext context) throws IOException {
        return new DeltaSplitPlanner(options, tablePath, context.snapshotId(), project);
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<DeltaSplit> context) throws IOException {
        return new DeltaRecordReader(options, context.lakeSplit());
    }

    @Override
    public SimpleVersionedSerializer<DeltaSplit> getSplitSerializer() {
        return new DeltaSplitSerializer();
    }
}
