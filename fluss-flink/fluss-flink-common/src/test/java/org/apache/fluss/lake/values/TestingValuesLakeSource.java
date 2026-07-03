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

package org.apache.fluss.lake.values;

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Testing values lake source for Flink connector tests. */
public class TestingValuesLakeSource implements LakeSource<LakeSplit> {

    private List<Predicate> predicates = Collections.emptyList();
    private int[][] project;

    @Override
    public void withProject(int[][] project) {
        this.project = project;
    }

    @Override
    public void withLimit(int limit) {}

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        this.predicates = new ArrayList<>(predicates);
        return FilterPushDownResult.of(predicates, Collections.emptyList());
    }

    @Override
    public Planner<LakeSplit> createPlanner(PlannerContext context) {
        return Collections::emptyList;
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
        return CloseableIterator::emptyIterator;
    }

    @Override
    public SimpleVersionedSerializer<LakeSplit> getSplitSerializer() {
        return new SimpleVersionedSerializer<LakeSplit>() {
            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(LakeSplit obj) throws IOException {
                return new byte[0];
            }

            @Override
            public LakeSplit deserialize(int version, byte[] serialized) throws IOException {
                throw new IOException("Unsupported testing split deserialization.");
            }
        };
    }

    public List<Predicate> getPredicates() {
        return predicates;
    }

    public int[][] getProject() {
        return project;
    }
}
