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

package org.apache.fluss.lake.hudi.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Hudi implementation of {@link LakeSource}. */
public class HudiLakeSource implements LakeSource<HudiSplit> {

    private static final long serialVersionUID = 1L;

    private final Configuration hudiConfig;
    private final TablePath tablePath;
    private @Nullable int[][] project;

    public HudiLakeSource(Configuration hudiConfig, TablePath tablePath) {
        this.hudiConfig = hudiConfig;
        this.tablePath = tablePath;
    }

    @Override
    public void withProject(int[][] project) {
        this.project = project;
    }

    @Override
    public void withLimit(int limit) {
        throw new UnsupportedOperationException("Hudi lake source does not support limit yet.");
    }

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        return FilterPushDownResult.of(Collections.emptyList(), new ArrayList<>(predicates));
    }

    @Override
    public Planner<HudiSplit> createPlanner(PlannerContext context) throws IOException {
        return new HudiSplitPlanner(hudiConfig, tablePath, context.snapshotId());
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<HudiSplit> context) throws IOException {
        try {
            return new HudiRecordReader(hudiConfig, tablePath, context.lakeSplit(), project);
        } catch (Exception e) {
            throw new IOException("Fail to create Hudi record reader for " + tablePath + ".", e);
        }
    }

    @Override
    public SimpleVersionedSerializer<HudiSplit> getSplitSerializer() {
        return new HudiSplitSerializer();
    }
}
