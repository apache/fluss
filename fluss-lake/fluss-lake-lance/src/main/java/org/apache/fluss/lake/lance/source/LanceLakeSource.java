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

package org.apache.fluss.lake.lance.source;

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
import java.util.List;
import java.util.Optional;

/** Lance lake source. */
public class LanceLakeSource implements LakeSource<LanceSplit> {
    private static final long serialVersionUID = 1L;

    private final Configuration configuration;
    private final TablePath tablePath;

    private @Nullable int[][] project;
    private @Nullable Integer limit;
    private @Nullable String filterSql;

    public LanceLakeSource(Configuration configuration, TablePath tablePath) {
        this.configuration = configuration;
        this.tablePath = tablePath;
    }

    @Override
    public void withProject(int[][] project) {
        this.project = project;
    }

    @Override
    public void withLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        List<Predicate> acceptedPredicates = new ArrayList<Predicate>();
        List<Predicate> remainingPredicates = new ArrayList<Predicate>();
        List<String> sqlParts = new ArrayList<String>();

        for (Predicate predicate : predicates) {
            Optional<String> sql = LancePredicatePushDown.toSql(predicate);
            if (sql.isPresent()) {
                acceptedPredicates.add(predicate);
                sqlParts.add("(" + sql.get() + ")");
            } else {
                remainingPredicates.add(predicate);
            }
        }

        if (sqlParts.isEmpty()) {
            this.filterSql = null;
        } else {
            this.filterSql = String.join(" AND ", sqlParts);
        }

        return FilterPushDownResult.of(acceptedPredicates, remainingPredicates);
    }

    @Override
    public Planner<LanceSplit> createPlanner(PlannerContext context) {
        return new LanceSplitPlanner(configuration, tablePath, context.snapshotId(), limit);
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<LanceSplit> context) throws IOException {
        return new LanceRecordReader(configuration, tablePath, context.lakeSplit(), project, filterSql);
    }

    @Override
    public SimpleVersionedSerializer<LanceSplit> getSplitSerializer() {
        return new LanceSplitSerializer();
    }
}
