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
import org.apache.fluss.lake.hudi.utils.FlussToHudiExpressionPredicateConverter;
import org.apache.fluss.lake.hudi.utils.HudiTableInfo;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.util.StreamerUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Hudi implementation of {@link LakeSource}. */
public class HudiLakeSource implements LakeSource<HudiSplit> {

    private static final long serialVersionUID = 1L;

    private final Configuration hudiConfig;
    private final TablePath tablePath;
    private @Nullable int[][] project;
    private List<ExpressionPredicates.Predicate> predicates = Collections.emptyList();

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
        if (predicates.isEmpty()) {
            this.predicates = Collections.emptyList();
            return FilterPushDownResult.of(Collections.emptyList(), Collections.emptyList());
        }

        List<Predicate> remainingPredicates = new ArrayList<>();
        List<Predicate> acceptedPredicates = new ArrayList<>();
        List<ExpressionPredicates.Predicate> convertedPredicates = new ArrayList<>();

        Schema hudiSchema = getHudiSchema();
        for (Predicate predicate : predicates) {
            Optional<ExpressionPredicates.Predicate> convertedPredicate =
                    FlussToHudiExpressionPredicateConverter.convert(hudiSchema, predicate);
            if (convertedPredicate.isPresent()) {
                acceptedPredicates.add(predicate);
                convertedPredicates.add(convertedPredicate.get());
            } else {
                remainingPredicates.add(predicate);
            }
        }

        this.predicates =
                convertedPredicates.isEmpty()
                        ? Collections.emptyList()
                        : Collections.unmodifiableList(new ArrayList<>(convertedPredicates));
        return FilterPushDownResult.of(acceptedPredicates, remainingPredicates);
    }

    @Override
    public Planner<HudiSplit> createPlanner(PlannerContext context) throws IOException {
        return new HudiSplitPlanner(hudiConfig, tablePath, context.snapshotId());
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<HudiSplit> context) throws IOException {
        try {
            if (shouldUseSortedRecordReader()) {
                return new HudiSortedRecordReader(
                        hudiConfig, tablePath, context.lakeSplit(), project, predicates);
            }
            return new HudiRecordReader(
                    hudiConfig, tablePath, context.lakeSplit(), project, predicates);
        } catch (Exception e) {
            throw new IOException("Fail to create Hudi record reader for " + tablePath + ".", e);
        }
    }

    @Override
    public SimpleVersionedSerializer<HudiSplit> getSplitSerializer() {
        return new HudiSplitSerializer();
    }

    private Schema getHudiSchema() {
        try (HudiTableInfo hudiTableInfo = HudiTableInfo.create(tablePath, hudiConfig)) {
            return StreamerUtil.getTableAvroSchema(hudiTableInfo.getMetaClient(), true);
        } catch (Exception e) {
            throw new RuntimeException("Fail to get Hudi schema for " + tablePath + ".", e);
        }
    }

    private boolean shouldUseSortedRecordReader() throws IOException {
        try (HudiTableInfo hudiTableInfo = HudiTableInfo.create(tablePath, hudiConfig)) {
            return hudiTableInfo.getTableType() == HoodieTableType.MERGE_ON_READ
                    && HudiSortedRecordReader.canSortByRecordKey(hudiTableInfo, project);
        }
    }
}
