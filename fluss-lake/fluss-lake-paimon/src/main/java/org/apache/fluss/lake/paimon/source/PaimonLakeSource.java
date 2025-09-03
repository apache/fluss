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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.paimon.utils.FlussToPaimonPredicateConverter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;

/**
 * Paimon Lake format implementation of {@link org.apache.fluss.lake.source.LakeSource} for reading
 * paimon table.
 */
public class PaimonLakeSource implements LakeSource<PaimonSplit> {

    private final Configuration paimonConfig;
    private final TablePath tablePath;

    private @Nullable int[][] project;
    private @Nullable org.apache.paimon.predicate.Predicate predicate;

    public PaimonLakeSource(Configuration paimonConfig, TablePath tablePath) {
        this.paimonConfig = paimonConfig;
        this.tablePath = tablePath;
    }

    @Override
    public void withProject(int[][] project) {
        this.project = project;
    }

    @Override
    public void withLimit(int limit) {
        throw new UnsupportedOperationException("Not impl.");
    }

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        List<Predicate> unConsumedPredicates = new ArrayList<>();
        List<Predicate> consumedPredicates = new ArrayList<>();
        List<org.apache.paimon.predicate.Predicate> converted = new ArrayList<>();
        for (Predicate predicate : predicates) {
            Optional<org.apache.paimon.predicate.Predicate> optPredicate =
                    FlussToPaimonPredicateConverter.convert(getRowType(tablePath), predicate);
            if (optPredicate.isPresent()) {
                consumedPredicates.add(predicate);
                converted.add(optPredicate.get());
            } else {
                unConsumedPredicates.add(predicate);
            }
        }
        if (!converted.isEmpty()) {
            predicate = PredicateBuilder.and(converted);
        }
        return FilterPushDownResult.of(consumedPredicates, unConsumedPredicates);
    }

    @Override
    public Planner<PaimonSplit> createPlanner(PlannerContext plannerContext) {
        return new PaimonSplitPlanner(
                paimonConfig, tablePath, predicate, plannerContext.snapshotId());
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<PaimonSplit> context) throws IOException {
        try (Catalog catalog = getCatalog()) {
            FileStoreTable fileStoreTable = getTable(catalog, tablePath);
            if (fileStoreTable.primaryKeys().isEmpty()) {
                return new PaimonRecordReader(
                        fileStoreTable, context.lakeSplit(), project, predicate);
            } else {
                return new PaimonSortedRecordReader(
                        fileStoreTable, context.lakeSplit(), project, predicate);
            }
        } catch (Exception e) {
            throw new IOException("Fail to create record reader.", e);
        }
    }

    @Override
    public SimpleVersionedSerializer<PaimonSplit> getSplitSerializer() {
        return new PaimonSplitSerializer();
    }

    private Catalog getCatalog() {
        return CatalogFactory.createCatalog(
                CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
    }

    private FileStoreTable getTable(Catalog catalog, TablePath tablePath) throws Exception {
        return (FileStoreTable) catalog.getTable(toPaimon(tablePath));
    }

    private RowType getRowType(TablePath tablePath) {
        try (Catalog catalog = getCatalog()) {
            FileStoreTable fileStoreTable = getTable(catalog, tablePath);
            return fileStoreTable.rowType();
        } catch (Exception e) {
            throw new RuntimeException("Fail to get row type of " + tablePath, e);
        }
    }
}
