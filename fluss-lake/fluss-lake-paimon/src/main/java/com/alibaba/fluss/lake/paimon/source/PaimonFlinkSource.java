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

package com.alibaba.fluss.lake.paimon.source;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.lake.hybrid.FlinkSourceContext;
import com.alibaba.fluss.flink.lake.hybrid.LakeFlinkSource;
import com.alibaba.fluss.lake.paimon.flink.FlinkStaticFIleStoreSource;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.PredicateConverter;
import org.apache.paimon.flink.Projection;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** PaimonFlinkSource is a flink source for paimon table. */
public class PaimonFlinkSource implements LakeFlinkSource {

    private final Configuration paimonConfig;

    public PaimonFlinkSource(Configuration paimonConfig) {
        this.paimonConfig = paimonConfig;
    }

    @Override
    public Source<RowData, ?, ?> createFlinkSource(FlinkSourceContext flinkSourceContext) {
        try {
            Catalog paimonCatalog = createCatalog(paimonConfig.toMap());
            TablePath tablePath = flinkSourceContext.getTablePath();
            Table table =
                    paimonCatalog.getTable(
                            Identifier.create(
                                    tablePath.getDatabaseName(), tablePath.getTableName()));
            table =
                    table.copy(
                            Collections.singletonMap(
                                    CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                    String.valueOf(flinkSourceContext.getSnapshotId())));
            Options options = Options.fromMap(table.options());
            Predicate predicate = toPredicate(table, flinkSourceContext.getFilters());
            return new FlinkStaticFIleStoreSource(
                    createReadBuilder(
                            table,
                            predicate,
                            projectedRowType(table, flinkSourceContext.getProjectedFields()),
                            flinkSourceContext.getPartitions()),
                    options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_BATCH_SIZE),
                    flinkSourceContext.getLakeSplitAssigner());
        } catch (Exception e) {
            throw new RuntimeException("Fail to create Flink Source", e);
        }
    }

    private Catalog createCatalog(Map<String, String> lakeProperties) {
        CatalogContext catalogContext = CatalogContext.create(Options.fromMap(lakeProperties));
        return CatalogFactory.createCatalog(catalogContext);
    }

    @Nullable
    private Predicate toPredicate(Table table, @Nullable List<ResolvedExpression> filters) {
        if (filters == null) {
            return null;
        }

        RowType rowType = LogicalTypeConversion.toLogicalType(table.rowType());

        List<Predicate> converted = new ArrayList<>();

        for (ResolvedExpression filter : filters) {
            Optional<Predicate> predicateOptional = PredicateConverter.convert(rowType, filter);
            predicateOptional.ifPresent(converted::add);
        }
        return converted.isEmpty() ? null : PredicateBuilder.and(converted);
    }

    private ReadBuilder createReadBuilder(
            Table table,
            @Nullable Predicate predicate,
            @Nullable org.apache.paimon.types.RowType readType,
            Set<String> partitions) {
        ReadBuilder readBuilder = table.newReadBuilder();
        if (readType != null) {
            readBuilder.withReadType(readType);
        }
        readBuilder.withFilter(predicate);

        for (String partition : partitions) {
            readBuilder =
                    readBuilder.withPartitionFilter(
                            Collections.singletonMap(table.partitionKeys().get(0), partition));
        }

        return readBuilder.dropStats();
    }

    private @Nullable org.apache.paimon.types.RowType projectedRowType(
            Table table, @Nullable int[][] projectedFields) {
        return Optional.ofNullable(projectedFields)
                .map(Projection::of)
                .map(p -> p.project(table.rowType()))
                .orElse(null);
    }
}
