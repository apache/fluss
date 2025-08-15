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

package com.alibaba.fluss.server.metrics.group;

import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.groups.AbstractMetricGroup;
import com.alibaba.fluss.metrics.registry.MetricRegistry;

import java.util.Map;

import static com.alibaba.fluss.metrics.utils.MetricGroupUtils.makeScope;

/** Metrics for the table buckets with table as parent group. */
public class BucketMetricGroup extends AbstractMetricGroup {

    private final int bucket;

    public BucketMetricGroup(MetricRegistry registry, int bucket, TableMetricGroup parent) {
        super(registry, makeScope(parent, String.valueOf(bucket)), parent);
        this.bucket = bucket;
    }

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put("bucket", String.valueOf(bucket));
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "bucket";
    }

    public TableMetricGroup getTableMetricGroup() {
        return (TableMetricGroup) parent;
    }
}
