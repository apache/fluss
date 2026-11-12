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

package org.apache.fluss.trino;

import org.apache.fluss.trino.functions.HllBuildAggregation;
import org.apache.fluss.trino.functions.HllMergeAggregation;
import org.apache.fluss.trino.functions.KllDoubleBuildAggregation;
import org.apache.fluss.trino.functions.KllDoubleMergeAggregation;
import org.apache.fluss.trino.functions.SketchReaders;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Apache Fluss connector plugin for Trino. */
public class FlussPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return new HashSet<>(
                Arrays.asList(
                        HllBuildAggregation.class,
                        HllMergeAggregation.class,
                        KllDoubleBuildAggregation.class,
                        KllDoubleMergeAggregation.class,
                        SketchReaders.class));
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return Collections.singletonList(new FlussConnectorFactory());
    }
}
