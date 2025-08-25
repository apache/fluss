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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;

/** Utilities for various metric groups for testing. */
public class TestingMetricGroups {

    public static final TabletServerMetricGroup TABLET_SERVER_METRICS =
            new TabletServerMetricGroup(NOPMetricRegistry.INSTANCE, "fluss", "host", "rack", 0);

    public static final CoordinatorMetricGroup COORDINATOR_METRICS =
            new CoordinatorMetricGroup(NOPMetricRegistry.INSTANCE, "cluster1", "host", "0");

    public static final PhysicalTableMetricGroup TABLE_METRICS =
            new PhysicalTableMetricGroup(
                    NOPMetricRegistry.INSTANCE,
                    PhysicalTablePath.of(TablePath.of("mydb", "mytable"), null),
                    false,
                    TABLET_SERVER_METRICS);

    public static final BucketMetricGroup BUCKET_METRICS =
            new BucketMetricGroup(NOPMetricRegistry.INSTANCE, 0, TABLE_METRICS);
}
