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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.lake.lakestorage.LakeTableLookuperManager.Context;
import org.apache.fluss.lake.lakestorage.LakeTableLookuperManager.LookupCacheOptions;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the shared lake table lookuper manager contract. */
class LakeTableLookuperManagerTest {

    @Test
    void testContextCarriesLookuperSettings() {
        Configuration lakeConfiguration = new Configuration();
        TableConfig tableConfig = new TableConfig(new Configuration());
        AtomicBoolean diskWriteChecked = new AtomicBoolean();
        Runnable diskWriteGuard = () -> diskWriteChecked.set(true);

        Context context = new Context(lakeConfiguration, "table-1", tableConfig, diskWriteGuard);

        assertThat(context.lakeConfiguration()).isSameAs(lakeConfiguration);
        assertThat(context.cacheNamespace()).isEqualTo("table-1");
        assertThat(context.tableConfig()).isSameAs(tableConfig);
        context.diskWriteGuard().run();
        assertThat(diskWriteChecked.get()).isTrue();
    }

    @Test
    void testContextRejectsMissingSettings() {
        Configuration lakeConfiguration = new Configuration();
        TableConfig tableConfig = new TableConfig(new Configuration());
        Runnable diskWriteGuard = () -> {};

        assertThatThrownBy(() -> new Context(null, "table-1", tableConfig, diskWriteGuard))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Context(lakeConfiguration, null, tableConfig, diskWriteGuard))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Context(lakeConfiguration, "table-1", null, diskWriteGuard))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Context(lakeConfiguration, "table-1", tableConfig, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testDefaultCapacityEvictionsMetricIsZero() {
        LakeTableLookuperManager manager =
                new LakeTableLookuperManager() {
                    @Override
                    public LakeTableLookuper createLakeTableLookuper(
                            TablePath tablePath, Context context) {
                        return null;
                    }

                    @Override
                    public void reconfigure(LookupCacheOptions options) {}

                    @Override
                    public void close() {}
                };

        assertThat(manager.fileCacheCapacityEvictions()).isZero();
    }
}
