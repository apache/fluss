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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import java.util.List;
import java.util.Optional;

/**
 * Notified by {@link CoordinatorContext}'s own mutators whenever state relevant to
 * cluster/tablet-server health changes, regardless of which caller triggered the mutation.
 *
 * <p>This is what closes the "did we remember to instrument this call site" class of gap: any
 * current or future caller of a hooked {@code CoordinatorContext} mutator is covered automatically,
 * without needing to know this interface exists. {@code CoordinatorHealthCache} implements this
 * interface directly -- its {@code onXxx} methods already have this exact signature. The interface
 * exists only so {@code CoordinatorContext}'s field can be typed as an abstraction instead of
 * depending on {@code CoordinatorHealthCache} concretely.
 */
interface CoordinatorContextListener {

    void onBucketLeaderAndIsrChanged(
            TableBucket tableBucket, List<Integer> assignment, Optional<LeaderAndIsr> current);

    void onLeaderActivityChanged(boolean isActive);

    void onTabletServerDied();

    void onTabletServerRegistered();

    void onTopologyChanged();

    /** The default until {@link CoordinatorContext#setListener} is called with a real one. */
    CoordinatorContextListener NO_OP =
            new CoordinatorContextListener() {
                @Override
                public void onBucketLeaderAndIsrChanged(
                        TableBucket tableBucket,
                        List<Integer> assignment,
                        Optional<LeaderAndIsr> current) {}

                @Override
                public void onLeaderActivityChanged(boolean isActive) {}

                @Override
                public void onTabletServerDied() {}

                @Override
                public void onTabletServerRegistered() {}

                @Override
                public void onTopologyChanged() {}
            };
}
