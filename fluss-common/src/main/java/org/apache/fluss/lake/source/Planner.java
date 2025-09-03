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

package org.apache.fluss.lake.source;

import org.apache.fluss.annotation.PublicEvolving;

import java.io.IOException;
import java.util.List;

/**
 * A planner interface for generating readable splits for lake data sources.
 *
 * <p>Implementations of this interface are responsible for determining how to divide the data into
 * manageable splits that can be read in parallel. The planning should consider the pushed-down
 * optimizations (filters, limits, etc.) from {@link LakeSource}.
 *
 * @param <Split> the type of data split this planner generates, must extend {@link LakeSplit}
 * @since 0.8
 */
@PublicEvolving
public interface Planner<Split extends LakeSplit> {

    /**
     * Plans and generates a list of readable data splits in parallel.
     *
     * @return the list of readable data splits
     * @throws IOException if an I/O error occurs
     */
    List<Split> plan() throws IOException;
}
