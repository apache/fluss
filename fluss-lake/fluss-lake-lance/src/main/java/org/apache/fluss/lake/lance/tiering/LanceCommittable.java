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

package org.apache.fluss.lake.lance.tiering;

import com.lancedb.lance.FragmentMetadata;

import java.util.List;

/** The committable that derived from {@link LanceWriteResult} to commit to Lance. */
public class LanceCommittable {
    private final List<FragmentMetadata> committable;

    public LanceCommittable(List<FragmentMetadata> committable) {
        this.committable = committable;
    }

    public List<FragmentMetadata> committable() {
        return committable;
    }
}
