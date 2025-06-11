/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.committer;

import com.alibaba.fluss.lake.committer.CommitterInitContext;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.metadata.TablePath;

/** The {@link CommitterInitContext} implementation for {@link LakeCommitter}. */
public class TieringCommitterInitContext implements CommitterInitContext {

    private final TablePath tablePath;

    public TieringCommitterInitContext(TablePath tablePath) {
        this.tablePath = tablePath;
    }

    @Override
    public TablePath tablePath() {
        return tablePath;
    }
}
