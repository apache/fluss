/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.metadata.TableBucket;

/**
 * A {@link PendingFetch} that already has been completed. This is a CompletedFetch that needs to in
 * queue to wait other PendingFetch.
 */
class CompletedPendingFetch implements PendingFetch {

    private final CompletedFetch completedFetch;

    public CompletedPendingFetch(CompletedFetch completedFetch) {
        this.completedFetch = completedFetch;
    }

    @Override
    public TableBucket tableBucket() {
        return completedFetch.tableBucket;
    }

    @Override
    public boolean isCompleted() {
        return true;
    }

    @Override
    public CompletedFetch toCompletedFetch() {
        return completedFetch;
    }
}
