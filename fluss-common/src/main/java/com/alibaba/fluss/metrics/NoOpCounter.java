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

package com.alibaba.fluss.metrics;

/**
 * A {@link Counter} that does nothing.
 *
 * <p>This is a useful util to avoid exceptions (e.g., NPE) thrown from metrics, because metrics are
 * tools that should not fail the program when used incorrectly.
 */
public class NoOpCounter implements Counter {

    public static final NoOpCounter INSTANCE = new NoOpCounter();

    @Override
    public void inc() {}

    @Override
    public void inc(long n) {}

    @Override
    public void dec() {}

    @Override
    public void dec(long n) {}

    @Override
    public long getCount() {
        return 0;
    }
}
