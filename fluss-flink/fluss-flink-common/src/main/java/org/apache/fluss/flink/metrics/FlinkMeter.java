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

package org.apache.fluss.flink.metrics;

import org.apache.fluss.metrics.MetricView;

import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.View;

/**
 * An implementation of Flink's {@link Meter} which wraps Fluss's Meter.
 *
 * <p>It is a Flink {@link View}, so Flink's metric registry updates it on the interval a Fluss
 * {@link MetricView} expects. Fluss's own registry does not see the wrapped meter, and without this
 * a {@code MeterView} never computes a rate.
 */
public class FlinkMeter implements Meter, View {

    private final org.apache.fluss.metrics.Meter wrapped;

    public FlinkMeter(org.apache.fluss.metrics.Meter wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void markEvent() {
        wrapped.markEvent();
    }

    @Override
    public void markEvent(long n) {
        wrapped.markEvent(n);
    }

    @Override
    public double getRate() {
        return wrapped.getRate();
    }

    @Override
    public long getCount() {
        return wrapped.getCount();
    }

    @Override
    public void update() {
        if (wrapped instanceof MetricView) {
            ((MetricView) wrapped).update();
        }
    }
}
