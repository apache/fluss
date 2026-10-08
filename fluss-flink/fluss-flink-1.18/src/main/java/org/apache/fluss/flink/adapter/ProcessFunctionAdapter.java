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

package org.apache.fluss.flink.adapter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;

/**
 * Flink 1.18 variant of the process function adapter.
 *
 * <p>In Flink 1.18, only {@code open(Configuration)} is available (OpenContext does not exist).
 * This adapter overrides that method and delegates to {@link #doOpen()}.
 *
 * <p>TODO: remove this class when no longer support flink 1.18.
 *
 * @param <I> the input element type
 * @param <O> the output element type
 */
public abstract class ProcessFunctionAdapter<I, O> extends ProcessFunction<I, O> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        doOpen();
    }

    /** Subclass initialization logic, called exactly once when the function is opened. */
    protected abstract void doOpen() throws Exception;
}
