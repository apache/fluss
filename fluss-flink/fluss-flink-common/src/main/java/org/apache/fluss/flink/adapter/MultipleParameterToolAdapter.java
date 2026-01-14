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

import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Map;

/**
 * An adapter for Flink {@link MultipleParameterTool} class. The {@link MultipleParameterTool} is
 * moved to a new package since Flink 2.x, so this adapter helps to bridge compatibility for
 * different Flink versions.
 *
 * <p>TODO: remove this class when no longer support all the Flink 1.x series.
 */
public class MultipleParameterToolAdapter {

    private MultipleParameterToolAdapter() {}

    private MultipleParameterTool multipleParameterTool;

    public static MultipleParameterToolAdapter fromArgs(String[] args) {
        MultipleParameterToolAdapter adapter = new MultipleParameterToolAdapter();
        adapter.multipleParameterTool = MultipleParameterTool.fromArgs(args);
        return adapter;
    }

    public Map<String, String> toMap() {
        return this.multipleParameterTool.toMap();
    }
}
