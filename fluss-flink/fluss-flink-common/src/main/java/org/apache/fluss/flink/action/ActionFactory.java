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

package org.apache.fluss.flink.action;

import org.apache.fluss.annotation.Internal;

import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Optional;

/** SPI for {@link Action} factories, registered via JDK {@link java.util.ServiceLoader}. */
@Internal
public interface ActionFactory {

    /**
     * Identifier matched against the first CLI argument after lowercasing and replacing {@code -}
     * with {@code _}.
     */
    String identifier();

    /** Construct the action from parsed CLI parameters. Empty when {@code --help} is requested. */
    Optional<Action> create(MultipleParameterTool params);

    /** Help text printed when {@code --help} is passed. */
    default String help() {
        return "";
    }
}
